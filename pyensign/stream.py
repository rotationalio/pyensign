import grpc
import asyncio
import logging
from datetime import datetime, timedelta

from ulid import ULID
from pyensign.events import from_proto
from pyensign.api.v1beta1 import ensign_pb2
from pyensign.utils.queue import BidiQueue
from pyensign.utils.tasks import with_callback
from pyensign.api.v1beta1.event import wrap, unwrap
from pyensign.exceptions import (
    EnsignError,
    EnsignTypeError,
    EnsignTimeoutError,
    EnsignInitError,
    EnsignInvalidTopicError,
    EnsignClientClosingError,
    EnsignTopicNotFoundError,
)
from pyensign.iterator import ResponseIterator

RECONNECT_TICK = timedelta(milliseconds=750)
RECONNECT_TIMEOUT = timedelta(minutes=5)


class StreamHandler:
    """
    StreamHandler manages the connection to a gRPC stream and handles reconnections if
    the stream goes down.
    """

    def __init__(
        self,
        client,
        queue,
        reconnect_tick=RECONNECT_TICK,
        reconnect_timeout=RECONNECT_TIMEOUT,
    ):
        self.client_id = client.client_id
        self.stub = client.stub
        self.topic_cache = client.topics
        self.queue = queue
        self.reconnect_tick = reconnect_tick
        self.reconnect_timeout = reconnect_timeout
        self.shutdown = asyncio.Event()
        self.stream = None
        self._topics = {}

    async def connect(self):
        """
        Implement this method for protocol-specific stream initialization.
        """

        raise NotImplementedError

    async def handle_requests(self):
        """
        Handle requests from the queue by writing them to a gRPC stream. Override this
        method to implement specific request handling.
        """

        async for req in self.queue.requests():
            await self.stream.write(req)

    async def handle_responses(self):
        """
        Handle responses from the stream by writing them to the queue. Override this
        method to implement specific response handling.
        """

        async for rep in ResponseIterator(self.stream):
            await self.queue.write_response(rep)

    async def run(self):
        """
        Send and recv on the stream and attempt to reconnect to the stream on failure.
        """

        if not self.stream:
            raise EnsignInitError("stream is not initialized")

        while True:
            # Run the stream handlers concurrently.
            requests = asyncio.create_task(self.handle_requests())
            requests.add_done_callback(lambda _: self.stream.cancel())
            responses = asyncio.create_task(self.handle_responses())
            responses.add_done_callback(lambda _: self.queue.done_writing())
            await asyncio.gather(requests, responses)

            # We are done writing to the stream
            await self.stream.done_writing()

            # If shutdown is set then don't try to reconnect
            if self.shutdown.is_set():
                # Consume any pending requests so close
                self.queue.discard_requests()
                return

            # Try to reconnect to the stream
            self.queue.ready()
            try:
                await self.reconnect()
            except EnsignError as e:
                # Return timeout errors etc. to the user by writing them to the queue
                await self.queue.write_response(e)
                return

    async def reconnect(self):
        """
        Try to reconnect to the stream until the timeout expires.
        """

        self.reset_timeout()
        while not self.timeout_expired():
            # Wait for the reconnect tick
            await asyncio.sleep(self.reconnect_tick.microseconds / 1000000)

            # Don't try to reconnect if the stream is being closed
            if self.shutdown.is_set():
                return

            # Try to connect and ignore gRPC errors
            try:
                await self.connect()
                return
            except grpc.aio.AioRpcError:
                continue

        # Timeout expired, give up.
        raise EnsignTimeoutError("timeout expired while trying to reconnect to stream")

    def _parse_topics(self, topic_map):
        """
        Parse topics from a topic map, updating the state on the stream and the global
        topic cache.
        """

        for name, id_bytes in topic_map.items():
            try:
                id = ULID(id_bytes)
            except ValueError:
                # Ignore unparseable topic IDs
                continue

            # Update the topic cache
            self._topics[name] = id
            self.topic_cache.add(name, id)

    def reset_timeout(self):
        self.timeout = datetime.now() + self.reconnect_timeout

    def timeout_expired(self):
        return datetime.now() > self.timeout

    async def flush(self, timeout=None):
        """
        Flush all pending requests from the queue.
        """

        await self.queue.flush_requests(timeout=timeout)

    async def close(self):
        """
        Close the stream as gracefully as possible, ensuring that all pending requests
        are flushed from the queue.
        """

        self.shutdown.set()
        await self.queue.close()
        if self.stream:
            self.stream.cancel()


class Publisher(StreamHandler):
    """
    Publisher manages the connection to a gRPC publish stream. Once the ready message
    is received from the server, the stream is considered open and the request/response
    iterators are used to send and receive messages on the stream. The publisher relies
    on the response iterator to detect when the stream has been closed. If the iterator
    is exhausted, the publisher attempts to reconnect to the stream. If the reconnect
    timeout is exceeded then the publisher gives up and closes the stream.
    """

    def __init__(
        self,
        client,
        topic,
        on_ack=None,
        on_nack=None,
        **kwargs,
    ):
        super().__init__(client, BidiQueue(), **kwargs)
        self.topic = topic
        self.on_ack = on_ack
        self.on_nack = on_nack
        self.pending = {}

    async def connect(self):
        """
        Attempt to establish a connection to the server and raise an exception if the
        connection could not be established.
        """

        # Create the gRPC stream and ensure that it is connected
        self.stream = self.stub.Publish()
        await self.stream.wait_for_connection()

        # Send the open stream request to the server
        open_stream = ensign_pb2.PublisherRequest(
            open_stream=ensign_pb2.OpenStream(client_id=self.client_id)
        )
        await self.stream.write(open_stream)

        # First response from the server should be a ready message
        rep = await self.stream.read()
        rep_type = rep.WhichOneof("embed")
        if rep_type != "ready":
            raise EnsignTypeError("expected ready response, got {}".format(rep_type))

        # Save the topics from the ready message
        self._parse_topics(rep.ready.topics)

        # Set the topic ID for publish requests
        try:
            self._update_topic()
        except EnsignTopicNotFoundError as e:
            await self.close()
            raise e

    async def handle_requests(self):
        """
        Handle requests from the queue by writing them to a gRPC stream.
        """

        async for req in self.queue.requests():
            try:
                await self.stream.write(req)
            except grpc.aio.AioRpcError as e:
                logging.warning(
                    f"gRPC error occurred while writing to the publisher stream: {e}",
                )
                return e
            except asyncio.InvalidStateError as e:
                # TODO: Prevent dropping the event if the stream is closed
                logging.info(
                    "publisher stream was unexpectedly closed",
                )
                return e
            finally:
                # Whether or not the write succeeded, mark the request as processed
                self.queue.request_done()

            id = req.event.local_id
            if id in self.pending:
                self.pending[id].mark_published()

    async def handle_responses(self):
        """
        Handle acks and nacks from the stream by executing the user-defined callbacks.
        """

        async for rep in ResponseIterator(self.stream):
            # Handle messages from the server
            rep_type = rep.WhichOneof("embed")
            if rep_type == "ack":
                event = self.pending.pop(rep.ack.id, None)
                if event:
                    event.mark_acked(rep.ack)
                    if self.on_ack:
                        try:
                            await self.on_ack(rep.ack)
                        except Exception as e:
                            logging.warning(
                                f"unhandled exception while awaiting ack callback: {e}",
                                exc_info=True,
                            )
            elif rep_type == "nack":
                event = self.pending.pop(rep.nack.id, None)
                if event:
                    event.mark_nacked(rep.nack)
                    if self.on_nack:
                        try:
                            await self.on_nack(rep.nack)
                        except Exception as e:
                            logging.warning(
                                f"unhandled exception while awaiting nack callback: {e}",
                                exc_info=True,
                            )
            elif rep_type == "close_stream":
                break
            else:
                raise EnsignTypeError(f"unexpected response type: {rep_type}")

    async def queue_events(self, events):
        """
        Queue events to be published on the stream.
        """

        async for event in events:
            # If we're shutting down, stop queueing events
            if self.shutdown.is_set():
                break

            # Queue the event
            wrapper = wrap(event.proto(), self.topic.id)
            req = ensign_pb2.PublisherRequest(event=wrapper)
            try:
                await self.queue.write_request(req)
            except asyncio.InvalidStateError:
                raise EnsignClientClosingError
            event.mark_queued()

            # Save the event for ack/nack handling
            # TODO: How do we handle events that are never acked/nacked?
            self.pending[wrapper.local_id] = event

    def _update_topic(self):
        """
        Set the topic ID for the publisher by performing a lookup into the map. An
        exception is raised if the topic is not in the map, which implies that the
        topic does not exist or it was created after the stream was opened.
        """

        if self.topic.id:
            # If topic ID is already set, ensure that it exists
            # Edge case: The topic name looks like a topic ID
            if (
                self.topic.id not in self._topics.values()
                and str(self.topic.id) not in self._topics
            ):
                raise EnsignTopicNotFoundError(self.topic.id)
        elif self.topic.name:
            self.topic.id = self._topics.get(self.topic.name, None)
            if not self.topic.id:
                raise EnsignTopicNotFoundError(self.topic.id)
        else:
            raise EnsignInvalidTopicError("topic has no name or ID")


class Subscriber(StreamHandler):
    """
    Subscriber manages the connection to a gRPC stream. Once the ready message is
    received from the server, the stream is considered open and the request/response
    iterators are used to send and receive messages on the stream. The subscriber
    relies on the response iterator to detect when the stream has been closed. If the
    iterator is exhausted, the subscriber attempts to reconnect to the stream. If
    the reconnect timeout is exceeded then the subscriber gives up and closes the
    stream.
    """

    def __init__(
        self,
        client,
        topics,
        query="",
        consumer_group=None,
        **kwargs,
    ):
        super().__init__(client, BidiQueue(), **kwargs)
        self.topics = topics
        self.query = query
        self.consumer_group = consumer_group

    async def connect(self):
        """
        Attempt to establish a connection to the server and raise an exception if the
        connection could not be established.
        """

        # Create the gRPC stream and ensure that it is connected
        self.stream = self.stub.Subscribe()
        await self.stream.wait_for_connection()

        # Send the subscribe request on the stream
        sub = ensign_pb2.SubscribeRequest(
            subscription=ensign_pb2.Subscription(
                client_id=self.client_id,
                topics=self.topics,
                query=self.query,
                group=self.consumer_group,
            )
        )
        await self.stream.write(sub)

        # First response from the server should be a ready message
        rep = await self.stream.read()
        rep_type = rep.WhichOneof("embed")
        if rep_type != "ready":
            raise EnsignTypeError("expected ready response, got {}".format(rep_type))

        # Save the topics from the ready message
        self._parse_topics(rep.ready.topics)

    async def handle_requests(self):
        """
        Handle acks and nacks from the queue by writing them to the gRPC stream.
        """

        async for req in self.queue.requests():
            try:
                await self.stream.write(req)
            except grpc.aio.AioRpcError as e:
                logging.warning(
                    f"gRPC error occurred while writing to the subscriber stream: {e}"
                )
                return e
            except asyncio.InvalidStateError as e:
                # TODO: Prevent dropping the ack if the stream is closed
                logging.info(
                    "subscriber stream was unexpectedly closed",
                )
                return e
            finally:
                # Whether or not the write succeeded, mark the request as processed
                self.queue.request_done()

    async def handle_responses(self):
        """
        Handle events from the stream by writing them to the queue.
        """

        async for rep in ResponseIterator(self.stream):
            rep_type = rep.WhichOneof("embed")
            if rep_type == "event":
                await self.queue.write_response(rep.event)
            elif rep_type == "close_stream":
                break
            else:
                raise EnsignTypeError(f"unexpected response type: {rep_type}")

    async def consume(self):
        """
        Consume the events from the incoming queue and yield them to the caller. This
        coroutine is completely independent from the gRPC stream, so it will continue
        to run even if the stream is closed. This allows the subscriber to reconnect to
        the stream in the background without interrupting any event processing. The
        caller must handle EnsignError exceptions raised from this iterator.
        """

        async for rep in self.queue.responses():
            if isinstance(rep, EnsignError):
                # Raise errors to the caller, this includes connection timeout errors
                # and protocol errors
                raise rep
            else:
                # Convert the event into the user facing type
                event = from_proto(unwrap(rep))
                event.mark_subscribed(rep.id, self.queue)
                yield event
