import grpc
import asyncio
from datetime import datetime, timedelta

from ulid import ULID
from pyensign.events import from_proto
from pyensign.api.v1beta1 import ensign_pb2
from pyensign.utils.queue import BidiQueue
from pyensign.api.v1beta1.event import wrap, unwrap
from pyensign.exceptions import (
    EnsignError,
    EnsignTypeError,
    EnsignTimeoutError,
    EnsignInitError,
    EnsignInvalidTopicError,
    EnsignTopicNotFoundError,
)
from pyensign.iterator import (
    RequestIterator,
    PublishResponseIterator,
    SubscribeResponseIterator,
)

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
        self._responses = None
        self._topics = {}

    async def connect(self):
        """
        Implement this method for protocol-specific stream initialization.
        """

        raise NotImplementedError

    async def run(self):
        """
        Send and recv on the stream and attempt to reconnect to the stream on failure.
        """

        if self._responses is None:
            raise EnsignInitError("stream is not connected")

        while not self.shutdown.is_set():
            # Consume the responses until a gRPC error occurs
            await self._responses.consume()

            # Close the queue to stop the current request iterator
            await self.queue.close()

            # Try to reconnect to the stream
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
            except asyncio.CancelledError:
                # Respect task cancellation from gRPC, which usually means that the
                # underlying channel was closed by the client. At this point it won't
                # be possible to reconnect to the stream.
                await self.close()
                return

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

    async def close(self):
        """
        Close the stream as gracefully as possible.
        """

        self.shutdown.set()
        await self.queue.close()


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

        # Create the gRPC stream from the request iterator
        open_stream = ensign_pb2.PublisherRequest(
            open_stream=ensign_pb2.OpenStream(client_id=self.client_id)
        )
        stream = self.stub.Publish(RequestIterator(self.queue, open_stream))

        # First response from the server should be a ready message
        rep = await stream.read()
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

        # Create the response iterator from the gRPC stream
        self._responses = PublishResponseIterator(
            stream, self.pending, on_ack=self.on_ack, on_nack=self.on_nack
        )

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
            await self.queue.write_request(req)
            event.mark_published()

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

        # Create the gRPC stream from the request iterator
        sub = ensign_pb2.SubscribeRequest(
            subscription=ensign_pb2.Subscription(
                client_id=self.client_id,
                topics=self.topics,
                query=self.query,
                group=self.consumer_group,
            )
        )
        stream = self.stub.Subscribe(RequestIterator(self.queue, sub))

        # First response from the server should be a ready message
        rep = await stream.read()
        rep_type = rep.WhichOneof("embed")
        if rep_type != "ready":
            raise EnsignTypeError("expected ready response, got {}".format(rep_type))

        # Save the topics from the ready message
        self._parse_topics(rep.ready.topics)

        # Create the response iterator from the gRPC stream
        self._responses = SubscribeResponseIterator(stream, self.queue)

    async def consume(self):
        """
        Consume the events from the incoming queue and yield them to the caller. This
        coroutine is completely independent from the gRPC stream, so it will continue
        to run even if the stream is closed. This allows the subscriber to reconnect to
        the stream in the background without interrupting any event processing. The
        caller must handle EnsignError exceptions.
        """

        while True:
            rep = await self.queue.read_response()
            if rep is None:
                if self.shutdown.is_set():
                    # If the subscriber is closing, stop consuming events
                    break
                else:
                    # Otherwise, wait for reconnect
                    continue
            elif isinstance(rep, EnsignError):
                # Raise errors to the caller, this includes connection timeout errors
                # and protocol errors
                raise rep
            else:
                # Convert the event into the user facing type
                event = from_proto(unwrap(rep))
                event.mark_subscribed(rep.id, self.queue)
                yield event
