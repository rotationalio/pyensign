import grpc
import asyncio
from datetime import datetime, timedelta

from pyensign.api.v1beta1 import ensign_pb2
from pyensign.exceptions import EnsignTypeError, EnsignTimeoutError, EnsignInitError
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
        stub,
        queue,
        reconnect_tick=RECONNECT_TICK,
        reconnect_timeout=RECONNECT_TIMEOUT,
    ):
        self.stub = stub
        self.queue = queue
        self.reconnect_tick = reconnect_tick
        self.reconnect_timeout = reconnect_timeout
        self.shutdown = asyncio.Event()
        self._responses = None

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
            except EnsignTimeoutError:
                break

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

        # Timeout expired, give up
        raise EnsignTimeoutError("timeout expired while trying to reconnect to stream")

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
        stub,
        queue,
        client_id,
        on_ack=None,
        on_nack=None,
        reconnect_tick=RECONNECT_TICK,
        reconnect_timeout=RECONNECT_TIMEOUT,
    ):
        self.stub = stub
        self.queue = queue
        self.client_id = client_id
        self.on_ack = on_ack
        self.on_nack = on_nack
        self.reconnect_tick = reconnect_tick
        self.reconnect_timeout = reconnect_timeout
        self.shutdown = asyncio.Event()

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
        # TODO: Parse the topic map from the ready message
        rep = await stream.read()
        rep_type = rep.WhichOneof("embed")
        if rep_type != "ready":
            raise EnsignTypeError("expected ready response, got {}".format(rep_type))

        # Create the response iterator from the gRPC stream
        self._responses = PublishResponseIterator(
            stream, on_ack=self.on_ack, on_nack=self.on_nack
        )


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
        stub,
        queue,
        client_id,
        topic_ids,
        query="",
        consumer_group=None,
        reconnect_tick=RECONNECT_TICK,
        reconnect_timeout=RECONNECT_TIMEOUT,
    ):
        self.stub = stub
        self.queue = queue
        self.client_id = client_id
        self.topic_ids = topic_ids
        self.query = query
        self.consumer_group = consumer_group
        self.reconnect_tick = reconnect_tick
        self.reconnect_timeout = reconnect_timeout
        self.shutdown = asyncio.Event()

    async def connect(self):
        """
        Attempt to establish a connection to the server and raise an exception if the
        connection could not be established.
        """

        # Create the gRPC stream from the request iterator
        sub = ensign_pb2.SubscribeRequest(
            subscription=ensign_pb2.Subscription(
                client_id=self.client_id,
                topics=self.topic_ids,
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

        # Create the response iterator from the gRPC stream
        self._responses = SubscribeResponseIterator(stream, self.queue)
