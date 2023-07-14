import asyncio

import grpc
import pytest
from unittest.mock import AsyncMock

from pyensign.events import Event
from pyensign.utils.queue import BidiQueue
from pyensign.api.v1beta1 import ensign_pb2, event_pb2
from pyensign.iterator import (
    RequestIterator,
    ResponseIterator,
    PublishResponseIterator,
    SubscribeResponseIterator,
)
from pyensign.exceptions import EnsignTypeError


class TestRequestIterator:
    """
    Tests for the RequestIterator class.
    """

    @pytest.mark.asyncio
    async def test_iter(self):
        """
        Should be able to asynchronously iterate over the class.
        """
        requests = ["request1", "request2"]
        queue = BidiQueue()
        for request in requests:
            await queue.write_request(request)
        await queue.close()

        # Should iterate until the queue is closed
        i = 0
        async for request in RequestIterator(queue, "init_request"):
            if i == 0:
                assert request == "init_request"
            else:
                assert request == requests[i - 1]
            i += 1


class TestResponseIterator:
    """
    Tests for the ResponseIterator class.
    """

    @pytest.mark.asyncio
    async def test_iter(self):
        """
        Should be able to asynchronously iterate over the class.
        """
        responses = ["response1", "response2", grpc.aio.EOF]
        stream = AsyncMock(spec=grpc.aio.StreamStreamCall)
        stream.read.side_effect = responses

        # Should iterate until the stream is closed
        i = 0
        async for response in ResponseIterator(stream):
            assert response == responses[i]
            i += 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "exception",
        [
            (grpc.aio.AioRpcError(None, None, None)),
            (asyncio.CancelledError()),
        ],
    )
    async def test_iter_exception(self, exception):
        """
        Iteration should stop on exceptions.
        """
        stream = AsyncMock(spec=grpc.aio.StreamStreamCall)
        stream.read.side_effect = exception
        async for _ in ResponseIterator(stream):
            assert False, "Should not yield on exception"


class TestPublishResponseIterator:
    """
    Tests for the PublishResponseIterator class.
    """

    @pytest.mark.asyncio
    async def test_consume(self):
        responses = [
            ensign_pb2.PublisherReply(ack=ensign_pb2.Ack(id=b"1")),
            ensign_pb2.PublisherReply(nack=ensign_pb2.Nack(id=b"2")),
            ensign_pb2.PublisherReply(close_stream=ensign_pb2.CloseStream()),
        ]
        stream = AsyncMock(spec=grpc.aio.StreamStreamCall)
        stream.read.side_effect = responses

        async def on_ack(ack):
            assert ack.id == b"1"

        async def on_nack(nack):
            assert nack.id == b"2"

        events = [
            Event(data=b"foo", mimetype="text/plain"),
            Event(data=b"bar", mimetype="text/plain"),
        ]
        pending = {
            b"1": events[0],
            b"2": events[1],
        }

        # Should iterate and mark pending events as acked/nacked
        responses = PublishResponseIterator(
            stream, pending, on_ack=on_ack, on_nack=on_nack
        )
        await responses.consume()
        assert len(pending) == 0
        assert events[0].acked()
        assert events[1].nacked()

    @pytest.mark.asyncio
    async def test_consume_no_callbacks(self):
        responses = [
            ensign_pb2.PublisherReply(ack=ensign_pb2.Ack(id=b"1")),
            ensign_pb2.PublisherReply(nack=ensign_pb2.Nack(id=b"2")),
            ensign_pb2.PublisherReply(close_stream=ensign_pb2.CloseStream()),
        ]
        stream = AsyncMock(spec=grpc.aio.StreamStreamCall)
        stream.read.side_effect = responses

        events = [
            Event(data=b"foo", mimetype="text/plain"),
            Event(data=b"bar", mimetype="text/plain"),
        ]
        pending = {
            b"1": events[0],
            b"2": events[1],
        }

        # Should iterate and mark pending events as acked/nacked
        responses = PublishResponseIterator(stream, pending)
        await responses.consume()
        assert len(pending) == 0
        assert events[0].acked()
        assert events[1].nacked()

    @pytest.mark.asyncio
    async def test_consume_bad_type(self):
        """
        Should raise an exception if Ensign returns an unexpected message type.
        """

        stream = AsyncMock(spec=grpc.aio.StreamStreamCall)
        stream.read.side_effect = [ensign_pb2.SubscribeReply()]

        responses = PublishResponseIterator(stream, {})
        with pytest.raises(EnsignTypeError):
            await responses.consume()


class TestSubscribeResponseIterator:
    """
    Tests for the SubscribeResponseIterator class.
    """

    @pytest.mark.asyncio
    async def test_consume(self):
        events = [
            event_pb2.EventWrapper(id=b"1", event=b"foo"),
            event_pb2.EventWrapper(id=b"2", event=b"bar"),
        ]

        responses = [
            ensign_pb2.SubscribeReply(event=events[0]),
            ensign_pb2.SubscribeReply(event=events[1]),
            ensign_pb2.SubscribeReply(close_stream=ensign_pb2.CloseStream()),
        ]
        stream = AsyncMock(spec=grpc.aio.StreamStreamCall)
        stream.read.side_effect = responses

        queue = BidiQueue()

        # Should iterate and send events to the response queue
        responses = SubscribeResponseIterator(stream, queue)
        await responses.consume()
        for event in events:
            assert await queue.read_response() == event
        assert await queue.read_response() is None

    @pytest.mark.asyncio
    async def test_consume_bad_type(self):
        """
        Should send an error to the queue if Ensign returns an unexpected message type.
        """

        stream = AsyncMock(spec=grpc.aio.StreamStreamCall)
        stream.read.side_effect = [ensign_pb2.PublisherReply()]

        queue = BidiQueue()

        responses = SubscribeResponseIterator(stream, queue)
        await responses.consume()
        assert isinstance(await queue.read_response(), EnsignTypeError)
        assert await queue.read_response() is None
