import grpc
import pytest
import asyncio
from ulid import ULID
from asyncmock import patch, AsyncMock
from datetime import timedelta
from unittest.mock import Mock

from pyensign.api.v1beta1.event import wrap, unwrap
from pyensign.api.v1beta1 import ensign_pb2
from pyensign.stream import StreamHandler, Publisher, Subscriber
from pyensign.iterator import ResponseIterator
from pyensign.utils.queue import BidiQueue
from pyensign.exceptions import EnsignTimeoutError, EnsignTypeError
from pyensign.events import Event, EventState, from_proto
from pyensign.utils.topics import Topic


class TestStreamHandler:
    """
    Tests for the StreamHandler base class.
    """

    @pytest.mark.asyncio
    @patch("pyensign.stream.StreamHandler.connect")
    async def test_run(self, mock_connect):
        """
        Test that the StreamHandler runs and reconnects to the stream on failure.
        """

        mock_connect.return_value = None
        handler = StreamHandler(
            Mock(spec=["client_id", "stub", "topics"]),
            BidiQueue(),
            reconnect_tick=timedelta(milliseconds=1),
        )
        handler.stream = AsyncMock(spec=["read", "write", "done_writing", "cancel"])
        handler.stream.read.side_effect = grpc.aio.AioRpcError(None, None, None)
        handler.stream.cancel = AsyncMock(not_async=True)

        # Run the stream handler in a separate task, which should reconnect a few times
        task = asyncio.create_task(handler.run())
        while mock_connect.call_count < 3:
            await asyncio.sleep(0.01)

        # Close the stream handler which should gracefully shutdown the task.
        await handler.close()
        result = await task
        assert result is None

    @pytest.mark.asyncio
    @patch("pyensign.stream.StreamHandler.connect")
    async def test_run_timeout(self, mock_connect):
        """
        Test that the StreamHandler times out after failing to reconnect.
        """

        mock_connect.return_value = None
        mock_connect.side_effect = grpc.aio.AioRpcError(None, None, None)
        handler = StreamHandler(
            Mock(spec=["client_id", "stub", "topics"]),
            BidiQueue(),
            reconnect_tick=timedelta(milliseconds=1),
            reconnect_timeout=timedelta(milliseconds=5),
        )
        handler.stream = AsyncMock(spec=["read", "write", "done_writing", "cancel"])
        handler.stream.read.side_effect = grpc.aio.AioRpcError(None, None, None)
        handler.stream.cancel = AsyncMock(not_async=True)

        # Run the stream handler which should fail to reconnect
        await handler.run()
        exception = await handler.queue.read_response()
        assert isinstance(exception, EnsignTimeoutError)


def async_iter(items):
    async def next():
        for item in items:
            yield item

    return next()


class TestPublisher:
    """
    Tests for the Publisher stream handler.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "events",
        [
            (),
            ([Event(data=b"foo")]),
            ([Event(data=b"foo"), Event(data=b"bar")]),
        ],
    )
    async def test_queue_events(self, events):
        publisher = Publisher(
            Mock(spec=["client_id", "stub", "topics"]), Topic(id=ULID(), name="topic")
        )
        await publisher.queue_events(async_iter(events))

        # Check that the publish requests were queued
        for event in events:
            assert event._state == EventState.QUEUED
            req = await publisher.queue.read_request()
            assert isinstance(req, ensign_pb2.PublisherRequest)
            wrapper = req.event
            assert wrapper.topic_id == publisher.topic.id.bytes
            published = from_proto(unwrap(wrapper))
            assert published.data == event.data

        # All events should be pending ACKs
        assert len(publisher.pending) == len(events)

    @pytest.mark.asyncio
    async def test_handle_requests(self):
        publisher = Publisher(
            Mock(spec=["client_id", "stub", "topics"]), Topic(id=ULID(), name="topic")
        )
        publisher.stream = AsyncMock()
        publisher.stream.cancel = AsyncMock(not_async=True)

        # Queue some events
        events = [Event(data=b"foo"), Event(data=b"bar")]
        await publisher.queue_events(async_iter(events))

        # Publish the events in the queue
        publish = asyncio.create_task(publisher.handle_requests())
        await publisher.close()
        publisher.queue.done_writing()
        await publish

        # All the events should be in the published state
        assert len(publisher.pending) == len(events)
        for _, event in publisher.pending.items():
            assert event.published()

    @pytest.mark.asyncio
    async def test_handle_requests_error(self):
        """
        Test that the request handler stops on gRPC errors.
        """
        publisher = Publisher(
            Mock(spec=["client_id", "stub", "topics"]), Topic(id=ULID(), name="topic")
        )
        publisher.stream = AsyncMock(spec=["write"])
        publisher.stream.write.side_effect = grpc.aio.AioRpcError(None, None, None)

        # Queue some events
        events = [Event(data=b"foo"), Event(data=b"bar")]
        await publisher.queue_events(async_iter(events))

        # Publish events, the handler should return the error
        result = await publisher.handle_requests()
        assert isinstance(result, grpc.aio.AioRpcError)

    @pytest.mark.asyncio
    async def test_handle_responses(self):
        async def on_ack(ack):
            assert ack.id == b"1"

        async def on_nack(nack):
            assert nack.id == b"2"

        publisher = Publisher(
            Mock(spec=["client_id", "stub", "topics"]),
            Topic(id=ULID(), name="topic"),
            on_ack=on_ack,
            on_nack=on_nack,
        )

        events = [Event(data=b"foo"), Event(data=b"bar")]
        responses = [
            ensign_pb2.PublisherReply(ack=ensign_pb2.Ack(id=b"1")),
            ensign_pb2.PublisherReply(nack=ensign_pb2.Nack(id=b"2")),
            ensign_pb2.PublisherReply(close_stream=ensign_pb2.CloseStream()),
        ]

        publisher.stream = AsyncMock(spec=["read"])
        publisher.stream.read.side_effect = responses
        publisher.on_ack = on_ack
        publisher.on_nack = on_nack
        publisher.pending = {
            b"1": events[0],
            b"2": events[1],
        }

        # Read all the responses from the stream
        await publisher.handle_responses()

        # All the events should be in the published state
        assert len(publisher.pending) == 0
        assert events[0].acked()
        assert events[1].nacked()

    @pytest.mark.asyncio
    async def test_handle_responses_no_callbacks(self):
        publisher = Publisher(
            Mock(spec=["client_id", "stub", "topics"]),
            Topic(id=ULID(), name="topic"),
        )

        events = [Event(data=b"foo"), Event(data=b"bar")]
        responses = [
            ensign_pb2.PublisherReply(ack=ensign_pb2.Ack(id=b"1")),
            ensign_pb2.PublisherReply(nack=ensign_pb2.Nack(id=b"2")),
            ensign_pb2.PublisherReply(close_stream=ensign_pb2.CloseStream()),
        ]

        publisher.stream = AsyncMock(spec=["read"])
        publisher.stream.read.side_effect = responses
        publisher.pending = {
            b"1": events[0],
            b"2": events[1],
        }

        # Read all the responses from the stream
        await publisher.handle_responses()

        # All the events should be in the published state
        assert len(publisher.pending) == 0
        assert events[0].acked()
        assert events[1].nacked()

    @pytest.mark.asyncio
    async def test_handle_responses_bad_type(self):
        """
        Should raise an exceptyion if Ensign returns an unexpected message type.
        """

        publisher = Publisher(
            Mock(spec=["client_id", "stub", "topics"]),
            Topic(id=ULID(), name="topic"),
        )
        publisher.stream = AsyncMock(spec=["read"])
        publisher.stream.read.side_effect = [ensign_pb2.SubscribeReply()]

        with pytest.raises(EnsignTypeError):
            await publisher.handle_responses()


class TestSubscriber:
    """
    Tests for the Subscriber stream handler.
    """

    @pytest.mark.asyncio
    async def test_consume(self):
        consumed = 0

        subscriber = Subscriber(
            Mock(spec=["client_id", "stub", "topics"]),
            [Topic(id=ULID(), name="topic")],
        )

        # Write some events to the queue
        events = [Event(data=b"foo %d" % i) for i in range(10)]
        for event in events:
            await subscriber.queue.write_response(
                wrap(event.proto(), subscriber.topics[0].id)
            )

        # Consume the events
        async for event in subscriber.consume():
            assert event.data == events[consumed].data
            consumed += 1
            if consumed == len(events):
                break

    @pytest.mark.asyncio
    async def test_consume_stop(self):
        """
        Test that consume() stops when the subscriber is closed.
        """

        subscriber = Subscriber(
            Mock(spec=["client_id", "stub", "topics"]),
            [Topic(id=ULID(), name="topic")],
        )

        async def consume():
            async for _ in subscriber.consume():
                pass

        # Run the consumer in a separate task
        task = asyncio.create_task(consume())

        # Close the subscriber which should stop the iterator
        await subscriber.close()
        await task

    @pytest.mark.asyncio
    async def test_consume_error(self):
        subscriber = Subscriber(
            Mock(spec=["client_id", "stub", "topics"]),
            [Topic(id=ULID(), name="topic")],
        )

        # Write an error to the queue
        await subscriber.queue.write_response(EnsignTimeoutError("reconnect timeout"))

        # Should raise the error when consumed
        with pytest.raises(EnsignTimeoutError):
            async for _ in subscriber.consume():
                pass

    @pytest.mark.asyncio
    async def test_handle_requests(self):
        subscriber = Subscriber(
            Mock(spec=["client_id", "stub", "topics"]),
            [Topic(id=ULID(), name="topic")],
        )
        subscriber.stream = AsyncMock(spec=["write"])
        subscriber.stream.cancel = AsyncMock(not_async=True)

        # Write some requests to the queue
        requests = [
            ensign_pb2.SubscribeRequest(ack=ensign_pb2.Ack(id=b"1")),
            ensign_pb2.SubscribeRequest(nack=ensign_pb2.Nack(id=b"2")),
        ]
        for request in requests:
            await subscriber.queue.write_request(
                request,
            )

        # Consume the requests
        subscribe = asyncio.create_task(subscriber.handle_requests())
        await subscriber.close()
        subscriber.queue.done_writing()
        await subscribe

        # All requests should be consumed
        assert subscriber.queue._request_queue.empty()

    @pytest.mark.asyncio
    async def test_handle_requests_error(self):
        """
        Test that the request handler stops on gRPC errors.
        """

        subscriber = Subscriber(
            Mock(spec=["client_id", "stub", "topics"]),
            [Topic(id=ULID(), name="topic")],
        )
        subscriber.stream = AsyncMock(spec=["write"])
        subscriber.stream.write.side_effect = grpc.aio.AioRpcError(None, None, None)

        # Write some requests to the queue
        requests = [
            ensign_pb2.SubscribeRequest(ack=ensign_pb2.Ack(id=b"1")),
            ensign_pb2.SubscribeRequest(nack=ensign_pb2.Nack(id=b"2")),
        ]
        for request in requests:
            await subscriber.queue.write_request(
                request,
            )

        # Consume the requests
        result = await subscriber.handle_requests()
        assert isinstance(result, grpc.aio.AioRpcError)

    @pytest.mark.asyncio
    async def test_handle_responses(self):
        subscriber = Subscriber(
            Mock(spec=["client_id", "stub", "topics"]),
            [Topic(id=ULID(), name="topic")],
        )
        topic_id = subscriber.topics[0].id
        responses = [
            ensign_pb2.SubscribeReply(event=wrap(Event(data=b"foo").proto(), topic_id)),
            ensign_pb2.SubscribeReply(event=wrap(Event(data=b"bar").proto(), topic_id)),
            ensign_pb2.SubscribeReply(close_stream=ensign_pb2.CloseStream()),
        ]

        subscriber.stream = AsyncMock(spec=["read"])
        subscriber.stream.read.side_effect = responses

        # Consume the responses
        await subscriber.handle_responses()

        # All responses should be in the queue
        assert await subscriber.queue.read_response() == responses[0].event
        assert await subscriber.queue.read_response() == responses[1].event
        assert subscriber.queue._response_queue.empty()

    @pytest.mark.asyncio
    async def test_handle_responses_bad_type(self):
        """
        Should raise an exceptyion if Ensign returns an unexpected message type.
        """

        subscriber = Subscriber(
            Mock(spec=["client_id", "stub", "topics"]),
            [Topic(id=ULID(), name="topic")],
        )
        subscriber.stream = AsyncMock(spec=["read"])
        subscriber.stream.read.side_effect = [ensign_pb2.PublisherReply()]

        with pytest.raises(EnsignTypeError):
            await subscriber.handle_responses()
