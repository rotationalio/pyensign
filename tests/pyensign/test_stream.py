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
from pyensign.exceptions import EnsignTimeoutError
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
        stream = StreamHandler(
            Mock(spec=["client_id", "stub", "topics"]),
            BidiQueue(),
            reconnect_tick=timedelta(milliseconds=1),
        )
        stream._responses = AsyncMock(spec=ResponseIterator)

        # Run the stream handler in a separate task, which should reconnect a few times
        task = asyncio.create_task(stream.run())
        while mock_connect.call_count < 3:
            await asyncio.sleep(0.01)

        # Close the stream handler which should gracefully shutdown the task.
        await stream.close()
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
        stream = StreamHandler(
            Mock(spec=["client_id", "stub", "topics"]),
            BidiQueue(),
            reconnect_tick=timedelta(milliseconds=1),
            reconnect_timeout=timedelta(milliseconds=5),
        )
        stream._responses = AsyncMock(spec=ResponseIterator)

        # Run the stream handler which should fail to reconnect
        with pytest.raises(EnsignTimeoutError):
            await stream.run()


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
            assert event._state == EventState.PUBLISHED
            req = await publisher.queue.read_request()
            assert isinstance(req, ensign_pb2.PublisherRequest)
            wrapper = req.event
            assert wrapper.topic_id == publisher.topic.id.bytes
            published = from_proto(unwrap(wrapper))
            assert published.data == event.data

        # All events should be pending ACKs
        assert len(publisher.pending) == len(events)


class TestSubscriber:
    """
    Tests for the Subscriber stream handler.
    """

    @pytest.mark.asyncio
    async def test_consume(self):
        acked_events = []

        async def ack_event(event):
            await event.ack()
            acked_events.append(event)

        subscriber = Subscriber(
            Mock(spec=["client_id", "stub", "topics"]),
            [Topic(id=ULID(), name="topic")],
            on_event=ack_event,
        )

        # Write some events to the queue
        events = [Event(data=b"foo %d" % i) for i in range(10)]
        for event in events:
            await subscriber.queue.write_response(
                wrap(event.proto(), subscriber.topics[0].id)
            )
        await subscriber.queue.close()

        # Consume the events
        await subscriber.consume()

        # Check that the events were ACKed
        assert len(acked_events) == len(events)
