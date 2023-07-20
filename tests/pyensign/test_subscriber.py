import os
from unittest import mock

import pytest
from asyncmock import patch

from pyensign.events import Event
from pyensign.utils.queue import BidiQueue
from pyensign.subscriber import Subscriber
from pyensign.exceptions import UnknownTopicError


def async_iter(items):
    async def next():
        for item in items:
            yield item

    return next()


class TestSubscriber:
    """
    Tests for the high-level Subscriber class.
    """

    @pytest.mark.parametrize(
        "topics, kwargs, exception",
        [
            ("topic", {"client_id": "id"}, ValueError),
            ("", {"client_id": "id", "client_secret": "secret"}, ValueError),
        ],
    )
    def test_bad_params(self, topics, kwargs, exception):
        # Should raise an exception if no topic is provided or there are insufficient
        # credentials to connect to Ensign.
        with pytest.raises(exception), mock.patch.dict(os.environ, {}, clear=True):
            Subscriber(*topics, **kwargs)

    @patch("pyensign.ensign.Ensign.subscribe")
    def test_run(self, mock_subscribe):
        # Create a fake event for the subscriber to receive.
        events = [Event(data=b"event1", mimetype="text/plain")]
        events[0].mark_subscribed(None, BidiQueue())
        mock_subscribe.return_value = async_iter(events)

        subscriber = Subscriber(
            "topic", client_id="client_id", client_secret="client_secret"
        )
        subscriber.run()
        mock_subscribe.assert_called_once_with(("topic",))
        assert events[0].acked()

    @patch("pyensign.ensign.Ensign.subscribe")
    def test_run_exception(self, mock_subscribe):
        mock_subscribe.side_effect = UnknownTopicError("topic not found")

        subscriber = Subscriber(
            "topic", client_id="client_id", client_secret="client_secret"
        )
        with pytest.raises(UnknownTopicError):
            subscriber.run()

    @patch("pyensign.ensign.Ensign.subscribe")
    def test_run_inherit(self, mock_subscribe):
        """
        Should be able to use the Subscriber as a base class.
        """

        class MockQueue:
            async def write_request(self, request):
                pass

        # Create a fake event for the subscriber to receive.
        events = [Event(data=b"event1", mimetype="text/plain")]
        events[0].mark_subscribed(None, MockQueue())
        mock_subscribe.return_value = async_iter(events)

        class MySubscriber(Subscriber):
            async def on_event(self, event):
                await event.nack(100)

        subscriber = MySubscriber(
            "otters",
            "lighthouses",
            client_id="client_id",
            client_secret="client_secret",
        )
        subscriber.run()
        mock_subscribe.assert_called_once_with(("otters", "lighthouses"))
        assert events[0].nacked()
