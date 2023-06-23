import os
from unittest import mock

import pytest
from asyncmock import patch

from pyensign.subscriber import Subscriber
from pyensign.exceptions import UnknownTopicError


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
        mock_subscribe.return_value = None

        subscriber = Subscriber(
            "topic", client_id="client_id", client_secret="client_secret"
        )
        assert subscriber.run() is None
        mock_subscribe.assert_called_once_with(("topic",))

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

        mock_subscribe.return_value = None

        class MySubscriber(Subscriber):
            async def on_event(self, event):
                event.ack()

            def consume(self):
                self.run(on_event=self.on_event)

        subscriber = MySubscriber(
            "otters",
            "lighthouses",
            client_id="client_id",
            client_secret="client_secret",
        )
        assert subscriber.consume() is None
        mock_subscribe.assert_called_once_with(
            ("otters", "lighthouses"), on_event=subscriber.on_event
        )
