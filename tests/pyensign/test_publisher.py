import pytest
from asyncmock import call, patch

from pyensign.publisher import Publisher
from pyensign.exceptions import UnknownTopicError


def async_iter(items):
    async def next():
        for item in items:
            yield item

    return next()


class TestPublisher:
    """
    Tests for the high-level Publisher class.
    """

    @pytest.mark.parametrize(
        "topic, kwargs, exception",
        [
            ("topic", {"client_id": "id"}, ValueError),
            (None, {"client_id": "id", "client_secret": "secret"}, ValueError),
            ("", {"client_id": "id", "client_secret": "secret"}, ValueError),
        ],
    )
    def test_bad_params(self, topic, kwargs, exception):
        with pytest.raises(exception):
            Publisher(topic, **kwargs)

    @patch("pyensign.ensign.Ensign.publish")
    def test_run(self, mock_publish):
        events = ["event1", "event2"]
        mock_publish.return_value = None

        publisher = Publisher(
            "topic", client_id="client_id", client_secret="client_secret"
        )
        assert publisher.run(iter(events)) is None
        mock_publish.assert_has_calls([call("topic", event) for event in events])
        assert publisher.run(async_iter(events)) is None
        mock_publish.assert_has_calls([call("topic", event) for event in events])

    @patch("pyensign.ensign.Ensign.publish")
    def test_run_exception(self, mock_publish):
        events = ["event1", "event2"]
        mock_publish.side_effect = UnknownTopicError("topic not found")

        publisher = Publisher(
            "topic", client_id="client_id", client_secret="client_secret"
        )
        with pytest.raises(UnknownTopicError):
            publisher.run(iter(events))
        with pytest.raises(UnknownTopicError):
            publisher.run(async_iter(events))

    @patch("pyensign.ensign.Ensign.publish")
    def test_run_inherit(self, mock_publish):
        """
        Should be able to use the Publisher as a base class.
        """

        mock_publish.return_value = None

        class MyPublisher(Publisher):
            def get_events(self):
                events = ["event1", "event2"]
                for event in events:
                    yield event

            def listen_and_publish(self):
                self.run(self.get_events())

        publisher = MyPublisher(
            "topic", client_id="client_id", client_secret="client_secret"
        )
        assert publisher.listen_and_publish() is None
        mock_publish.assert_has_calls(
            [call("topic", "event1"), call("topic", "event2")]
        )
