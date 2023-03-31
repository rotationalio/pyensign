import pytest

from pyensign.events import Event
from pyensign.events import mimetypes
from pyensign.mimetype.v1beta1.mimetype_pb2 import MIME


class TestEvent:
    """
    Test cases for the Event helper class.
    """

    @pytest.mark.parametrize(
        "mimetype, expected",
        [
            (MIME.APPLICATION_JSON, MIME.APPLICATION_JSON),
            (MIME.TEXT_PLAIN, MIME.TEXT_PLAIN),
            (MIME.TEXT_HTML, MIME.TEXT_HTML),
            ("APPLICATION_JSON", MIME.APPLICATION_JSON),
            ("TEXT_PLAIN", MIME.TEXT_PLAIN),
            ("TEXT_HTML", MIME.TEXT_HTML),
        ],
    )
    def test_event(self, mimetype, expected):
        data = b"test"
        event = Event(data=data, mimetype=mimetype)
        assert event.data == data
        assert event.mimetype == expected
        assert event.type.name == "Generic"
        assert event.type.version == 1

        proto = event.proto(topic_id="topic")
        assert proto.topic_id == "topic"
        assert proto.data == data
        assert proto.mimetype == expected
        assert proto.type.name == "Generic"
        assert proto.type.version == 1
