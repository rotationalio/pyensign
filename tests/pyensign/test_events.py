import pytest

from pyensign.events import Event
from pyensign.events import mimetypes
from pyensign.mimetype.v1beta1.mimetype_pb2 import MIME
from pyensign.api.v1beta1 import event_pb2


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
        assert event.type == event_pb2.Type(
            name="Generic",
            major_version=1,
            minor_version=0,
            patch_version=0,
        )
        assert event.created.seconds > 0
        assert event._proto == event_pb2.Event(
            data=data,
            mimetype=expected,
            type=event.type,
            created=event.created,
        )

    def test_modify_event(self):
        """
        Ensure events can be modified after creation and updates are present in the
        protobuf.
        """

        event = Event(data=b"test", mimetype=MIME.TEXT_PLAIN)
        proto = event._proto
        assert proto.data == b"test"
        assert proto.mimetype == MIME.TEXT_PLAIN
        assert proto.type == event_pb2.Type(
            name="Generic",
            major_version=1,
            minor_version=0,
            patch_version=0,
        )
        assert proto.created.seconds > 0

        event.meta["key"] = "value"
        modified = event.proto()
        assert modified.data == proto.data
        assert modified.mimetype == proto.mimetype
        assert modified.type == proto.type
        assert modified.created == proto.created
        assert modified.metadata == {"key": "value"}

    def test_bad_event(self):
        """
        Ensure an Event can't be created if it can't be converted to a protobuf.
        """

        with pytest.raises(TypeError):
            Event(data="notbytes", mimetype=MIME.TEXT_PLAIN)
