from random import randbytes
from datetime import datetime

import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from pyensign import nack
from pyensign import mimetypes as mt
from pyensign.utils.rlid import RLID
from pyensign.utils.queue import BidiQueue
from pyensign.events import Event, EventState, Type
from pyensign.api.v1beta1 import event_pb2, ensign_pb2
from pyensign.mimetype.v1beta1.mimetype_pb2 import MIME


class TestEvent:
    """
    Test cases for the Event helper class.
    """

    @pytest.mark.parametrize(
        "mimetype, schema_name, schema_version, expected_mimetype, major, minor, patch",
        [
            (
                MIME.APPLICATION_XML,
                "Document",
                "1.2.3",
                MIME.APPLICATION_XML,
                1,
                2,
                3,
            ),
            (
                MIME.TEXT_PLAIN,
                "Message",
                "0.1.0",
                MIME.TEXT_PLAIN,
                0,
                1,
                0,
            ),
            (
                MIME.TEXT_HTML,
                "Webpage",
                "v0.1.2",
                MIME.TEXT_HTML,
                0,
                1,
                2,
            ),
            (
                "application/json",
                "Status",
                "9.5.3",
                MIME.APPLICATION_JSON,
                9,
                5,
                3,
            ),
            (
                "text/plain",
                "note",
                "10.3.4",
                MIME.TEXT_PLAIN,
                10,
                3,
                4,
            ),
            (
                "text/html",
                "feed",
                "3.2.4",
                MIME.TEXT_HTML,
                3,
                2,
                4,
            ),
            (
                0,
                "update",
                "v8.3.45",
                MIME.APPLICATION_OCTET_STREAM,
                8,
                3,
                45,
            ),
            (
                100,
                "web page",
                "3.5.7",
                MIME.APPLICATION_XML,
                3,
                5,
                7,
            ),
        ],
    )
    def test_event(
        self,
        mimetype,
        schema_name,
        schema_version,
        expected_mimetype,
        major,
        minor,
        patch,
    ):
        data = b"test"
        event = Event(
            data=data,
            mimetype=mimetype,
            schema_name=schema_name,
            schema_version=schema_version,
        )
        assert event.data == data
        assert event.mimetype == expected_mimetype
        assert event.created.seconds > 0
        assert event._proto == event_pb2.Event(
            data=data,
            mimetype=expected_mimetype,
            type=event_pb2.Type(
                name=schema_name,
                major_version=major,
                minor_version=minor,
                patch_version=patch,
            ),
            created=event.created,
        )

    def test_modify_event(self):
        """
        Ensure events can be modified after creation and updates are present in the
        protobuf.
        """

        event = Event(data=b"test", mimetype=mt.TextPlain)
        proto = event._proto
        assert proto.data == b"test"
        assert proto.mimetype == MIME.TEXT_PLAIN
        assert proto.type == event_pb2.Type(
            name="Unknown",
            major_version=0,
            minor_version=0,
            patch_version=0,
        )
        assert proto.created.seconds > 0

        event.meta = {"key": "value"}
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
            Event(data="notbytes", mimetype=mt.TextPlain)

    def test_repr(self):
        event = Event(
            data=b"test",
            mimetype=mt.TextPlain,
            schema_name="Document",
            schema_version="1.2.3",
            meta={"key": "value"},
        )
        assert (
            repr(event)
            == "Event(data=b'test', mimetype=1, schema_name=Document, schema_version=1.2.3, meta={'key': 'value'})"
        )

    def test_str(self):
        event = Event(data=b"test", mimetype=mt.TextPlain)
        event.id = b"123"
        event.created = Timestamp(seconds=int(datetime(2023, 1, 1).timestamp()))
        event.committed = Timestamp(seconds=int(datetime(2023, 1, 2).timestamp()))
        event.error = "an error occurred"
        assert (
            str(event)
            == "Event:\n\tid: b'123'\n\tdata: b'test'\n\tmimetype: text/plain\n\tschema: Unknown v0.0.0\n\tstate: EventState.INITIALIZED\n\tcreated: 2023-01-01 00:00:00\n\tcommitted: 2023-01-02 00:00:00\n\terror: an error occurred"
        )

    def test_str_truncate(self):
        event = Event(data=b"test" * 100, mimetype=mt.TextPlain)
        assert len(str(event)) < 400

    @pytest.mark.asyncio
    async def test_nack_event(self):
        """
        Test nack-ing an event.
        """

        event = Event(data=b"test", mimetype=mt.Unknown)
        event.id = RLID(randbytes(10))
        event._state = EventState.SUBSCRIBED
        event._stream = BidiQueue()
        await event.nack(nack.UnknownType)

        assert event.nacked()
        req = await event._stream.read_request()
        assert isinstance(req, ensign_pb2.SubscribeRequest)
        assert req.nack.code == nack.UnknownType


class TestType:
    """
    Test cases for the Type helper class.
    """

    @pytest.mark.parametrize(
        "name, version, major, minor, patch, fullname",
        [
            ("otters", "1.4.2", 1, 4, 2, "otters v1.4.2"),
            ("lighthouses", "v0.0.4", 0, 0, 4, "lighthouses v0.0.4"),
            ("otters n' company", "1.2.3", 1, 2, 3, "otters n' company v1.2.3"),
            ("type", "10.20.30", 10, 20, 30, "type v10.20.30"),
            ("unknown", "1.1.2-prerelease+meta", 1, 1, 2, "unknown v1.1.2"),
            ("placeholder", "1.1.2+meta-valid", 1, 1, 2, "placeholder v1.1.2"),
            ("something", "1.1.2-alpha", 1, 1, 2, "something v1.1.2"),
            ("test", "1.1.2-beta", 1, 1, 2, "test v1.1.2"),
            ("foo", "1.1.2-alpha.1", 1, 1, 2, "foo v1.1.2"),
            ("bar baz", "1.1.2-rc.1+build.123", 1, 1, 2, "bar baz v1.1.2"),
            (
                "tbd",
                "999999999.999999999.999999999",
                999999999,
                999999999,
                999999999,
                "tbd v999999999.999999999.999999999",
            ),
        ],
    )
    def test_type(self, name, version, major, minor, patch, fullname):
        t = Type(name, version)
        proto = t.proto()
        assert proto.name == name
        assert proto.major_version == major
        assert proto.minor_version == minor
        assert proto.patch_version == patch
        assert str(t) == fullname

    @pytest.mark.parametrize(
        "version",
        [
            ("1"),
            ("1.2"),
            ("abc"),
        ],
    )
    def test_bad_version(self, version):
        with pytest.raises(ValueError):
            Type("foo", version)
