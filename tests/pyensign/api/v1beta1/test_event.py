import time
import pytest
from ulid import ULID
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.message import DecodeError

from pyensign.api.v1beta1 import event_pb2
from pyensign.api.v1beta1.event import wrap, unwrap
from pyensign.mimetype.v1beta1.mimetype_pb2 import MIME


@pytest.fixture
def event():
    return event_pb2.Event(
        data=b"some data",
        metadata={"key": "value"},
        mimetype=MIME.APPLICATION_JSON,
        type=event_pb2.Type(
            name="Generic",
            major_version=1,
            minor_version=2,
            patch_version=3,
        ),
        created=Timestamp(seconds=int(time.time())),
    )


def test_wrap(event):
    """
    Test wrapping an Event and topic ULID into an EventWrapper.
    """

    topic_id = ULID()
    ew = wrap(event, topic_id)
    assert isinstance(ew, event_pb2.EventWrapper)
    assert ew.event == event.SerializeToString()
    assert ew.topic_id == topic_id.bytes

    event = event_pb2.Event()
    ew = wrap(event, topic_id)


def test_wrap_error(event):
    """
    Test wrapping an Event raises the correct exception when the event is not a
    protobuf event.
    """

    with pytest.raises(TypeError):
        wrap("notanevent", ULID())


def test_unwrap(event):
    """
    Test unwrapping an event from an EventWrapper.
    """

    ew = event_pb2.EventWrapper(event=event.SerializeToString())
    unwrapped = unwrap(ew)
    assert isinstance(event, event_pb2.Event)
    assert event == unwrapped


@pytest.mark.parametrize(
    "event_wrapper, exception",
    [
        (event_pb2.EventWrapper(), AttributeError),
        (event_pb2.EventWrapper(event=b"notanevent"), DecodeError),
        (event_pb2.EventWrapper(event=None), AttributeError),
        (event_pb2.EventWrapper(event=b""), AttributeError),
    ],
)
def test_unwrap_error(event_wrapper, exception):
    """
    Test unwrapping an event raises the correct exception when the event is not
    deserializable.
    """

    with pytest.raises(exception):
        unwrap(event_wrapper)
