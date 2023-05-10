from ulid import ULID

from pyensign.api.v1beta1 import event_pb2


def wrap(event, topic_id):
    """
    Wrap an event and topic ULID into an EventWrapper.
    """

    if not isinstance(event, event_pb2.Event):
        raise TypeError("event must be a protobuf event")

    if not isinstance(topic_id, ULID):
        raise TypeError("topic_id must be a ULID")

    return event_pb2.EventWrapper(
        event=event.SerializeToString(), topic_id=topic_id.bytes
    )


def unwrap(event_wrapper):
    """
    Unwrap an event from an EventWrapper.
    """

    if event_wrapper.event is None or len(event_wrapper.event) == 0:
        raise AttributeError("event wrapper contains no event")

    event = event_pb2.Event()
    event.ParseFromString(event_wrapper.event)
    return event
