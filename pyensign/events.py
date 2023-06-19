import time
from enum import Enum
from google.protobuf.timestamp_pb2 import Timestamp

from pyensign import mimetypes as mtype
from pyensign.api.v1beta1 import ensign_pb2, event_pb2
from pyensign.exceptions import AckError, NackError


class Event:
    """
    Event is an abstraction of an Ensign protocol buffer Event to make it easier to
    create and parse events.
    """

    def __init__(self, data=None, mimetype=mtype.ApplicationJSON, meta={}):
        """
        Create a new Event from a mimetype and data.

        Parameters
        ----------
        data : bytes
            The data to use for the event.
        mimetype: str or int
            The mimetype of the data (e.g. "application/json").
        meta: dict (optional)
            A set of key-value pairs to associate with the event.
        """

        if not data:
            raise ValueError("no data provided")

        if mimetype is None:
            raise ValueError("no mimetype provided")

        self.mimetype = mtype.parse(mimetype)

        # Fields that the user may want to modify after creation.
        self.data = data
        self.meta = meta
        self.type = event_pb2.Type(
            name="Generic", major_version=1, minor_version=0, patch_version=0
        )
        self.created = Timestamp(seconds=int(time.time()))

        # Ensure that the protocol buffer representation is valid at the point of
        # creation.
        self._proto = self.proto()

        self.id = None
        self.committed = None
        self.error = None
        self._stream = None
        self._state = EventState.INITIALIZED

    def proto(self):
        """
        Return the protocol buffer representation of the event.

        Returns
        -------
        api.v1beta1.event_pb2.Event
            The protocol buffer representation of the event.
        """

        # TODO: Should we raise type errors and missing field errors to help the user?
        return event_pb2.Event(
            data=self.data,
            metadata=self.meta,
            mimetype=self.mimetype,
            type=self.type,
            created=self.created,
        )

    def acked(self):
        """
        Returns True if the event has been acked.
        """

        return self._state == EventState.ACKED

    def nacked(self):
        """
        Returns True if the event has been nacked.
        """

        return self._state == EventState.NACKED

    def nack_error(self):
        """
        Return the error if the event was nacked.
        """

        return self.error

    async def ack(self):
        """
        Send an acknowledgement back to the Ensign server to indicate that the Event
        has been successfully consumed. For consumer groups that have exactly-once or
        at-least-once delivery guarantees, this signals that the message has been
        delivered successfully to avoid redelivery of the message to another consumer.

        Returns
        -------
        True
            If the Event was already acked.
        False
            If a nack was sent before the ack.

        Raises
        ------
        AckError
            If the Event was not received by a subscriber.
        """

        if self._state == EventState.ACKED:
            return True
        elif self._state == EventState.NACKED:
            return False
        elif (
            self._state == EventState.INITIALIZED or self._state == EventState.PUBLISHED
        ):
            raise AckError("Event has not been received by a subscriber")

        await self._stream.write_request(
            ensign_pb2.SubscribeRequest(ack=ensign_pb2.Ack(id=self.id))
        )
        self._state = EventState.ACKED
        return True

    async def nack(self, code):
        """
        Send a negative acknowledgement back to the Ensign server to indicate that the
        Event has not been successfully consumed. For consumer groups that have
        exactly-once or at-least-once delivery guarantees, this signals that the
        message should be delivered to another consumer.

        Parameters
        ----------
        code : int
            The error code to send back to the server.

        Returns
        -------
        True
            If the Event was already nacked.
        False
            If an ack was sent before the nack.

        Raises
        ------
        NackError
            If the Event was not received by a subscriber.
        """

        if self._state == EventState.NACKED:
            return True
        elif self._state == EventState.ACKED:
            return False
        elif (
            self._state == EventState.INITIALIZED or self._state == EventState.PUBLISHED
        ):
            raise NackError("Event has not been received by a subscriber")

        await self._stream.write_request(
            ensign_pb2.SubscribeRequest(nack=ensign_pb2.Nack(id=self.id, code=code))
        )
        self._state = EventState.NACKED
        return True

    def mark_published(self):
        self._state = EventState.PUBLISHED

    def mark_acked(self, ack):
        self.id = ack.id
        self.committed = ack.committed
        self._state = EventState.ACKED

    def mark_nacked(self, nack):
        self.error = nack
        self._state = EventState.NACKED

    def mark_subscribed(self, id, ackback_stream):
        self.id = id
        self._stream = ackback_stream
        self._state = EventState.SUBSCRIBED


class EventState(Enum):
    # Event has been created but not published
    INITIALIZED = 0
    # Event has been published but not acked by the server
    PUBLISHED = 1
    # Event has been received by subscriber but not acked by the user
    SUBSCRIBED = 2
    # Event has been acked by a user or the server
    ACKED = 3
    # Event has been nacked by a user or the server
    NACKED = 4


def from_proto(proto):
    """
    Convert a protocol buffer event into an Event.

    Parameters
    ----------
    proto : api.v1beta1.event_pb2.Event
        The protocol buffer event to convert.

    Returns
    -------
    Event
        The converted event.
    """

    event = Event(
        data=proto.data,
        mimetype=proto.mimetype,
        meta=proto.metadata,
    )
    event.type = proto.type
    event.created = proto.created
    return event


async def ack_event(event):
    await event.ack()
