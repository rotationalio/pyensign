import re
import json
import time
import pickle
import asyncio
from enum import Enum
from google.protobuf.timestamp_pb2 import Timestamp

from pyensign.ack import Ack
from pyensign.utils import pbtime
from pyensign import mimetypes as mtype
from pyensign.api.v1beta1 import ensign_pb2, event_pb2
from pyensign.exceptions import CouldNotAck, CouldNotNack, NackError


class Event:
    """
    Event is an abstraction of an Ensign protocol buffer Event to make it easier to
    create and parse events.
    """

    def __init__(
        self,
        data=None,
        mimetype=mtype.ApplicationJSON,
        schema_name="Unknown",
        schema_version="0.0.0",
        meta=None,
    ):
        """
        Create a new Event from a mimetype and data.

        Parameters
        ----------
        data : bytes
            The data to use for the event.
        mimetype: str or int
            The mimetype of the data (e.g. "application/json").
        schema_name: str (optional, default: "Unknown")
            The name of the user-defined schema that the event data conforms to.
            Subscribers consuming the event can use the schema to validate and parse
            the event data.
        schema_version: str (optional, default: "0.0.0")
            The semantic version string of the user-defined schema.
        meta: dict (optional)
            A set of key-value pairs to associate with the event.
        """

        if not data:
            raise ValueError("no data provided")

        if mimetype is None:
            raise ValueError("no mimetype provided")

        schema_name = schema_name.strip()
        if schema_name == "":
            raise ValueError("schema name cannot be empty")

        self.mimetype = mtype.parse(mimetype)

        # Fields that the user may want to modify after creation.
        self.data = data
        self.meta = meta
        self.type = Type(
            name=schema_name,
            version=schema_version,
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
            type=self.type.proto(),
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
        CouldNotAck
            If the Event was not received by a subscriber.
        """

        if self._state == EventState.ACKED:
            return True
        elif self._state == EventState.NACKED:
            return False
        elif (
            self._state == EventState.INITIALIZED or self._state == EventState.PUBLISHED
        ):
            raise CouldNotAck("Event has not been received by a subscriber")

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
        CouldNotNack
            If the Event was not received by a subscriber.
        """

        if self._state == EventState.NACKED:
            return True
        elif self._state == EventState.ACKED:
            return False
        elif (
            self._state == EventState.INITIALIZED or self._state == EventState.PUBLISHED
        ):
            raise CouldNotNack("Event has not been received by a subscriber")

        await self._stream.write_request(
            ensign_pb2.SubscribeRequest(nack=ensign_pb2.Nack(id=self.id, code=code))
        )
        self._state = EventState.NACKED
        return True

    async def wait_for_ack(self):
        """
        Wait for the event to be acked or nacked. If the event was acked this returns
        the ack. If the event was nacked this raises a NackError.
        """

        while not self.acked() and not self.nacked():
            await asyncio.sleep(0.1)

        if self.nacked():
            raise NackError(self.error.code, self.error.error)

        return Ack(self.id, self.committed)

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

    def __repr__(self):
        repr = "Event("
        repr += "data={}, ".format(self.data)
        repr += "mimetype={}, ".format(self.mimetype)
        repr += "schema_name={}, ".format(self.type.name)
        repr += "schema_version={}, ".format(self.type.semver())
        if self.meta:
            repr += "meta={}".format(self.meta)
        repr += ")"
        return repr

    def __str__(self):
        s = "Event:"
        if self.id:
            s += "\n\tid: {}".format(self.id)
        data = str(self.data)
        if len(data) > 100:
            data = data[:100] + "..."
        s += "\n\tdata: {}".format(data)
        s += "\n\tmimetype: {}".format(mtype.to_str(self.mimetype))
        s += "\n\tschema: {}".format(self.type)
        if self.meta:
            s += "\n\tmeta: {}".format(self.meta)
        s += "\n\tstate: {}".format(self._state)
        s += "\n\tcreated: {}".format(str(pbtime.to_datetime(self.created)))
        if self.committed:
            s += "\n\tcommitted: {}".format(str(pbtime.to_datetime(self.committed)))
        if self.error:
            s += "\n\terror: {}".format(self.error)
        return s


class Type:
    """
    Type is a high level abstraction over the protocol buffer type that represents an
    event schema.
    """

    def __init__(self, name, version=None):
        """
        Create a new schema Type from the name and semantic version.

        Parameters
        ----------
        name : str
            The name of the schema.

        version : str (optional)
            The semantic version of the schema which must be parseable as a semver
            2.0.0 string (e.g. "1.0.0").
        """

        self.name = name
        if version:
            self.parse_version(version)

    semver_pattern = re.compile(
        r"^v*(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)(?:-(?P<prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+(?P<buildmetadata>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
    )

    def parse_version(self, version):
        """
        Parse a semver 2.0.0 version string into the major, minor, and patch components.
        Pre-release and build metadata are ignored. See https://semver.org/ and
        https://regex101.com/r/Ly7O1x/3/ for more on parsing.
        """

        match = Type.semver_pattern.search(version)
        if not match:
            raise ValueError("unparseable semantic version: {}".format(version))
        results = match.groupdict()
        self.major_version = int(results["major"])
        self.minor_version = int(results["minor"])
        self.patch_version = int(results["patch"])

    def semver(self):
        """
        Return the semver 2.0.0 representation of the version.
        """

        return "{}.{}.{}".format(
            self.major_version, self.minor_version, self.patch_version
        )

    def proto(self):
        """
        Return the protocol buffer representation of the schema type.
        """

        return event_pb2.Type(
            name=self.name,
            major_version=self.major_version,
            minor_version=self.minor_version,
            patch_version=self.patch_version,
        )

    def __repr__(self):
        return "Type(name={}, version={})".format(self.name, self.semver())

    def __str__(self):
        return "{} v{}".format(self.name, self.semver())


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


def from_object(obj, mimetype=None, encoder=None):
    """
    Makes a best effort attempt to convert a Python object into an Event. If an encoder is provided, it will be used to encode the object to bytes. If no encoder
    is provided, the object will be encoded according to its type or mimetype.

    Parameters
    ----------
    obj : object
        The object to convert into an Event.

    mimetype : str or int (optional)
        The mimetype of the object. If not provided, the mimetype will be inferred from
        the object type.

    encoder : class (optional)
        An encoder with an `encode()` method that encodes the object into bytes. If not
        provided, the object will be encoded according to its type or the provided
        mimetype.
    """

    if mimetype:
        mimetype = mtype.parse(mimetype)

    if encoder:
        # Use the provided encoder to encode the object
        if not mimetype:
            raise ValueError("mimetype must be provided if encoder is provided")
        if not hasattr(encoder, "encode"):
            raise ValueError("encoder must have an encode() method")
        data = encoder.encode(obj)
    elif isinstance(obj, bytes):
        data = obj
        if not mimetype:
            mimetype = mtype.ApplicationOctetStream
    elif isinstance(obj, str):
        data = obj.encode("utf-8")
        if not mimetype:
            mimetype = mtype.TextPlain
    elif mimetype == mtype.ApplicationPythonPickle:
        data = pickle.dumps(obj)
    else:
        # Default case: assume the object is JSON serializable
        data = json.dumps(obj).encode("utf-8")
        if not mimetype:
            mimetype = mtype.ApplicationJSON

    return Event(data=data, mimetype=mimetype)


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

    # Convert the schema type from the protocol buffer
    event.type = Type(
        name=proto.type.name,
    )
    event.type.major_version = proto.type.major_version
    event.type.minor_version = proto.type.minor_version
    event.type.patch_version = proto.type.patch_version
    event.created = proto.created
    return event


async def ack_event(event):
    await event.ack()
