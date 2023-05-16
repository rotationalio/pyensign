import time
from google.protobuf.timestamp_pb2 import Timestamp

from pyensign import mimetypes as mtype
from pyensign.api.v1beta1 import event_pb2


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
