import time
from google.protobuf.timestamp_pb2 import Timestamp

from pyensign.api.v1beta1 import event_pb2
from pyensign.mimetype.v1beta1 import mimetype_pb2

mimetypes = mimetype_pb2.DESCRIPTOR.enum_types_by_name["MIME"].values_by_name
mimetype_names = mimetypes.keys()


class Event:
    """
    Event is an abstraction of an Ensign protocol buffer Event to make it easier to
    create and parse events.
    """

    def __init__(self, data=None, mimetype=None, id="", meta={}):
        """
        Create a new Event from a mimetype and data.

        Parameters
        ----------
        data : bytes
            The data to use for the event.
        mimetype: str or int
            The mimetype of the data (e.g. "APPLICATION_JSON").
        id : str (optional)
            A user-defined ID for the event.
        meta: dict (optional)
            A set of key-value pairs to associate with the event.
        """

        if not data:
            raise ValueError("no data provided")

        if not mimetype:
            raise ValueError("no mimetype provided")

        # TODO: Support traditionally formatted mimetypes (e.g. application/json)
        if isinstance(mimetype, str):
            if mimetype not in mimetypes:
                raise ValueError(
                    "invalid mimetype, specify one of: {}".format(mimetype_names)
                )
            self.mimetype = mimetypes[mimetype].number
        else:
            self.mimetype = mimetype

        # Fields that the user may want to modify after creation.
        self.id = id
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
            user_defined_id=self.id,
            data=self.data,
            metadata=self.meta,
            mimetype=self.mimetype,
            type=self.type,
            created=self.created,
        )
