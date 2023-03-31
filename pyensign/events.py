from pyensign.api.v1beta1 import event_pb2
from pyensign.mimetype.v1beta1 import mimetype_pb2

mimetypes = mimetype_pb2.DESCRIPTOR.enum_types_by_name["MIME"].values_by_name
mimetype_names = mimetypes.keys()


class Event:
    """
    Event is an abstraction of an Ensign protocol buffer Event to make it easier to
    create and parse events.
    """

    def __init__(self, data=None, mimetype=None):
        """
        Create a new Event from a mimetype and data.

        Parameters
        ----------
        data : bytes
            The data to use for the event.
        mimetype: str or int
            The mimetype of the data (e.g. "APPLICATION_JSON").
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

        self.data = data
        self.type = event_pb2.Type(name="Generic", version=1)

    def proto(self, topic_id):
        """
        Return the protocol buffer representation of the event.

        Parameters
        ----------
        topic_id : str
            The topic ID to use when publishing the event.

        Returns
        -------
        api.v1beta1.event_pb2.Event
            The protocol buffer representation of the event with the topic ID set.
        """

        if not topic_id:
            raise ValueError("no topic_id provided")

        return event_pb2.Event(
            topic_id=topic_id, data=self.data, mimetype=self.mimetype, type=self.type
        )
