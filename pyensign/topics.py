from pyensign.utils import ulid
from pyensign.events import Type


class Topic(object):
    """
    A Topic is a stream of ordered events. Topics have a human readable name but also
    have a unique ID.
    """

    def __init__(self, id=None, name="", topic_str=""):
        if id:
            self.id = ulid.parse(id)
        else:
            self.id = None
        self.name = name
        self.topic_str = topic_str
        self.project_id = None
        self.event_offset_id = None
        self.events = 0
        self.duplicates = 0
        self.data_size_bytes = 0
        self.types = []

    def __hash__(self):
        if self.id is None:
            raise ValueError("cannot hash topic with no ID")
        return hash(str(self.id))

    def __eq__(self, other):
        return self.id == other.id

    def __repr__(self):
        repr = "Topic("
        if self.id:
            repr += "id={}, ".format(self.id)
        if self.name:
            repr += "name={}, ".format(self.name)
        if self.topic_str:
            repr += "topic_str={}".format(self.topic_str)
        repr += ")"
        return repr

    def __str__(self):
        s = "Topic"
        if self.id:
            s += "\n\tid={}".format(self.id)
        if self.name:
            s += "\n\tname={}".format(self.name)
        if self.project_id:
            s += "\n\tproject_id={}".format(self.project_id)
        if self.event_offset_id:
            s += "\n\tevent_offset_id={}".format(self.event_offset_id)
        if self.topic_str:
            s += "\n\ttopic_str={}".format(self.topic_str)
        s += "\n\tevents={}".format(self.events)
        s += "\n\tduplicates={}".format(self.duplicates)
        s += "\n\tdata_size_bytes={}".format(self.data_size_bytes)
        s += "\n\ttypes:{}".format(len(self.types))
        for type in self.types:
            s += "\n\t"
            s += str(type).replace("\t", "\t\t")
        return s

    @classmethod
    def from_info(cls, pb_val):
        """
        Convert a protocol buffer TopicInfo into a Topic.
        """

        topic = cls(id=pb_val.topic_id)
        topic.project_id = ulid.parse(pb_val.project_id)
        topic.event_offset_id = pb_val.event_offset_id
        topic.events = pb_val.events
        topic.duplicates = pb_val.duplicates
        topic.data_size_bytes = pb_val.data_size_bytes
        for type in pb_val.types:
            topic.types.append(EventType.from_info(type))
        return topic


class EventType(object):
    """
    An EventType represents a type of event that was published to a topic, which
    includes the schema type and the MIME type.
    """

    def __init__(self, type, mimetype):
        self.type = type
        self.mimetype = mimetype
        self.events = 0
        self.duplicates = 0
        self.data_size_bytes = 0

    def __repr__(self):
        repr = "EventType("
        repr += "type={}".format(self.type)
        repr += ", mimetype={}".format(self.mimetype)
        repr += ")"
        return repr

    def __str__(self):
        s = "EventType"
        s += "\n\ttype={}".format(self.type)
        s += "\n\tmimetype={}".format(self.mimetype)
        s += "\n\tevents={}".format(self.events)
        s += "\n\tduplicates={}".format(self.duplicates)
        s += "\n\tdata_size_bytes={}".format(self.data_size_bytes)
        return s

    @classmethod
    def from_info(cls, pb_val):
        """
        Convert a protocol buffer EventType into an EventType.
        """

        type = cls(Type.convert(pb_val.type), pb_val.mimetype)
        type.events = pb_val.events
        type.duplicates = pb_val.duplicates
        type.data_size_bytes = pb_val.data_size_bytes
        return type
