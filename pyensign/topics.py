from pyensign.utils import ulid
from pyensign.events import Type
from pyensign.utils import pbtime
from pyensign.enum import TopicState, DeduplicationStrategy, OffsetPosition


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
        self.offset = 0
        self.status = None
        self.deduplication = None
        self.types = []
        self.created = None
        self.modified = None

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
        if self.status:
            s += "\n\tstatus={}".format(self.status)
        if self.project_id:
            s += "\n\tproject_id={}".format(self.project_id)
        if self.event_offset_id:
            s += "\n\tevent_offset_id={}".format(self.event_offset_id)
        if self.topic_str:
            s += "\n\ttopic_str={}".format(self.topic_str)
        s += "\n\tevents={}".format(self.events)
        s += "\n\tduplicates={}".format(self.duplicates)
        s += "\n\tdata_size_bytes={}".format(self.data_size_bytes)
        s += "\n\toffset={}".format(self.offset)
        s += "\n\ttypes:{}".format(len(self.types))
        for type in self.types:
            s += "\n\t"
            s += str(type).replace("\t", "\t\t")
        if self.created:
            s += "\n\tcreated={}".format(self.created)
        if self.modified:
            s += "\n\tmodified={}".format(self.modified)
        return s

    @classmethod
    def from_proto(cls, pb_val):
        """
        Convert a protocol buffer Topic into a Topic.
        """

        topic = cls(id=pb_val.id, name=pb_val.name)
        topic.project_id = ulid.parse(pb_val.project_id)
        topic.offset = pb_val.offset
        topic.status = TopicState.convert(pb_val.status)
        topic.deduplication = Deduplication.convert(pb_val.deduplication)
        topic.created = pbtime.to_datetime(pb_val.created)
        topic.modified = pbtime.to_datetime(pb_val.modified)
        return topic

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


class Deduplication(object):
    """
    Deduplication stores information about how a topic is deduplicated, including the
    configured deduplication strategy and offset position.
    """

    def __init__(
        self,
        strategy=DeduplicationStrategy.NONE,
        offset=OffsetPosition.OFFSET_EARLIEST,
        keys=None,
        fields=None,
        overwrite_duplicate=False,
    ):
        self.strategy = strategy
        self.offset = offset
        self.keys = keys
        self.fields = fields
        self.overwrite_duplicate = overwrite_duplicate

    def __repr__(self):
        repr = "Deduplication("
        repr += "strategy={}, ".format(self.strategy)
        repr += "offset={},".format(self.offset)
        repr += "keys={}, ".format(self.keys)
        repr += "fields={}, ".format(self.fields)
        repr += "overwrite_duplicate={}".format(self.overwrite_duplicate)
        repr += ")"
        return repr

    def __str__(self):
        s = "Deduplication"
        s += "\n\tstrategy={}".format(self.strategy)
        s += "\n\toffset={}".format(self.offset)
        s += "\n\tkeys={}".format(self.keys)
        s += "\n\tfields={}".format(self.fields)
        s += "\n\toverwrite_duplicate={}".format(self.overwrite_duplicate)
        return s

    @classmethod
    def convert(cls, pb_val):
        """
        Convert a protocol buffer Deduplication into a Deduplication.
        """

        dedup = cls(
            strategy=DeduplicationStrategy.convert(pb_val.strategy),
            offset=OffsetPosition.convert(pb_val.offset),
            keys=pb_val.keys,
            fields=pb_val.fields,
            overwrite_duplicate=pb_val.overwrite_duplicate,
        )
        return dedup


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
