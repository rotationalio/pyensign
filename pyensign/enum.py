from enum import Enum
from pyensign.api.v1beta1 import topic_pb2


class PBEnum(Enum):
    """
    Base class for Python enumerations that map to protocol buffer enumeration
    descriptors, providing methods for parsing and converting values.
    """

    @classmethod
    def parse(cls, name):
        """
        Parse an protocol buffer enumeration value from a string (case and whitespace
        insensitive parsing). Raises a KeyError if the string is not in the enumeration.
        """
        name = name.strip().upper()
        return cls[name]

    @classmethod
    def convert(cls, pb_val, name):
        """
        Convert a protocol buffer descriptor or integer to the enumeration value.
        """
        pb_names = topic_pb2.DESCRIPTOR.enum_types_by_name[name].values_by_number
        return cls[pb_names[pb_val].name]


class TopicState(PBEnum):
    """
    TopicState enumerates the possible states that a topic might be in.

    States
    ------
    UNDEFINED : 0
        A topic should never be in this state, and indicates something has gone wrong.

    READY : 1
        The topic is ready to receive published events and subscribers.

    READONLY : 2
        The topic has been archived and can only be queried.

    DELETING : 3
        The topic has been destroyed and is removing past events, once all events have
        been destroyed, the topic itself will be destroyed and subsequent queries will
        return a not found error.

    PENDING : 4
        The topic will soon be changing state to one that may not be accessible such as
        the DELETING, ALLOCATING, or REPAIRING states.

    ALLOCATING : 5
        The topic is being assigned to nodes and is not ready to receive events.

    REPAIRING : 6
        The topic is repairing itself, e.g. updating a sharding or deduplication policy.
        The topic may be available to receive published events and subscribers depending
        on the repair operation that it is conducting.
    """

    UNDEFINED = topic_pb2.TopicState.UNDEFINED
    READY = topic_pb2.TopicState.READY
    READONLY = topic_pb2.TopicState.READONLY
    DELETING = topic_pb2.TopicState.DELETING
    PENDING = topic_pb2.TopicState.PENDING
    ALLOCATING = topic_pb2.TopicState.ALLOCATING
    REPAIRING = topic_pb2.TopicState.REPAIRING

    __aliases__ = {}

    @classmethod
    def convert(cls, pb_val):
        return super(TopicState, cls).convert(pb_val, "TopicState")


class DeduplicationStrategy(PBEnum):
    """
    DeduplicationStrategy enumerates the possible deduplication mechanisms for
    determining if two events are duplicates of each other.

    Strategies
    ----------
    UNKNOWN : 0
        A topic should never have this policy, indicating something has gone wrong.

    NONE : 1
        No deduplication is performed, all events are stored when published.

    STRICT : 2
        Deduplication considers events with identical data, metadata, mimetype, and type
        as being duplicates. All data is compared as is in a case-sensitive form.

    DATAGRAM : 3
        Deduplication considers events with identical data as being duplicates
        regardless of the metadata, mimetype, or type.

    KEY_GROUPED : 4
        A deduplication policy that compares the data of two events so long as they have
        the same values for specified keys (keys are required). E.g. an event is not a
        duplicate if the metadata contains different "color" keys but identical data.

    UNIQUE_KEY : 5
        A deduplication policy that only compares the set of specified keys in the
        metadata and does not consider data, mimetype, or type.

    UNIQUE_FIELD : 6
        A deduplication policy that only compares the set of specified fields in the
        data. The data must be in a parseable representation by Ensign for this policy.
    """

    UNKNOWN = topic_pb2.Deduplication.UNKNOWN
    NONE = topic_pb2.Deduplication.NONE
    STRICT = topic_pb2.Deduplication.STRICT
    DATAGRAM = topic_pb2.Deduplication.DATAGRAM
    KEY_GROUPED = topic_pb2.Deduplication.KEY_GROUPED
    UNIQUE_KEY = topic_pb2.Deduplication.UNIQUE_KEY
    UNIQUE_FIELD = topic_pb2.Deduplication.UNIQUE_FIELD

    @classmethod
    def convert(cls, pb_val):
        """
        Convert a protocol buffer descriptor or integer to the enumeration value.
        """
        return super(DeduplicationStrategy, cls).convert(pb_val, "Deduplication.Strategy")


class OffsetPosition(PBEnum):
    """
    OffsetPosition determines where duplicates are stored in the log.

    Positions
    ---------
    OFFSET_UNKNOWN : 0
        Only valid if the deduplication policy is None.

    OFFSET_EARLIEST : 1
        The default deduplication offset policy: the first record in the log is
        identified as the canonical event and all duplicate events point to this record.

    OFFSET_LATEST : 2
        An offset policy where earlier duplicates are marked as duplicates and the
        latest event is identified as the canonical event. This policy slows down event
        processing, but allows queries to see duplicate values sooner.
    """

    OFFSET_UNKNOWN = topic_pb2.Deduplication.OFFSET_UNKNOWN
    OFFSET_EARLIEST = topic_pb2.Deduplication.OFFSET_EARLIEST
    OFFSET_LATEST = topic_pb2.Deduplication.OFFSET_LATEST

    __aliases__ = {
        "EARLIEST": "OFFSET_EARLIEST",
        "LATEST": "OFFSET_LATEST",
    }

    @classmethod
    def parse(cls, name):
        """
        Parse an protocol buffer enumeration value from a string (case and whitespace
        insensitive parsing). Raises a KeyError if the string is not in the enumeration.
        """
        name = name.strip().upper()
        if name in cls.__aliases__:
            name = cls.__aliases__[name]
        return cls[name]

    @classmethod
    def convert(cls, pb_val):
        """
        Convert a protocol buffer descriptor or integer to the enumeration value.
        """
        return super(OffsetPosition, cls).convert(pb_val, "Deduplication.OffsetPosition")

    @classmethod
    def __getitem__(cls, name):
        if name in cls.__aliases__:
            name = cls.__aliases__[name]
        return super(OffsetPosition, cls).__getitem__(name)


class ShardingStrategy(PBEnum):
    """
    ShardingStrategy specifies how events are distributed to multiple nodes.

    Strategies
    ----------
    UNKNOWN : 0
        This strategy is only valid if Ensign is in single node mode

    NO_SHARDING : 1
        Nodes will handle events that come to them without load balancing

    CONSISTENT_KEY_HASH : 2
        A sharding strategy that attempts to load balance events to different nodes by
        using a consistent key hash algorithm to determine by key where the event goes.

    RANDOM : 3
        A sharding strategy that attempts to load balance by randomly selecting a node.

    PUBLISHER_ORDERING : 4
        A publisher is assigned to a single node and will always publish to that node.
    """

    UNKNOWN = topic_pb2.ShardingStrategy.UNKNOWN
    NO_SHARDING = topic_pb2.ShardingStrategy.NO_SHARDING
    CONSISTENT_KEY_HASH = topic_pb2.ShardingStrategy.CONSISTENT_KEY_HASH
    RANDOM = topic_pb2.ShardingStrategy.RANDOM
    PUBLISHER_ORDERING = topic_pb2.ShardingStrategy.PUBLISHER_ORDERING

    @classmethod
    def convert(cls, pb_val):
        """
        Convert a protocol buffer descriptor or integer to the enumeration value.
        """
        return super(ShardingStrategy, cls).convert(pb_val, "ShardingStrategy")
