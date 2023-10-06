from enum import Enum
from pyensign.api.v1beta1 import topic_pb2


class TopicState(Enum):
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

    @classmethod
    def parse(cls, name):
        name = name.strip().upper()
        return cls[name]

    @classmethod
    def convert(cls, pb_val):
        pb_names = topic_pb2.DESCRIPTOR.enum_types_by_name["TopicState"].names_by_value
        return cls[pb_names[pb_val]]


class DeduplicationStrategy(Enum):

    UNKNOWN = topic_pb2.Deduplication.UNKNOWN
    NONE = topic_pb2.Deduplication.NONE
    STRICT = topic_pb2.Deduplication.STRICT
    DATAGRAM = topic_pb2.Deduplication.DATAGRAM
    KEY_GROUPED = topic_pb2.Deduplication.KEY_GROUPED
    UNIQUE_KEY = topic_pb2.Deduplication.UNIQUE_KEY
    UNIQUE_FIELD = topic_pb2.Deduplication.UNIQUE_FIELD

    @classmethod
    def parse(cls, name):
        name = name.strip().upper()
        return cls[name]

    @classmethod
    def convert(cls, pb_val):
        pb_names = topic_pb2.DESCRIPTOR.enum_types_by_name["Deduplication.Strategy"]
        return cls[pb_names.names_by_value[pb_val]]


class ShardingStrategy(Enum):

    UNKNOWN = topic_pb2.ShardingStrategy.UNKNOWN
    NO_SHARDING = topic_pb2.ShardingStrategy.NO_SHARDING
    CONSISTENT_KEY_HASH = topic_pb2.ShardingStrategy.CONSISTENT_KEY_HASH
    RANDOM = topic_pb2.ShardingStrategy.RANDOM
    PUBLISHER_ORDERING = topic_pb2.ShardingStrategy.PUBLISHER_ORDERING

    @classmethod
    def parse(cls, name):
        name = name.strip().upper()
        return cls[name]

    @classmethod
    def convert(cls, pb_val):
        pb_names = topic_pb2.DESCRIPTOR.enum_types_by_name["ShardingStrategy"]
        return cls[pb_names.names_by_value[pb_val]]
