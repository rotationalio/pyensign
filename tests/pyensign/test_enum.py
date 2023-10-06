import pytest

from pyensign.api.v1beta1 import topic_pb2
from pyensign.enum import (
    TopicState,
    DeduplicationStrategy,
    ShardingStrategy,
    OffsetPosition,
)


@pytest.mark.parametrize(
    "name,enum",
    [
        ("TopicState", TopicState),
        ("Deduplication.Strategy", DeduplicationStrategy),
        ("Deduplication.OffsetPosition", OffsetPosition),
        ("ShardingStrategy", ShardingStrategy),
    ],
)
def test_enumerations(name, enum):
    """
    Ensure all enumerations in protobufs are defined in Python code and vice versa.
    """
    pb_states = topic_pb2.DESCRIPTOR.enum_types_by_name[name].values_by_name

    # ensure there are the same number of enumerations
    assert len(pb_states) == len(enum), "mismatch in defined enum and protobufs"

    # check that each enum in the python code is in the protobufs
    for item in enum:
        assert item.name in pb_states
        assert item.value == pb_states[item.name].number

    # check that each enum in the protobufs is in the python code
    for state, value in pb_states.items():
        assert state in enum.__members__
        assert enum[state].value == value.number


@pytest.mark.parametrize(
    "enum,input,expected",
    [
        (TopicState, "undefined", TopicState.UNDEFINED),
        (TopicState, "ready", TopicState.READY),
        (TopicState, "ReadOnly", TopicState.READONLY),
        (TopicState, "DELETING", TopicState.DELETING),
        (TopicState, "  pending\t", TopicState.PENDING),
        (TopicState, "Allocating", TopicState.ALLOCATING),
        (TopicState, "  repairing ", TopicState.REPAIRING),
        (DeduplicationStrategy, "unknown", DeduplicationStrategy.UNKNOWN),
        (DeduplicationStrategy, "None", DeduplicationStrategy.NONE),
        (DeduplicationStrategy, "STRICT", DeduplicationStrategy.STRICT),
        (DeduplicationStrategy, "datagram", DeduplicationStrategy.DATAGRAM),
        (DeduplicationStrategy, "key_grouped", DeduplicationStrategy.KEY_GROUPED),
        (DeduplicationStrategy, "  unique_KEY", DeduplicationStrategy.UNIQUE_KEY),
        (DeduplicationStrategy, "UNIQUE_field  ", DeduplicationStrategy.UNIQUE_FIELD),
        (OffsetPosition, "earliest", OffsetPosition.OFFSET_EARLIEST),
        (OffsetPosition, "OFFSET_earliest", OffsetPosition.OFFSET_EARLIEST),
        (OffsetPosition, "LATEST", OffsetPosition.OFFSET_LATEST),
        (OffsetPosition, "offset_latest", OffsetPosition.OFFSET_LATEST),
        (ShardingStrategy, "unknown", ShardingStrategy.UNKNOWN),
        (ShardingStrategy, "no_SHARDING", ShardingStrategy.NO_SHARDING),
        (
            ShardingStrategy,
            "Consistent_KeY_hash ",
            ShardingStrategy.CONSISTENT_KEY_HASH,
        ),
        (ShardingStrategy, "Random", ShardingStrategy.RANDOM),
        (ShardingStrategy, "publisher_ordering", ShardingStrategy.PUBLISHER_ORDERING),
    ],
)
def test_enumeration_parse(enum, input, expected):
    """
    Ensure parsing enumerations works as expected
    """
    assert enum.parse(input) == expected


@pytest.mark.parametrize(
    "enum,input,expected",
    [
        (TopicState, 0, TopicState.UNDEFINED),
        (TopicState, topic_pb2.TopicState.UNDEFINED, TopicState.UNDEFINED),
        (TopicState, 1, TopicState.READY),
        (TopicState, topic_pb2.TopicState.READY, TopicState.READY),
        (TopicState, 2, TopicState.READONLY),
        (TopicState, topic_pb2.TopicState.READONLY, TopicState.READONLY),
        (TopicState, 3, TopicState.DELETING),
        (TopicState, topic_pb2.TopicState.DELETING, TopicState.DELETING),
        (TopicState, 4, TopicState.PENDING),
        (TopicState, topic_pb2.TopicState.PENDING, TopicState.PENDING),
        (TopicState, 5, TopicState.ALLOCATING),
        (TopicState, topic_pb2.TopicState.ALLOCATING, TopicState.ALLOCATING),
        (TopicState, 6, TopicState.REPAIRING),
        (TopicState, topic_pb2.TopicState.REPAIRING, TopicState.REPAIRING),
        (DeduplicationStrategy, 0, DeduplicationStrategy.UNKNOWN),
        (
            DeduplicationStrategy,
            topic_pb2.Deduplication.Strategy.UNKNOWN,
            DeduplicationStrategy.UNKNOWN,
        ),
        (DeduplicationStrategy, 1, DeduplicationStrategy.NONE),
        (
            DeduplicationStrategy,
            topic_pb2.Deduplication.Strategy.NONE,
            DeduplicationStrategy.NONE,
        ),
        (DeduplicationStrategy, 2, DeduplicationStrategy.STRICT),
        (
            DeduplicationStrategy,
            topic_pb2.Deduplication.Strategy.STRICT,
            DeduplicationStrategy.STRICT,
        ),
        (DeduplicationStrategy, 3, DeduplicationStrategy.DATAGRAM),
        (
            DeduplicationStrategy,
            topic_pb2.Deduplication.Strategy.DATAGRAM,
            DeduplicationStrategy.DATAGRAM,
        ),
        (DeduplicationStrategy, 4, DeduplicationStrategy.KEY_GROUPED),
        (
            DeduplicationStrategy,
            topic_pb2.Deduplication.Strategy.KEY_GROUPED,
            DeduplicationStrategy.KEY_GROUPED,
        ),
        (DeduplicationStrategy, 5, DeduplicationStrategy.UNIQUE_KEY),
        (
            DeduplicationStrategy,
            topic_pb2.Deduplication.Strategy.UNIQUE_KEY,
            DeduplicationStrategy.UNIQUE_KEY,
        ),
        (DeduplicationStrategy, 6, DeduplicationStrategy.UNIQUE_FIELD),
        (
            DeduplicationStrategy,
            topic_pb2.Deduplication.Strategy.UNIQUE_FIELD,
            DeduplicationStrategy.UNIQUE_FIELD,
        ),
        (OffsetPosition, 0, OffsetPosition.OFFSET_UNKNOWN),
        (
            OffsetPosition,
            topic_pb2.Deduplication.OffsetPosition.OFFSET_UNKNOWN,
            OffsetPosition.OFFSET_UNKNOWN,
        ),
        (OffsetPosition, 1, OffsetPosition.OFFSET_EARLIEST),
        (
            OffsetPosition,
            topic_pb2.Deduplication.OffsetPosition.OFFSET_EARLIEST,
            OffsetPosition.OFFSET_EARLIEST,
        ),
        (OffsetPosition, 2, OffsetPosition.OFFSET_LATEST),
        (
            OffsetPosition,
            topic_pb2.Deduplication.OffsetPosition.OFFSET_LATEST,
            OffsetPosition.OFFSET_LATEST,
        ),
        (ShardingStrategy, 0, ShardingStrategy.UNKNOWN),
        (
            ShardingStrategy,
            topic_pb2.ShardingStrategy.UNKNOWN,
            ShardingStrategy.UNKNOWN,
        ),
        (ShardingStrategy, 1, ShardingStrategy.NO_SHARDING),
        (
            ShardingStrategy,
            topic_pb2.ShardingStrategy.NO_SHARDING,
            ShardingStrategy.NO_SHARDING,
        ),
        (ShardingStrategy, 2, ShardingStrategy.CONSISTENT_KEY_HASH),
        (
            ShardingStrategy,
            topic_pb2.ShardingStrategy.CONSISTENT_KEY_HASH,
            ShardingStrategy.CONSISTENT_KEY_HASH,
        ),
        (ShardingStrategy, 3, ShardingStrategy.RANDOM),
        (ShardingStrategy, topic_pb2.ShardingStrategy.RANDOM, ShardingStrategy.RANDOM),
        (ShardingStrategy, 4, ShardingStrategy.PUBLISHER_ORDERING),
        (
            ShardingStrategy,
            topic_pb2.ShardingStrategy.PUBLISHER_ORDERING,
            ShardingStrategy.PUBLISHER_ORDERING,
        ),
    ],
)
def test_enumeration_covert(enum, input, expected):
    """
    Ensure converting enumerations works as expected
    """
    assert enum.convert(input) == expected
