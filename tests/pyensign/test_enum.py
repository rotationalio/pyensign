import pytest

from pyensign.api.v1beta1 import topic_pb2
from pyensign.enum import TopicState, DeduplicationStrategy, ShardingStrategy


@pytest.mark.parametrize(
    "name,enum",
    [
        ("TopicState", TopicState),
        ("Deduplication.Strategy", DeduplicationStrategy),
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