import pytest

from pyensign.api.v1beta1.query import format_query
from pyensign.api.v1beta1.query_pb2 import Parameter, Query


@pytest.mark.parametrize(
    "params, expected_params",
    [
        ({}, []),
        ({"foo": 1}, [Parameter(name="foo", i=1)]),
        ({"foo": 2.3}, [Parameter(name="foo", d=2.3)]),
        ({"foo": True}, [Parameter(name="foo", b=True)]),
        ({"foo": False}, [Parameter(name="foo", b=False)]),
        ({"foo": b"data"}, [Parameter(name="foo", y=b"data")]),
        ({"foo": "bar"}, [Parameter(name="foo", s="bar")]),
        (
            {"foo": "bar", "bar": 2},
            [Parameter(name="foo", s="bar"), Parameter(name="bar", i=2)],
        ),
    ],
)
def test_format_query(params, expected_params):
    """
    Test parsing a query from valid parameters.
    """
    query = "SELECT * FROM foo;"
    expected = Query(query=query, params=expected_params)
    result = format_query(query, params)
    assert expected.query == result.query
    assert expected.params == result.params


@pytest.mark.parametrize(
    "query, params, exception",
    [
        (None, None, TypeError),
        (1, None, TypeError),
        (" ", None, ValueError),
        ("SELECT * from foo;", {"foo": None}, TypeError),
        ("SELECT * from foo;", {"foo": [1, 2, 3]}, TypeError),
        ("SELECT * from foo;", {"foo": {"bar": 1}}, TypeError),
        ("SELECT * from foo;", 42, TypeError),
    ],
)
def test_format_bad_query(query, params, exception):
    """
    Test that the correct error is returned when a bad query is provided.
    """

    with pytest.raises(exception):
        format_query(query, params)
