import pytest

from pyensign.events import Event
from pyensign.ml.dataframe import DataFrame


def async_iter(items):
    async def next():
        for item in items:
            yield item

    return next()


def make_event(data, id, mimetype="application/json"):
    event = Event(data, mimetype=mimetype)
    event.id = id
    return event


class TestDataFrame:
    """
    Test constructing and using a DataFrame.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "events, orient, columns, fillna, index, expected",
        [
            (
                [],
                "columns",
                None,
                False,
                "id",
                DataFrame([], columns=[], index=[]),
            ),
            (
                [make_event(b'{"a": 1, "b": 2}', "1")],
                "columns",
                None,
                False,
                "id",
                DataFrame([[1, 2]], columns=["a", "b"], index=["1"]),
            ),
            (
                [
                    make_event(b'{"a": 1, "b": 2}', "1"),
                    make_event(b'{"a": 3}', "2"),
                ],
                "columns",
                None,
                True,
                "id",
                DataFrame([[1, 2], [3, None]], columns=["a", "b"], index=["1", "2"]),
            ),
            (
                [
                    make_event(b'{"a": 1, "b": 2}', "1"),
                    make_event(b'{"a": 3}', "2"),
                ],
                "index",
                None,
                True,
                "id",
                DataFrame([[1, 3], [2, None]], columns=["1", "2"], index=["a", "b"]),
            ),
            (
                [
                    make_event(b'{"a": 1}', "1"),
                    make_event(b'{"a": 3, "b": 2}', "2"),
                ],
                "index",
                ["Event 1", "Event 2"],
                True,
                "id",
                DataFrame(
                    [[1, 3], [None, 2]],
                    columns=["Event 1", "Event 2"],
                    index=["a", "b"],
                ),
            ),
            (
                [
                    make_event(b'{"a": 1, "b": 2}', "1"),
                    make_event(b'{"a": 2, "b": 3}', "2"),
                ],
                "columns",
                None,
                False,
                "id",
                DataFrame([[1, 2], [2, 3]], columns=["a", "b"], index=["1", "2"]),
            ),
            (
                [
                    make_event(b"[1, 2, 3]", "1"),
                    make_event(b"[4, 5, 6]", "2"),
                ],
                "columns",
                None,
                True,
                "id",
                DataFrame([[1, 2, 3], [4, 5, 6]], columns=[0, 1, 2], index=["1", "2"]),
            ),
            (
                [
                    make_event(b"hello", "1", mimetype="text/plain"),
                    make_event(b"world", "2", mimetype="text/plain"),
                ],
                "columns",
                None,
                True,
                "id",
                DataFrame([["hello"], ["world"]], columns=[0], index=["1", "2"]),
            ),
            (
                [
                    make_event(b"hello", "1", mimetype="text/plain"),
                    make_event(b"world", "2", mimetype="text/plain"),
                ],
                "columns",
                None,
                False,
                "range",
                DataFrame([["hello"], ["world"]], columns=[0], index=[0, 1]),
            ),
        ],
    )
    async def test_from_events(self, events, orient, columns, fillna, index, expected):
        """
        Test constructing a DataFrame from events.
        """

        df = await DataFrame.from_events(
            async_iter(events),
            orient=orient,
            columns=columns,
            fillna=fillna,
            index=index,
        )
        print(df)
        print(expected)
        assert df.equals(expected)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "comment, events, orient, columns, fillna, index, exception",
        [
            (
                "bad orient option",
                [],
                "invalid",
                None,
                False,
                "id",
                ValueError,
            ),
            (
                "bad index option",
                [],
                "columns",
                None,
                False,
                "invalid",
                ValueError,
            ),
            (
                "missing event ID",
                [make_event(b"hello", None)],
                "columns",
                None,
                False,
                "id",
                ValueError,
            ),
            (
                "duplicate event ID",
                [make_event(b"hello", "1"), make_event(b"world", "1")],
                "index",
                None,
                False,
                "id",
                ValueError,
            ),
            (
                "unparseable event data",
                [make_event(b"hello", "1")],
                "columns",
                None,
                False,
                "id",
                ValueError,
            ),
            (
                "incompatible event data types",
                [
                    make_event(b'{"a": 1, "b": 2}', "1"),
                    make_event(b"[1, 2, 3]", "2"),
                ],
                "columns",
                None,
                False,
                "id",
                ValueError,
            ),
            (
                "mismatched array lengths",
                [
                    make_event(b"[1, 2, 3]", "1"),
                    make_event(b"[4, 5]", "2"),
                ],
                "columns",
                None,
                False,
                "id",
                ValueError,
            ),
            (
                "missing key for fillna=False",
                [
                    make_event(b'{"a": 1, "b": 2}', "1"),
                    make_event(b'{"a": 3}', "2"),
                ],
                "columns",
                None,
                False,
                "id",
                ValueError,
            ),
            (
                "extra key for fillna=False",
                [
                    make_event(b'{"a": 1}', "1"),
                    make_event(b'{"a": 3, "b": 4}', "2"),
                ],
                "columns",
                None,
                False,
                "id",
                ValueError,
            ),
        ],
    )
    async def test_from_events_error(
        self, comment, events, orient, columns, fillna, index, exception
    ):
        """
        Test user errors and ValueErrors when constructing a DataFrame from events.
        """

        with pytest.raises(exception):
            await DataFrame.from_events(
                async_iter(events),
                orient=orient,
                columns=columns,
                fillna=fillna,
                index=index,
            )
