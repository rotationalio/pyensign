import os
import json
import time
import pickle
import pytest
import asyncio
from datetime import datetime

from ulid import ULID
from unittest import mock
from asyncmock import patch
from google.protobuf.timestamp_pb2 import Timestamp

from pyensign.events import Event
from pyensign.connection import Cursor
from pyensign.topics import Topic
from pyensign.api.v1beta1 import ensign_pb2, topic_pb2, query_pb2, event_pb2
from pyensign.ensign import Ensign, authenticate, publisher, subscriber
from pyensign.enum import TopicState, DeduplicationStrategy, OffsetPosition
from pyensign.exceptions import (
    EnsignTopicCreateError,
    EnsignTopicDestroyError,
    EnsignTopicNotFoundError,
    UnknownTopicError,
    EnsignInvalidArgument,
    InvalidQueryError,
    QueryNoRows,
)
from pyensign.mimetype.v1beta1.mimetype_pb2 import MIME


@pytest.fixture
def live(request):
    return request.config.getoption("--live", default=False)


@pytest.fixture
def creds(request):
    return request.config.getoption("--creds", default="")


@pytest.fixture
def authserver():
    return os.environ.get("ENSIGN_AUTH_SERVER")


@pytest.fixture
def ensignserver():
    return os.environ.get("ENSIGN_SERVER")


@pytest.fixture
def ensign_args():
    return {
        "client_id": "id",
        "client_secret": "secret",
        "endpoint": "localhost:1234",
    }


@pytest.fixture
def ensign(ensign_args):
    return Ensign(**ensign_args)


@pytest.fixture()
def ensign_no_cache():
    return Ensign(
        client_id="id",
        client_secret="secret",
        endpoint="localhost:1234",
        disable_topic_cache=True,
    )


def async_iter(items):
    async def next():
        for item in items:
            yield item

    return next()


class TestEnsign:
    """
    Tests for the Ensign client.
    """

    def test_creds(self):
        """
        Test creating an Ensign client from credentials.
        """
        client_id = "id"
        client_secret = "secret"
        Ensign(client_id=client_id, client_secret=client_secret)

        with mock.patch.dict(
            os.environ,
            {"ENSIGN_CLIENT_ID": client_id, "ENSIGN_CLIENT_SECRET": client_secret},
        ):
            Ensign()

        with mock.patch.dict(os.environ, {"ENSIGN_CLIENT_ID": client_id}):
            Ensign(client_secret=client_secret)

        with mock.patch.dict(os.environ, {"ENSIGN_CLIENT_SECRET": client_secret}):
            Ensign(client_id=client_id)

    def test_creds_json(self):
        """
        Test reading credentials from JSON file.
        """
        # set the path for json file
        cred_path = "tests/fixtures/cred.json"
        Ensign(cred_path=cred_path)

    @pytest.mark.parametrize(
        "client_id, client_secret, cred_path, exception",
        [
            (None, None, "", ValueError),
            ("", "", "", ValueError),
            ("id", "", "", ValueError),
            ("", "secret", "", ValueError),
            (1, 2, "", TypeError),
            ("", "", "tests/fixtures/cred_missing.json", ValueError),
            ("", "", "tests/fixtures/cred.txt", ValueError),
            ("", "", "tests/fixtures/cred_no_file.json", ValueError),
        ],
    )
    def test_bad_creds(self, client_id, client_secret, cred_path, exception):
        with pytest.raises(exception), mock.patch.dict(os.environ, {}, clear=True):
            Ensign(
                client_id=client_id, client_secret=client_secret, cred_path=cred_path
            )

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.publish")
    @pytest.mark.parametrize(
        "events",
        [
            (Event(b'{"foo": "bar"}', "application/json")),
            ([Event(b'{"foo": "bar"}', "application/json")]),
            (Event(b'{"foo": "bar"}', "application/json"), Event(b"foo", "text/plain")),
            ((Event(b'{"foo": "bar"}', "application/json"),)),
            (*(Event(b'{"foo": "bar"}'), Event(b"foo", "text/plain")),),
        ],
    )
    async def test_publish_name(self, mock_publish, events, ensign):
        topic = Topic(name="otters", id=ULID())
        ensign.topics.add(topic.name, topic.id)

        # Publish the events
        await ensign.publish(topic.name, events)

        # Ensure that the publish call was made with the correct topic ID
        args, _ = mock_publish.call_args
        assert isinstance(args[0], Topic)
        assert args[0].id == topic.id
        assert args[0].name == ""

        # Ensure that the protobuf events are passed to the publish call
        async for event in args[1]:
            assert isinstance(event, Event)

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.publish")
    async def test_publish_id(self, mock_publish, ensign):
        events = [
            Event(b'{"foo": "bar"}', "application/json"),
            Event(b"foo", "text/plain"),
        ]
        id = ULID()

        # Publish the events
        await ensign.publish(str(id), events)

        # Ensure that the publish call was made with the correct topic ID
        args, _ = mock_publish.call_args
        assert isinstance(args[0], Topic)
        assert args[0].id == id
        assert args[0].name == ""

        # Ensure that the protobuf events are passed to the publish call
        async for event in args[1]:
            assert isinstance(event, Event)

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.publish")
    async def test_publish_new_topic(self, mock_publish, ensign):
        # Test publishing to a topic that the client has not seen before, should not
        # error because the server might know about it.
        events = [
            Event(b'{"foo": "bar"}', "application/json"),
            Event(b"foo", "text/plain"),
        ]
        name = "otters"

        # Publish the events
        await ensign.publish(name, events)

        # Ensure that the publish call was made with the correct topic name
        args, _ = mock_publish.call_args
        assert isinstance(args[0], Topic)
        assert args[0].name == name
        assert args[0].id is None

        # Ensure that the protobuf events are passed to the publish call
        async for event in args[1]:
            assert isinstance(event, Event)

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.topic_exists")
    @patch("pyensign.connection.Client.create_topic")
    @patch("pyensign.connection.Client.publish")
    async def test_publish_ensure(self, mock_publish, mock_create, mock_exists, ensign):
        name = "otters"
        id = ULID()

        # Test the case where the topic has to be created
        mock_create.return_value = topic_pb2.Topic(id=id.bytes, name=name)
        mock_exists.return_value = (None, False)

        # Publish two events, one of them errors
        events = [
            Event(b'{"foo": "bar"}', MIME.APPLICATION_JSON),
            Event(b'{"bar": "bz"}', MIME.APPLICATION_JSON),
        ]
        await ensign.publish(name, events, ensure_exists=True)

        # Ensure that the publish call was made with the correct topic ID
        args, _ = mock_publish.call_args
        assert isinstance(args[0], Topic)
        assert args[0].id == id
        assert args[0].name == name

        # Ensure that the protobuf events are passed to the publish call
        async for event in args[1]:
            assert isinstance(event, Event)

        # Test the case where the topic already exists
        mock_exists.return_value = (None, True)
        mock_create.return_value = None
        await ensign.publish(name, events, ensure_exists=True)

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.publish")
    @pytest.mark.parametrize(
        "topic, exists, events, exception",
        [
            ("", True, Event(b"foo", "text/plain"), ValueError),
            (42, True, Event(b"foo", "text/plain"), TypeError),
            (None, True, Event(b"foo", "text/plain"), TypeError),
            ("otters", True, [], ValueError),
            ("lighthouses", False, [Event(b"foo", "text/plain")], UnknownTopicError),
        ],
    )
    async def test_publish_error(
        self, mock_publish, topic, exists, events, exception, ensign
    ):
        ensign.topics.add("otters", ULID())
        if not exists:
            mock_publish.side_effect = UnknownTopicError(topic)

        with pytest.raises(exception):
            await ensign.publish(topic, events)

    @pytest.mark.asyncio
    async def test_publish_no_events(self, ensign):
        with pytest.raises(ValueError):
            await ensign.publish("otters")

    @patch("pyensign.connection.Client.publish")
    def test_publish_sync(self, mock_publish, ensign):
        """
        Test executing publish from synchronous code as a coroutine.
        """

        async def publish():
            await ensign.publish("otters", [Event(b"foo", "text/plain")])

        asyncio.run(publish())

        # Ensure that the publish call was made with the correct topic ID
        args, _ = mock_publish.call_args
        assert isinstance(args[0], Topic)
        assert args[0].name == "otters"
        assert args[0].id is None

    @pytest.mark.asyncio
    async def test_auth_decorator(self):
        """
        Test using the auth decorator to mark an async function.
        """

        @authenticate()
        async def marked_fn():
            return True

        with mock.patch.dict(
            os.environ,
            {"ENSIGN_CLIENT_ID": "client_id", "ENSIGN_CLIENT_SECRET": "client_secret"},
        ):
            await marked_fn()

    @pytest.mark.asyncio
    async def test_auth_decorator_args(self):
        @authenticate(client_id="client_id", client_secret="client_secret")
        async def marked_fn():
            return True

        await marked_fn()

    @pytest.mark.asyncio
    async def test_auth_decorator_generator(self):
        """
        Test using the auth decorator to mark an async generator function.
        """

        @authenticate()
        async def marked_fn():
            yield True

        with mock.patch.dict(
            os.environ,
            {"ENSIGN_CLIENT_ID": "client_id", "ENSIGN_CLIENT_SECRET": "client_secret"},
        ):
            async for _ in marked_fn():
                pass

    @pytest.mark.asyncio
    async def test_auth_decorator_wrong_type(self):
        """
        Test cannot use the auth decorator on a non-function.
        """

        with pytest.raises(TypeError):

            @authenticate()
            class foo:
                pass

            foo()

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.publish")
    @pytest.mark.parametrize(
        "obj, mimetype, expected_data, expected_mimetype",
        [
            (
                {"name": "Enson"},
                None,
                b'{"name": "Enson"}',
                MIME.APPLICATION_JSON,
            ),
            (
                {"name": "Enson"},
                "application/json",
                b'{"name": "Enson"}',
                MIME.APPLICATION_JSON,
            ),
            (
                "Enson",
                None,
                b"Enson",
                MIME.TEXT_PLAIN,
            ),
            (
                b"Enson",
                "text/plain",
                b"Enson",
                MIME.TEXT_PLAIN,
            ),
            (
                {"name": "Enson"},
                "application/python-pickle",
                pickle.dumps({"name": "Enson"}),
                MIME.APPLICATION_PYTHON_PICKLE,
            ),
        ],
    )
    async def test_publish_decorator(
        self,
        mock_publish,
        ensign_args,
        obj,
        mimetype,
        expected_data,
        expected_mimetype,
    ):
        @authenticate(**ensign_args)
        @publisher("otters", mimetype=mimetype)
        async def marked_fn(obj):
            return obj

        # Invoke the marked async function
        val = await marked_fn(obj)
        assert val == obj

        # Should have called publish with the correct topic and event
        args, _ = mock_publish.call_args
        assert isinstance(args[0], Topic)
        assert args[0].name == "otters"

        async for event in args[1]:
            assert isinstance(event, Event)
            assert event.data == expected_data
            assert event.mimetype == expected_mimetype

    @patch("pyensign.connection.Client.publish")
    def test_publish_decorator_sync(self, mock_publish, ensign_args):
        @authenticate(**ensign_args)
        @publisher("otters")
        def marked_fn(obj):
            return obj

        # Invoke the marked async function
        val = marked_fn("Enson")
        assert val == "Enson"

        # Should have called publish with the correct topic and event
        args, _ = mock_publish.call_args
        assert isinstance(args[0], Topic)
        assert args[0].name == "otters"

        async def check_args():
            async for event in args[1]:
                assert isinstance(event, Event)
                assert event.data == b"Enson"
                assert event.mimetype == MIME.TEXT_PLAIN

        asyncio.run(check_args())

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.publish")
    async def test_publish_decorator_encoder(self, mock_publish, ensign_args):
        """
        Test the publish decorator with a custom encoder.
        """

        class CustomJSONEncoder:
            def encode(self, obj):
                return json.dumps(obj, indent=2).encode("utf-8")

        @authenticate(**ensign_args)
        @publisher("otters", mimetype="application/json", encoder=CustomJSONEncoder())
        async def marked_fn(obj):
            return obj

        # Invoke the marked async function
        val = await marked_fn({"name": "Enson"})
        assert val == {"name": "Enson"}

        # Should have called publish with the correct topic and event
        args, _ = mock_publish.call_args
        assert isinstance(args[0], Topic)
        assert args[0].name == "otters"

        async for event in args[1]:
            assert isinstance(event, Event)
            assert event.data == b'{\n  "name": "Enson"\n}'
            assert event.mimetype == MIME.APPLICATION_JSON

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.publish")
    async def test_publish_decorator_generator(self, mock_publish, ensign_args):
        """
        Test marking a generator function with the publish decorator.
        """

        @authenticate(**ensign_args)
        @publisher("otters", mimetype="application/json")
        async def marked_fn(objects):
            for obj in objects:
                yield obj

        # Invoke the marked async function
        objects = [{"name": "Enson"}, {"name": "Otto"}]
        i = 0
        async for val in marked_fn(objects):
            assert val == objects[i]
            args, _ = mock_publish.call_args
            assert isinstance(args[0], Topic)
            assert args[0].name == "otters"
            async for event in args[1]:
                assert isinstance(event, Event)
                assert event.data == json.dumps(objects[i]).encode("utf-8")
                assert event.mimetype == MIME.APPLICATION_JSON
            i += 1

    @patch("pyensign.connection.Client.publish")
    def test_publish_decorator_generator_sync(self, mock_publish, ensign_args):
        @authenticate(**ensign_args)
        @publisher("otters", mimetype="application/json")
        def marked_fn(objects):
            for obj in objects:
                yield obj

        # Invoke the marked async function
        objects = [{"name": "Enson"}, {"name": "Otto"}]
        i = 0
        for val in marked_fn(objects):
            assert val == objects[i]
            args, _ = mock_publish.call_args
            assert isinstance(args[0], Topic)
            assert args[0].name == "otters"

            async def check_args():
                async for event in args[1]:
                    assert isinstance(event, Event)
                    assert event.data == json.dumps(objects[i]).encode("utf-8")
                    assert event.mimetype == MIME.APPLICATION_JSON

            asyncio.run(check_args())
            i += 1

    @pytest.mark.asyncio
    async def test_publish_decorator_no_auth(self):
        """
        Should raise an exception if the publish decorator is used without authenticate
        being called.
        """

        @publisher("otters", mimetype="application/json")
        async def marked_fn(obj):
            return obj

        # Invoke the marked async function
        with pytest.raises(RuntimeError):
            await marked_fn(None)

    @pytest.mark.asyncio
    async def test_publish_decorator_error(self, ensign_args):
        """
        Publish errors should be raised from the decorator.
        """

        @authenticate(**ensign_args)
        @publisher("")
        async def marked_fn(obj):
            return obj

        # No topic provided should be a ValueError
        with pytest.raises(ValueError):
            await marked_fn("obj")

    @pytest.mark.asyncio
    async def test_publish_decorator_wrong_type(self):
        """
        Test cannot use the publish decorator on a non-async function.
        """

        with pytest.raises(TypeError):

            @authenticate()
            @publisher("otters")
            class foo:
                pass

            foo()

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.status")
    async def test_status(self, mock_status, ensign):
        mock_status.return_value = ("AVAILABLE", "1.0.0", "10 minutes", "", "")
        status = await ensign.status()
        assert status.status == "AVAILABLE"
        assert status.version == "1.0.0"
        assert status.uptime == "10 minutes"

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.subscribe")
    @pytest.mark.parametrize(
        "topics",
        [
            ("otters"),
            (str(ULID())),
            (["otters"]),
            (["otters", "lighthouses"]),
            (("otters", "lighthouses")),
            (["otters", str(ULID())]),
            (*("otters", "lighthouses"),),
        ],
    )
    async def test_subscribe(self, mock_subscribe, topics, ensign):
        ensign.topics.add("otters", ULID())
        ensign.topics.add("lighthouses", ULID())
        async for event in ensign.subscribe(topics):
            assert isinstance(event, Event)

        # Ensure that ULID strings are passed to the subscribe call
        args, _ = mock_subscribe.call_args
        for id in args[0]:
            assert isinstance(ULID.from_str(id), ULID)

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.list_topics")
    @patch("pyensign.connection.Client.topic_exists")
    @pytest.mark.parametrize(
        "topics, query, params, exception",
        [
            ([], "", None, ValueError),
            (111, "", None, TypeError),
            (("otters", 111), "", None, TypeError),
            (["otters"], 42, {"foo": "bar"}, TypeError),
            (["otters"], " SELECT * from foo; ", 42, TypeError),
        ],
    )
    async def test_subscribe_error(
        self, mock_exists, mock_list, topics, query, params, exception, ensign
    ):
        ensign.topics.add("otters", ULID())
        mock_exists.return_value = (None, False)
        mock_list.return_value = ([topic_pb2.Topic(name="otters", id=ULID().bytes)], "")
        with pytest.raises(exception):
            async for _ in ensign.subscribe(topics, query=query, params=params):
                pass

    @pytest.mark.asyncio
    async def test_subscribe_no_topics(self, ensign):
        with pytest.raises(ValueError):
            async for _ in ensign.subscribe():
                pass

    @patch("pyensign.connection.Client.subscribe")
    def test_subscribe_sync(self, mock_subscribe, ensign):
        """
        Test executing subscribe from synchronous code as a coroutine.
        """
        ensign.topics.add("otters", ULID())

        async def subscribe():
            async for event in ensign.subscribe("otters"):
                assert isinstance(event, Event)

        asyncio.run(subscribe())

        # Ensure that ULID strings are passed to the subscribe call
        args, _ = mock_subscribe.call_args
        for id in args[0]:
            assert isinstance(ULID.from_str(id), ULID)

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.subscribe")
    async def test_subscriber_decorator(self, mock_subscribe, ensign_args):
        all_events = []
        mock_subscribe.return_value = async_iter([Event(b"foo", "text/plain")])

        @authenticate(**ensign_args)
        @subscriber("otters")
        async def marked_fn(events, arg, keyword_arg="default"):
            assert arg == "arg"
            assert keyword_arg == "override"
            async for event in events:
                all_events.append(event)

        # Invoke the subscriber
        await marked_fn("arg", keyword_arg="override")

        # Should have called subscribe with the topic
        args, _ = mock_subscribe.call_args
        assert args[0] == ["otters"]

        # Events should be passed to the subscriber
        assert len(all_events) == 1
        assert isinstance(all_events[0], Event)

    @patch("pyensign.connection.Client.subscribe")
    def test_subscriber_decorator_sync(self, mock_subscribe, ensign_args):
        all_events = []
        mock_subscribe.return_value = async_iter(
            [Event(b"foo", "text/plain"), Event(b"bar", "text/plain")]
        )

        @authenticate(**ensign_args)
        @subscriber("otters")
        def marked_fn(event, arg, keyword_arg="default"):
            assert arg == "arg"
            assert keyword_arg == "override"
            all_events.append(event)

        # Invoke the subscriber
        marked_fn("arg", keyword_arg="override")

        # Should have called subscribe with the topic
        args, _ = mock_subscribe.call_args
        assert args[0] == ["otters"]

        # Events should be passed to the subscriber
        assert len(all_events) == 2
        assert isinstance(all_events[0], Event)
        assert isinstance(all_events[1], Event)

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.subscribe")
    async def test_subscriber_generator(self, mock_subscribe, ensign_args):
        events = []
        mock_subscribe.return_value = async_iter(
            [Event(b"foo", "text/plain"), Event(b"bar", "text/plain")]
        )

        @authenticate(**ensign_args)
        @subscriber("otters", "lighthouses")
        async def marked_fn(events):
            async for event in events:
                yield event

        # Read events from the subscriber
        async for event in marked_fn():
            events.append(event)

        # Should have called subscribe with the topic
        args, _ = mock_subscribe.call_args
        assert args[0] == ["otters", "lighthouses"]

        # Events should be passed to the subscriber
        assert len(events) == 2
        assert events[0].data == b"foo"
        assert events[1].data == b"bar"

    @patch("pyensign.connection.Client.subscribe")
    def test_subscriber_generator_sync(self, mock_subscribe, ensign_args):
        events = []
        mock_subscribe.return_value = async_iter(
            [Event(b"foo", "text/plain"), Event(b"bar", "text/plain")]
        )

        # Synchronous generators not currently supported on subscribe
        with pytest.raises(TypeError):

            @authenticate(**ensign_args)
            @subscriber("otters", "lighthouses")
            def marked_fn(event):
                yield event

            for event in marked_fn():
                events.append(event)

    @pytest.mark.asyncio
    async def test_subscriber_decorator_no_auth(self):
        """
        Should raise an exception if the subscriber decorator is used without
        authenticate being called.
        """

        @subscriber("otters")
        async def marked_fn(events):
            async for _ in events:
                pass

        # Invoke the marked async function
        with pytest.raises(RuntimeError):
            await marked_fn()

    @pytest.mark.asyncio
    async def test_subscriber_decorator_error(self, ensign_args):
        """
        Subscribe errors should be raised from the decorator.
        """

        @authenticate(**ensign_args)
        @subscriber()
        async def marked_fn(events):
            async for _ in events:
                pass

        # No topic provided should be a ValueError
        with pytest.raises(ValueError):
            await marked_fn()

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.publish")
    @patch("pyensign.connection.Client.subscribe")
    async def test_sub_pub_decorators(self, mock_subscribe, mock_publish, ensign_args):
        """
        Test a decorated method that's both a subscriber and publisher.
        """

        events = [Event(b"foo", "text/plain"), Event(b"bar", "text/plain")]
        mock_subscribe.return_value = async_iter(events)

        @authenticate(**ensign_args)
        @subscriber("otters")
        @publisher("lighthouses")
        async def marked_fn(events):
            async for event in events:
                yield event.data

        # Invoke the marked async function
        results = [data async for data in marked_fn()]
        assert results == [b"foo", b"bar"]

        # Should have called subscribe with the topic
        args, _ = mock_subscribe.call_args
        assert args[0] == ["otters"]

        # Should have called publish with the topic and events
        args, _ = mock_publish.call_args
        assert isinstance(args[0], Topic)
        assert args[0].name == "lighthouses"
        async for event in args[1]:
            assert isinstance(event, Event)
            assert event.data in [b"foo", b"bar"]

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.en_sql")
    async def test_query(self, mock_en_sql, ensign):
        mock_en_sql.return_value = Cursor()
        cursor = await ensign.query("SELECT * FROM otters")
        expected_query = query_pb2.Query(query="SELECT * FROM otters", params=[])
        mock_en_sql.assert_called_once_with(expected_query)
        assert isinstance(cursor, Cursor)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "raises, exception",
        [
            (EnsignInvalidArgument(None, None, "bad syntax"), InvalidQueryError),
            (QueryNoRows, QueryNoRows),
        ],
    )
    @patch("pyensign.connection.Client.en_sql")
    async def test_query_error(self, mock_en_sql, raises, exception, ensign):
        mock_en_sql.side_effect = raises
        with pytest.raises(exception):
            await ensign.query("SELECT * FROM otters")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "params, exception",
        [
            ({}, None),
            ({"foo": 1}, None),
            ({"foo": 2.3}, None),
            ({"foo": True}, None),
            ({"foo": b"data"}, None),
            ({"foo": "bar"}, None),
            ({"foo": "bar", "bar": 2}, None),
            ({"foo": None}, TypeError),
            ({"foo": [1, 2, 3]}, TypeError),
            ({"foo": {"bar": 1}}, TypeError),
        ],
    )
    @patch("pyensign.connection.Client.en_sql")
    async def test_query_params(self, mock_en_sql, params, exception, ensign):
        mock_en_sql.return_value = Cursor()
        if exception:
            with pytest.raises(exception):
                await ensign.query("SELECT * FROM otters", params=params)
        else:
            cursor = await ensign.query("SELECT * FROM otters", params=params)
            assert isinstance(cursor, Cursor)

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.list_topics")
    async def test_get_topics(self, mock_list, ensign):
        topic_id = ULID()
        project_id = ULID()
        topics = [
            topic_pb2.Topic(
                id=topic_id.bytes,
                project_id=project_id.bytes,
                name="otters",
                offset=42,
                status=topic_pb2.TopicState.READY,
                deduplication=topic_pb2.Deduplication(
                    strategy=topic_pb2.Deduplication.Strategy.KEY_GROUPED,
                    offset=topic_pb2.Deduplication.OffsetPosition.OFFSET_EARLIEST,
                    keys=["foo"],
                    overwrite_duplicate=True,
                ),
                created=Timestamp(seconds=int(time.time())),
                modified=Timestamp(seconds=int(time.time())),
            )
        ]
        mock_list.return_value = (topics, "")
        actual = await ensign.get_topics()
        assert len(actual) == 1
        assert actual[0].id == topic_id
        assert actual[0].project_id == project_id
        assert actual[0].name == "otters"
        assert actual[0].offset == 42
        assert actual[0].status == TopicState.READY
        assert actual[0].deduplication.strategy == DeduplicationStrategy.KEY_GROUPED
        assert actual[0].deduplication.offset == OffsetPosition.OFFSET_EARLIEST
        assert actual[0].deduplication.keys == ["foo"]
        assert actual[0].deduplication.overwrite_duplicate
        assert isinstance(actual[0].created, datetime)
        assert isinstance(actual[0].modified, datetime)

        # TODO: test pagination

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.create_topic")
    async def test_create_topic(self, mock_create, ensign):
        id = ULID()
        mock_create.return_value = topic_pb2.Topic(id=id.bytes, name="otters")
        topic_id = await ensign.create_topic("otters")
        assert topic_id == str(id)

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.create_topic")
    async def test_create_topic_error(self, mock_create, ensign):
        mock_create.return_value = None
        with pytest.raises(EnsignTopicCreateError):
            await ensign.create_topic("otters")

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.topic_exists")
    @patch("pyensign.connection.Client.list_topics")
    async def test_ensure_topic_exists(self, mock_list, mock_exists, ensign):
        id = ULID()
        mock_exists.return_value = (None, True)
        mock_list.return_value = ([topic_pb2.Topic(id=id.bytes, name="otters")], "")
        topic_id = await ensign.ensure_topic_exists("otters")
        assert topic_id == str(id)

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.topic_exists")
    @patch("pyensign.connection.Client.create_topic")
    async def test_ensure_topic_not_exists(self, mock_create, mock_exists, ensign):
        id = ULID()
        mock_exists.return_value = (None, False)
        mock_create.return_value = topic_pb2.Topic(id=id.bytes, name="otters")
        topic_id = await ensign.ensure_topic_exists("otters")
        assert topic_id == str(id)

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.destroy_topic")
    async def test_destroy_topic_id(self, mock_destroy, ensign):
        id = ULID()
        mock_destroy.return_value = (
            id.bytes,
            TopicState.DELETING,
        )
        await ensign.destroy_topic(str(id))

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.destroy_topic")
    async def test_destroy_topic_cached(self, mock_destroy, ensign):
        ensign.topics.add("otters", ULID())
        mock_destroy.return_value = (
            ULID().bytes,
            TopicState.DELETING,
        )
        await ensign.destroy_topic("otters")

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.destroy_topic")
    @patch("pyensign.connection.Client.list_topics")
    async def test_destroy_topic_no_cache(self, mock_list, mock_destroy, ensign):
        id = ULID()
        mock_destroy.return_value = (
            id.bytes,
            TopicState.DELETING,
        )
        mock_list.return_value = ([topic_pb2.Topic(id=id.bytes, name="otters")], "")
        await ensign.destroy_topic("otters")

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.list_topics")
    async def test_destroy_topic_not_exists(self, mock_list, ensign):
        mock_list.return_value = ([], "")
        with pytest.raises(EnsignTopicNotFoundError):
            await ensign.destroy_topic("otters")

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.destroy_topic")
    async def test_destroy_topic_error(self, mock_destroy, ensign):
        ensign.topics.add("otters", ULID())
        mock_destroy.return_value = (
            ULID().bytes,
            TopicState.UNDEFINED,
        )
        with pytest.raises(EnsignTopicDestroyError):
            await ensign.destroy_topic("otters")

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.list_topics")
    async def test_topic_id(self, mock_list, ensign):
        id = ULID()
        topics = [topic_pb2.Topic(id=id.bytes, name="otters")]
        mock_list.return_value = (topics, "")
        actual = await ensign.topic_id("otters")
        assert actual == str(id)

        # Test that the cached value is used on subsequent calls
        mock_list.return_value = ([], "")
        actual = await ensign.topic_id("otters")
        assert actual == str(id)

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.list_topics")
    async def test_topic_id_no_cache(self, mock_list, ensign_no_cache):
        id = ULID()
        topics = [topic_pb2.Topic(id=id.bytes, name="otters")]
        mock_list.return_value = (topics, "")
        actual = await ensign_no_cache.topic_id("otters")
        assert actual == str(id)

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.list_topics")
    async def test_topic_id_error(self, mock_list, ensign):
        mock_list.return_value = ([], "")
        with pytest.raises(EnsignTopicNotFoundError):
            await ensign.topic_id("otters")

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.topic_exists")
    async def test_topic_exists(self, mock_exists, ensign):
        mock_exists.return_value = (None, True)
        exists = await ensign.topic_exists("otters")
        assert exists

        # Test that the cache is checked first
        ensign.topics.add("otters", ULID().bytes)
        mock_exists.return_value = (None, False)
        exists = await ensign.topic_exists("otters")
        assert exists

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.info")
    async def test_info(self, mock_info, ensign):
        project_id = ULID()
        expected = ensign_pb2.ProjectInfo(
            project_id=project_id.bytes,
            num_topics=2,
            num_readonly_topics=1,
            events=100,
            duplicates=10,
            data_size_bytes=1024,
            topics=[
                topic_pb2.TopicInfo(
                    topic_id=ULID().bytes,
                    project_id=project_id.bytes,
                    event_offset_id=ULID().bytes,
                    events=60,
                    duplicates=6,
                    data_size_bytes=512,
                    types=[
                        topic_pb2.EventTypeInfo(
                            type=event_pb2.Type(
                                name="message",
                                major_version=1,
                                minor_version=2,
                                patch_version=3,
                            ),
                            mimetype=MIME.TEXT_PLAIN,
                            events=60,
                            duplicates=6,
                            data_size_bytes=512,
                        ),
                    ],
                ),
                topic_pb2.TopicInfo(
                    topic_id=ULID().bytes,
                    project_id=project_id.bytes,
                    event_offset_id=ULID().bytes,
                    events=40,
                    duplicates=4,
                    data_size_bytes=512,
                    types=[
                        topic_pb2.EventTypeInfo(
                            type=event_pb2.Type(
                                name="data",
                                major_version=1,
                                patch_version=2,
                            ),
                            mimetype=MIME.APPLICATION_JSON,
                            events=30,
                            duplicates=3,
                            data_size_bytes=256,
                        ),
                        topic_pb2.EventTypeInfo(
                            type=event_pb2.Type(
                                name="model",
                                major_version=2,
                            ),
                            mimetype=MIME.APPLICATION_PYTHON_PICKLE,
                            events=10,
                            duplicates=1,
                            data_size_bytes=256,
                        ),
                    ],
                ),
            ],
        )
        mock_info.return_value = expected
        info = await ensign.info()
        assert info.id == project_id
        assert info.num_topics > 0
        assert info.num_readonly_topics > 0
        assert info.events > 0
        assert info.duplicates > 0
        assert info.data_size_bytes > 0
        assert len(info.topics) > 0
        for topic in info.topics:
            assert isinstance(topic.id, ULID)
            assert isinstance(topic.project_id, ULID)
            assert isinstance(topic.event_offset_id, bytes)
            assert topic.events > 0
            assert topic.duplicates > 0
            assert topic.data_size_bytes > 0
            assert len(topic.types) > 0
            for type in topic.types:
                assert len(type.type.semver()) > 0
                assert type.mimetype > 0
                assert type.events > 0
                assert type.duplicates > 0
                assert type.data_size_bytes > 0

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.info")
    async def test_info_filter(self, mock_info, ensign):
        project_id = ULID()
        expected = ensign_pb2.ProjectInfo(
            project_id=project_id.bytes,
            num_topics=1,
            num_readonly_topics=1,
            events=100,
            duplicates=10,
            data_size_bytes=1024,
            topics=[
                topic_pb2.TopicInfo(
                    topic_id=ULID().bytes,
                    project_id=project_id.bytes,
                    event_offset_id=ULID().bytes,
                    events=60,
                    duplicates=6,
                    data_size_bytes=512,
                    types=[
                        topic_pb2.EventTypeInfo(
                            type=event_pb2.Type(
                                name="message",
                                major_version=1,
                                minor_version=2,
                                patch_version=3,
                            ),
                            mimetype=MIME.TEXT_PLAIN,
                            events=60,
                            duplicates=6,
                            data_size_bytes=512,
                        ),
                    ],
                ),
            ],
        )
        mock_info.return_value = expected
        topic_ids = [str(ULID())]
        info = await ensign.info(topic_ids=topic_ids)

        # Ensure that ID bytes, not ULIDs, are passed to the client
        id_bytes = [ULID.from_str(id).bytes for id in topic_ids]
        mock_info.assert_called_once_with(id_bytes)

        assert info.id == project_id
        assert info.num_topics > 0
        assert info.num_readonly_topics > 0
        assert info.events > 0
        assert info.duplicates > 0
        assert info.data_size_bytes > 0
        assert len(info.topics) > 0
        for topic in info.topics:
            assert isinstance(topic.id, ULID)
            assert isinstance(topic.project_id, ULID)
            assert isinstance(topic.event_offset_id, bytes)
            assert topic.events > 0
            assert topic.duplicates > 0
            assert topic.data_size_bytes > 0
            assert len(topic.types) > 0
            for type in topic.types:
                assert len(type.type.semver()) > 0
                assert type.mimetype > 0
                assert type.events > 0
                assert type.duplicates > 0
                assert type.data_size_bytes > 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "topic_ids,exception",
        [
            ("otters", TypeError),
            (1234, TypeError),
            (None, TypeError),
            (["otters"], ValueError),
            (["otters", 1234], ValueError),
            ([str(ULID()), "otters"], ValueError),
        ],
    )
    async def test_info_bad_topics(self, topic_ids, exception, ensign):
        with pytest.raises(exception):
            await ensign.info(topic_ids)

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.topic_exists")
    async def test_topic_exists_no_cache(self, mock_exists, ensign_no_cache):
        mock_exists.return_value = (None, True)
        exists = await ensign_no_cache.topic_exists("otters")
        assert exists

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.set_topic_deduplication_policy")
    @pytest.mark.parametrize(
        "args",
        [
            {"strategy": 1},
            {"strategy": "STRICT"},
            {"strategy": "unique_key", "keys": ["foo", "bar"]},
            {"strategy": topic_pb2.Deduplication.DATAGRAM, "offset": 2},
            {"strategy": topic_pb2.Deduplication.STRICT, "offset": "latest"},
            {"strategy": "unique_field", "fields": ["foo", "bar"]},
            {"strategy": "Strict", "offset": topic_pb2.Deduplication.OFFSET_EARLIEST},
        ],
    )
    async def test_set_topic_deduplication_policy(self, mock_set_policy, args, ensign):
        mock_set_policy.return_value = topic_pb2.TopicStatus(id="123", state=1)
        state = await ensign.set_topic_deduplication_policy(123, **args)
        assert state == TopicState.READY

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.set_topic_sharding_strategy")
    @pytest.mark.parametrize(
        "strategy",
        [
            2,
            "CONSISTENT_KEY_HASH",
            topic_pb2.ShardingStrategy.RANDOM,
        ],
    )
    async def test_set_topic_sharding_strategy(self, mock_set_policy, strategy, ensign):
        mock_set_policy.return_value = topic_pb2.TopicStatus(id="123", state=1)
        state = await ensign.set_topic_sharding_strategy(123, strategy)
        assert state == TopicState.READY

    @pytest.mark.asyncio
    async def test_live_multi_publish(self, live, creds, authserver, ensignserver):
        if not live:
            pytest.skip("Skipping live test")
        if not authserver:
            pytest.skip("Skipping live test")
        if not ensignserver:
            pytest.skip("Skipping live test")

        ensign = Ensign(endpoint=ensignserver, auth_url=authserver, cred_path=creds)
        events = [
            Event("event {}".format(i).encode(), mimetype="text/plain")
            for i in range(10)
        ]
        topic = "python-events"

        # Ensure the topic exists
        await ensign.ensure_topic_exists(topic)

        # Publish the event
        await ensign.publish(topic, events)

        # Close the client to flush the events
        await ensign.close()

        # All events should be published
        for event in events:
            assert event.published()

    @pytest.mark.asyncio
    async def test_live_pubsub(self, live, creds, authserver, ensignserver):
        if not live:
            pytest.skip("Skipping live test")
        if not authserver:
            pytest.skip("Skipping live test")
        if not ensignserver:
            pytest.skip("Skipping live test")

        ensign = Ensign(endpoint=ensignserver, auth_url=authserver, cred_path=creds)
        event = Event(
            b"message in a bottle",
            "text/plain",
            schema_name="message",
            schema_version="1.0.0",
        )
        topic = "pyensign-pub-sub"

        # Ensure the topic exists
        await ensign.ensure_topic_exists(topic)

        # Run publish and subscribe as coroutines
        publish_ack = asyncio.Event()

        async def handle_ack(ack):
            assert isinstance(ack, ensign_pb2.Ack)
            publish_ack.set()

        async def pub():
            # Delay the publisher to prevent deadlock
            await ensign.publish(topic, event, on_ack=handle_ack)
            await publish_ack.wait()

        async def sub():
            async for event in ensign.subscribe(topic):
                await event.ack()
                return event

        _, received = await asyncio.gather(pub(), sub())
        assert event.acked()
        assert not event.nacked()
        assert received.acked()
        assert not received.nacked()
        assert str(received.type) == "message v1.0.0"
        assert received.data == b"message in a bottle"
        assert received.mimetype == MIME.TEXT_PLAIN
        assert received.id
