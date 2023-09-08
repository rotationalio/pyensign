import os
import json
import pickle
import pytest
import asyncio
from ulid import ULID
from unittest import mock
from asyncmock import patch

from pyensign.ensign import Ensign, authenticate, publish
from pyensign.events import Event
from pyensign.connection import Cursor
from pyensign.utils.topics import Topic
from pyensign.api.v1beta1 import ensign_pb2
from pyensign.api.v1beta1 import topic_pb2
from pyensign.exceptions import (
    EnsignTopicCreateError,
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
        Test cannot use the auth decorator on a non-async function.
        """

        with pytest.raises(TypeError):

            @authenticate()
            def marked_fn():
                return True

            marked_fn()

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
        @publish("otters", mimetype=mimetype)
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
        @publish("otters", mimetype="application/json", encoder=CustomJSONEncoder())
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
        @publish("otters", mimetype="application/json")
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

    @pytest.mark.asyncio
    async def test_publish_decorator_no_auth(self):
        """
        Should raise an exception if the publish decorator is used without authenticate
        being called.
        """

        @publish("otters", mimetype="application/json")
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
        @publish("")
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
            @publish("otters")
            def marked_fn():
                return True

            marked_fn()

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.status")
    async def test_status(self, mock_status, ensign):
        mock_status.return_value = ("AVAILABLE", "1.0.0", "10 minutes", "", "")
        status = await ensign.status()
        assert status == "status: AVAILABLE\nversion: 1.0.0\nuptime: 10 minutes"

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
        "topics, exception",
        [
            ([], ValueError),
            (111, TypeError),
            (("otters", 111), TypeError),
        ],
    )
    async def test_subscribe_error(
        self, mock_exists, mock_list, topics, exception, ensign
    ):
        ensign.topics.add("otters", ULID())
        mock_exists.return_value = (None, False)
        mock_list.return_value = ([topic_pb2.Topic(name="otters", id=ULID().bytes)], "")
        with pytest.raises(exception):
            async for _ in ensign.subscribe(topics):
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
    @patch("pyensign.connection.Client.en_sql")
    async def test_query(self, mock_en_sql, ensign):
        mock_en_sql.return_value = Cursor()
        cursor = await ensign.query("SELECT * FROM otters")
        mock_en_sql.assert_called_once_with("SELECT * FROM otters", params=[])
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
        topics = [topic_pb2.Topic(id=ULID().bytes, name="otters")]
        mock_list.return_value = (topics, "")
        recv = await ensign.get_topics()
        assert recv[0].name == topics[0].name

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
    async def test_destroy_topic(self, mock_destroy, ensign):
        mock_destroy.return_value = (
            ULID().bytes,
            topic_pb2.TopicTombstone.Status.DELETING,
        )
        success = await ensign.destroy_topic("otters")
        assert success

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.destroy_topic")
    async def test_destroy_topic_error(self, mock_destroy, ensign):
        mock_destroy.return_value = (
            ULID().bytes,
            topic_pb2.TopicTombstone.Status.UNKNOWN,
        )
        success = await ensign.destroy_topic("otters")
        assert not success

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
        expected = ensign_pb2.ProjectInfo(
            project_id=ULID().bytes,
            num_topics=3,
            num_readonly_topics=1,
            events=100,
        )
        mock_info.return_value = expected
        actual = await ensign.info()
        assert actual == expected

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.info")
    async def test_info_filter(self, mock_info, ensign):
        expected = ensign_pb2.ProjectInfo(
            project_id=ULID().bytes,
            num_topics=3,
            num_readonly_topics=1,
            events=100,
        )
        mock_info.return_value = expected
        topic_ids = [str(ULID()), str(ULID())]
        actual = await ensign.info(topic_ids=topic_ids)

        # Ensure that ID bytes, not ULIDs, are passed to the client
        id_bytes = [ULID.from_str(id).bytes for id in topic_ids]
        mock_info.assert_called_once_with(id_bytes)
        assert actual == expected

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
