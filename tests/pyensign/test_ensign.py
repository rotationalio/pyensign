import os
import pytest
import asyncio
from ulid import ULID
from unittest import mock
from asyncmock import patch

from pyensign.ensign import Ensign
from pyensign.events import Event
from pyensign.utils.topics import Topic
from pyensign.api.v1beta1 import ensign_pb2
from pyensign.api.v1beta1 import topic_pb2
from pyensign.exceptions import (
    EnsignTopicCreateError,
    EnsignTopicNotFoundError,
    UnknownTopicError,
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
def ensign():
    return Ensign(client_id="id", client_secret="secret", endpoint="localhost:1234")


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
        await ensign.subscribe(topics)

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
            await ensign.subscribe(topics)

    @pytest.mark.asyncio
    async def test_subscribe_no_topics(self, ensign):
        with pytest.raises(ValueError):
            await ensign.subscribe()

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
            project_id=str(ULID),
            topics=3,
            readonly_topics=1,
            events=100,
        )
        mock_info.return_value = expected
        actual = await ensign.info()
        assert actual == expected

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.info")
    async def test_info_filter(self, mock_info, ensign):
        expected = ensign_pb2.ProjectInfo(
            project_id=str(ULID),
            topics=3,
            readonly_topics=1,
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
        event = Event(b"message in a bottle", "text/plain")
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
            await asyncio.sleep(1)
            await ensign.publish(topic, event, on_ack=handle_ack)
            await publish_ack.wait()

        received = None
        subscribe_ack = asyncio.Event()

        async def ack_event(e):
            nonlocal received
            await e.ack()
            subscribe_ack.set()
            received = e

        async def sub():
            await ensign.subscribe(topic, on_event=ack_event)
            await subscribe_ack.wait()
            return received

        _, recevied = await asyncio.gather(pub(), sub())
        assert event.acked()
        assert not event.nacked()
        assert received.acked()
        assert not received.nacked()
        assert recevied.data == b"message in a bottle"
        assert recevied.mimetype == MIME.TEXT_PLAIN
        assert recevied.type.name == "Generic"
        assert recevied.type.major_version == 1
        assert recevied.type.minor_version == 0
        assert recevied.type.patch_version == 0
        assert recevied.id
