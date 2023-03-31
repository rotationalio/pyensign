import os
import mmh3
import base64
import pytest
import asyncio
from ulid import ULID
from unittest import mock
from asyncmock import patch

from pyensign.ensign import Ensign
from pyensign.events import Event
from pyensign.api.v1beta1 import ensign_pb2
from pyensign.api.v1beta1 import event_pb2
from pyensign.api.v1beta1 import topic_pb2
from pyensign.exceptions import EnsignTopicCreateError
from pyensign.mimetype.v1beta1.mimetype_pb2 import MIME


@pytest.fixture
def live(request):
    return request.config.getoption("--live", default=False)


@pytest.fixture
def authserver():
    return os.environ.get("ENSIGN_AUTH_SERVER")


@pytest.fixture
def ensignserver():
    return os.environ.get("ENSIGN_SERVER")


@pytest.fixture
def ensign():
    return Ensign(client_id="id", client_secret="secret", endpoint="localhost:1234")


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

    @pytest.mark.parametrize(
        "client_id, client_secret, exception",
        [
            (None, None, ValueError),
            ("", "", ValueError),
            ("id", "", ValueError),
            ("", "secret", ValueError),
            (1, 2, TypeError),
        ],
    )
    def test_bad_creds(self, client_id, client_secret, exception):
        with pytest.raises(exception), mock.patch.dict(os.environ, {}, clear=True):
            Ensign(client_id=client_id, client_secret=client_secret)

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.list_topics")
    @patch("pyensign.connection.Client.publish")
    async def test_publish_exists(self, mock_publish, mock_list, ensign):
        name = "otters"
        pubs = [
            ensign_pb2.Publication(ack=ensign_pb2.Ack()),
            ensign_pb2.Publication(nack=ensign_pb2.Nack()),
            ensign_pb2.Publication(close_stream=ensign_pb2.CloseStream()),
        ]
        mock_publish.return_value = async_iter(pubs)
        mock_list.return_value = ([topic_pb2.Topic(name=name, id=ULID().bytes)], "")

        # Publish two events, one of them errors
        events = [
            Event(b'{"foo": "bar"}', MIME.APPLICATION_JSON),
            Event(b'{"bar": "bz"}', MIME.APPLICATION_JSON),
        ]

        errors = await ensign.publish(name, events)
        assert len(errors) == 1

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.list_topics")
    @patch("pyensign.connection.Client.create_topic")
    @patch("pyensign.connection.Client.publish")
    async def test_publish_not_exists(
        self, mock_publish, mock_create, mock_list, ensign
    ):
        name = "otters"
        id = ULID().bytes
        pubs = [
            ensign_pb2.Publication(ack=ensign_pb2.Ack()),
            ensign_pb2.Publication(nack=ensign_pb2.Nack()),
            ensign_pb2.Publication(close_stream=ensign_pb2.CloseStream()),
        ]
        mock_publish.return_value = async_iter(pubs)
        mock_create.return_value = topic_pb2.Topic(id=id, name=name)
        mock_list.return_value = ([], "")

        # Publish two events, one of them errors
        events = [
            Event(b'{"foo": "bar"}', MIME.APPLICATION_JSON),
            Event(b'{"bar": "bz"}', MIME.APPLICATION_JSON),
        ]
        errors = await ensign.publish(name, events)
        assert len(errors) == 1

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.status")
    async def test_status(self, mock_status, ensign):
        mock_status.return_value = ("AVAILABLE", "1.0.0", "10 minutes", "", "")
        status = await ensign.status()
        assert status == "status: AVAILABLE\nversion: 1.0.0\nuptime: 10 minutes"

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.subscribe")
    async def test_subscribe(self, mock_subscribe, ensign):
        events = [
            event_pb2.Event(
                id=str(ULID()),
                data=b'{"foo": "bar"}',
                mimetype=MIME.APPLICATION_JSON,
            ),
            event_pb2.Event(
                id=str(ULID()),
                data=b'{"bar": "bz"}',
                mimetype=MIME.APPLICATION_JSON,
            ),
        ]
        mock_subscribe.return_value = async_iter(events)
        recv = []
        async for event in ensign.subscribe("otters", "lighthouses"):
            recv.append(event)
            if len(recv) == 2:
                break
        assert recv[0].data == events[0].data
        assert recv[1].data == events[1].data

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
        id = ULID().bytes
        mock_create.return_value = topic_pb2.Topic(id=id, name="otters")
        topic = await ensign.create_topic("otters")
        assert topic.id == id

    @pytest.mark.asyncio
    @patch("pyensign.connection.Client.create_topic")
    async def test_create_topic_error(self, mock_create, ensign):
        mock_create.return_value = None
        with pytest.raises(EnsignTopicCreateError):
            await ensign.create_topic("otters")

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
    async def test_live_pubsub(self, live, authserver, ensignserver):
        if not live:
            pytest.skip("Skipping live test")
        if not authserver:
            pytest.skip("Skipping live test")
        if not ensignserver:
            pytest.skip("Skipping live test")

        ensign = Ensign(endpoint=ensignserver, auth_url=authserver)
        event = Event(b"message in a bottle", "TEXT_PLAIN")
        topic = "pyensign-pub-sub"

        # Ensure the topic exists
        if not await ensign.topic_exists(topic):
            await ensign.create_topic(topic)

        # Run publish and subscribe as coroutines
        async def pub():
            # Delay the publisher to prevent deadlock
            await asyncio.sleep(1)
            return await ensign.publish(topic, event)

        async def sub():
            id = await ensign.topic_id(topic)
            async for event in ensign.subscribe(id):
                return event

        errors, event = await asyncio.gather(pub(), sub())
        assert len(errors) == 0
        assert event.data == b"message in a bottle"
        assert event.mimetype == MIME.TEXT_PLAIN
        assert event.type.name == "Generic"
        assert event.type.version == 1
