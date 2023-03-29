import os
import json
import pytest
import asyncio
from ulid import ULID
from unittest import mock

from pyensign.ensign import Ensign
from pyensign.api.v1beta1 import event_pb2
from pyensign.api.v1beta1 import topic_pb2
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
    async def test_live_pubsub(self, live, authserver, ensignserver):
        if not live:
            pytest.skip("Skipping live test")
        if not authserver:
            pytest.skip("Skipping live test")
        if not ensignserver:
            pytest.skip("Skipping live test")

        ensign = Ensign(endpoint=ensignserver, auth_url=authserver)

        # Get or create the topic
        name = "pyensign-pub-sub"
        topic_id = ""
        if not await ensign.topic_exists(name):
            topic = topic_pb2.Topic(name=name)
            await ensign.create_topic(topic)
            topic_id = str(ULID.from_bytes(topic.id))
        else:
            topics = await ensign.get_topics()
            for t in topics:
                if t.name == name:
                    topic_id = str(ULID.from_bytes(t.id))
                    break

        # Create an event
        data = json.dumps({"foo": "bar"}).encode("utf-8")
        type = event_pb2.Type(name="Generic", version=1)
        event = event_pb2.Event(
            topic_id=topic_id, data=data, mimetype=MIME.APPLICATION_JSON, type=type
        )

        # Run publish and subscribe as coroutines
        async def pub():
            # Allow the subscriber to start before publishing to prevent deadlock
            await asyncio.sleep(1)
            return await ensign.publish(iter([event]))

        async def sub():
            async for event in ensign.subscribe([topic_id]):
                return event

        errors, event = await asyncio.gather(pub(), sub())
        assert len(errors) == 0
        assert event.data == data
        assert event.mimetype == MIME.APPLICATION_JSON
        assert event.type.name == "Generic"
        assert event.type.version == 1
