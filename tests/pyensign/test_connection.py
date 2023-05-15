import os
import pytest
import asyncio
from ulid import ULID
from grpc import RpcError
from pytest_httpserver import HTTPServer

from pyensign.connection import Client
from pyensign.api.v1beta1 import event_pb2
from pyensign.api.v1beta1 import topic_pb2
from pyensign.connection import Connection
from pyensign.auth.client import AuthClient
from pyensign.api.v1beta1 import ensign_pb2
from pyensign.api.v1beta1 import ensign_pb2_grpc


@pytest.fixture
def live(request):
    return request.config.getoption("--live")


@pytest.fixture
def authserver():
    return os.environ.get("ENSIGN_AUTH_SERVER")


@pytest.fixture
def ensignserver():
    return os.environ.get("ENSIGN_SERVER")


@pytest.fixture(scope="module")
def grpc_add_to_server():
    from pyensign.api.v1beta1.ensign_pb2_grpc import add_EnsignServicer_to_server

    return add_EnsignServicer_to_server


@pytest.fixture(scope="module")
def grpc_servicer():
    return MockServicer()


@pytest.fixture(scope="module")
def grpc_create_channel(request, grpc_addr, grpc_server):
    def _create_channel():
        from grpc.experimental import aio

        return aio.insecure_channel(grpc_addr)

    return _create_channel


@pytest.fixture(scope="module")
def grpc_channel(grpc_create_channel):
    return grpc_create_channel()


@pytest.fixture(scope="module")
def client(grpc_channel):
    return Client(MockConnection(grpc_channel))


@pytest.fixture
def auth(httpserver: HTTPServer):
    creds = {"client_id": "id", "client_secret": "secret"}
    return AuthClient(httpserver.url_for(""), creds)


@pytest.fixture
def creds():
    return {
        "client_id": os.environ.get("ENSIGN_CLIENT_ID"),
        "client_secret": os.environ.get("ENSIGN_CLIENT_SECRET"),
    }


@pytest.fixture
def rpc_error_coro():
    async def coro():
        raise RpcError("error")

    return coro


class TestConnection:
    """
    Test establishing a connection to an Ensign server.
    """

    def test_connect(self):
        conn = Connection("localhost:5356")
        assert conn.channel is not None

    def test_connect_insecure(self):
        conn = Connection("localhost:5356", insecure=True)
        assert conn.channel is not None

    def test_connect_secure(self, auth):
        conn = Connection("localhost:5356", auth=auth)
        assert conn.channel is not None

    @pytest.mark.parametrize(
        "addrport",
        [
            "localhost",
            "localhost:",
            ":5356",
            "localhost:5356:5356" "https://localhost:5356",
        ],
    )
    def test_connect_bad_addr(self, addrport):
        with pytest.raises(ValueError):
            Connection(addrport)

    def test_connect_bad_args(self):
        with pytest.raises(ValueError):
            Connection(
                "localhost:5356", insecure=True, auth=AuthClient("localhost:5356", {})
            )


class MockConnection(Connection):
    def __init__(self, channel):
        self.channel = channel
        pass


class MockServicer(ensign_pb2_grpc.EnsignServicer):
    """
    Minimal mock of the Ensign service so we can exercise the client code directly in
    tests.
    """

    def Publish(self, request_iterator, context):
        stream_ready = ensign_pb2.StreamReady(
            client_id="client_id",
            server_id="server_id",
            topics={"topic_name": ULID().bytes},
        )
        yield ensign_pb2.PublisherReply(ready=stream_ready)

        for _ in request_iterator:
            ack = ensign_pb2.Ack(id=ULID().bytes)
            yield ensign_pb2.PublisherReply(ack=ack)

        yield ensign_pb2.PublisherReply(close_stream=ensign_pb2.CloseStream())

    def Subscribe(self, request_iterator, context):
        stream_ready = ensign_pb2.StreamReady(
            client_id="client_id",
            server_id="server_id",
            topics={"topic_name": ULID().bytes},
        )
        yield ensign_pb2.SubscribeReply(ready=stream_ready)

        for _ in request_iterator:
            ew = event_pb2.EventWrapper(
                event=event_pb2.Event(data=b"data").SerializeToString()
            )
            yield ensign_pb2.SubscribeReply(event=ew)

        yield ensign_pb2.SubscribeReply(close_stream=ensign_pb2.CloseStream())

    def ListTopics(self, request, context):
        topics = [
            topic_pb2.Topic(name="expresso"),
            topic_pb2.Topic(name="arabica"),
        ]
        return topic_pb2.TopicsPage(topics=topics, next_page_token="next")

    def CreateTopic(self, request, context):
        return topic_pb2.Topic(id=request.id)

    def RetrieveTopic(self, request, context):
        return topic_pb2.Topic(id=request.id)

    def DeleteTopic(self, request, context):
        return topic_pb2.TopicTombstone(id=request.id)

    def TopicNames(self, request, context):
        names = [
            topic_pb2.TopicName(name="expresso"),
            topic_pb2.TopicName(name="arabica"),
        ]
        return topic_pb2.TopicNamesPage(topic_names=names, next_page_token="next")

    def TopicExists(self, request, context):
        return topic_pb2.TopicExistsInfo(query="query", exists=True)

    def Info(self, request, context):
        return ensign_pb2.ProjectInfo(
            project_id=str(ULID()),
            topics=3,
            readonly_topics=1,
            events=100,
        )

    def Status(self, request, context):
        return ensign_pb2.ServiceState(
            status=1,
            version="version",
        )


@pytest.mark.asyncio
class TestClient:
    """
    Test that the client uses the stub correctly.
    """

    async def test_publish(self, client):
        events = [
            event_pb2.Event(),
            event_pb2.Event(),
        ]
        async for rep in client.publish(ULID(), iter(events)):
            assert isinstance(rep, ensign_pb2.Ack)

    async def test_subscribe(self, client):
        async for rep in client.subscribe(topic_ids=iter(["expresso", "arabica"])):
            assert isinstance(rep, event_pb2.Event)

    async def test_pub_sub(self, client):
        events = [
            event_pb2.Event(),
            event_pb2.Event(),
        ]
        async for rep in client.publish(ULID(), iter(events)):
            await asyncio.sleep(0.01)
            assert isinstance(rep, ensign_pb2.Ack)

        async for rep in client.subscribe(topic_ids=iter(["expresso", "arabica"])):
            await asyncio.sleep(0.01)
            assert isinstance(rep, event_pb2.Event)

    async def test_list_topics(self, client):
        topics, next_page_token = await client.list_topics()
        assert len(topics) == 2
        assert next_page_token == "next"

    async def test_create_topic(self, client):
        id = ULID().bytes
        topic = await client.create_topic(topic_pb2.Topic(id=id))
        assert topic.id == id

    async def test_retrieve_topic(self, client):
        id = ULID().bytes
        topic = await client.retrieve_topic(id)
        assert topic.id == id

    async def test_archive_topic(self, client):
        id, state = await client.archive_topic("1")
        assert id == "1"
        assert isinstance(state, int)

    async def test_destroy_topic(self, client):
        id, state = await client.destroy_topic("1")
        assert id == "1"
        assert isinstance(state, int)

    async def test_topic_names(self, client):
        names, next_page_token = await client.topic_names()
        assert len(names) == 2
        assert next_page_token == "next"

    async def test_topic_exists(self, client):
        query, exists = await client.topic_exists("topic_id", "project_id", "expresso")
        assert query == "query"
        assert exists is True

    async def test_info(self, client):
        topic_ids = [ULID().bytes, ULID().bytes]
        info = await client.info(topics=topic_ids)
        assert ULID.from_str(info.project_id) is not None
        assert info.topics > 0
        assert info.readonly_topics > 0
        assert info.events > 0

    async def test_status(self, client):
        status, version, uptime, not_before, not_after = await client.status()
        assert status is not None
        assert version is not None
        assert uptime is not None
        assert not_before is not None
        assert not_after is not None

    async def test_insecure(self, live, ensignserver):
        if not live:
            pytest.skip("Skipping live test")
        if not ensignserver:
            pytest.fail("ENSIGN_SERVER environment variable not set")

        ensign = Client(Connection(ensignserver, insecure=True))
        status, version, uptime, not_before, not_after = await ensign.status()
        assert status is not None
        assert version is not None
        assert uptime is not None
        assert not_before is not None
        assert not_after is not None

    async def test_status_live(self, live, ensignserver):
        if not live:
            pytest.skip("Skipping live test")
        if not ensignserver:
            pytest.fail("ENSIGN_SERVER environment variable not set")

        ensign = Client(Connection(ensignserver))
        status, version, uptime, not_before, not_after = await ensign.status()
        assert status is not None
        assert version is not None
        assert uptime is not None
        assert not_before is not None
        assert not_after is not None

    async def test_auth_endpoint(self, live, ensignserver, authserver, creds):
        if not live:
            pytest.skip("Skipping live test")
        if not ensignserver:
            pytest.fail("ENSIGN_SERVER environment variable not set")
        if not authserver:
            pytest.fail("ENSIGN_AUTH_SERVER environment variable not set")
        if not creds:
            pytest.fail(
                "ENSIGN_CLIENT_ID and ENSIGN_CLIENT_SECRET environment variables not set"
            )

        ensign = Client(Connection(ensignserver, auth=AuthClient(authserver, creds)))
        topics, next_page_token = await ensign.list_topics()
        assert topics is not None
        assert next_page_token is not None
