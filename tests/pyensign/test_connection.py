import os
import pytest
from ulid import ULID

from pyensign.connection import Connection
from pyensign.connection import Client
from pyensign.api.v1beta1 import ensign_pb2
from pyensign.api.v1beta1 import event_pb2
from pyensign.api.v1beta1 import topic_pb2
from pyensign.api.v1beta1 import ensign_pb2_grpc


@pytest.fixture
def live(request):
    return request.config.getoption("--live")


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
def client(grpc_channel):
    return Client(MockConnection(grpc_channel))


class TestConnection:
    """
    Test establishing a connection to an Ensign server.
    """

    def test_connect(self):
        conn = Connection("localhost:5356")
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
        for request in request_iterator:
            ack = ensign_pb2.Ack(id=request.id)
            yield ensign_pb2.Publication(ack=ack)

    def Subscribe(self, request_iterator, context):
        for _ in request_iterator:
            yield event_pb2.Event()

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

    def Status(self, request, context):
        return ensign_pb2.ServiceState(
            status=1,
            version="version",
        )


class TestClient:
    """
    Test that the client uses the stub correctly.
    """

    def test_publish(self, client):
        events = [
            event_pb2.Event(id="1"),
            event_pb2.Event(id="2"),
        ]
        for rep in client.publish(iter(events)):
            assert isinstance(rep, ensign_pb2.Publication)

    def test_subscribe(self, client):
        for rep in client.subscribe(topics=["expresso", "arabica"]):
            assert isinstance(rep, event_pb2.Event)

    def test_list_topics(self, client):
        topics, next_page_token = client.list_topics()
        assert len(topics) == 2
        assert next_page_token == "next"

    def test_create_topic(self, client):
        id = ULID().bytes
        topic = client.create_topic(topic_pb2.Topic(id=id))
        assert topic.id == id

    def test_retrieve_topic(self, client):
        id = ULID().bytes
        topic = client.retrieve_topic(id)
        assert topic.id == id

    def test_archive_topic(self, client):
        id, state = client.archive_topic("1")
        assert id == "1"
        assert isinstance(state, int)

    def test_destroy_topic(self, client):
        id, state = client.destroy_topic("1")
        assert id == "1"
        assert isinstance(state, int)

    def test_topic_names(self, client):
        names, next_page_token = client.topic_names()
        assert len(names) == 2
        assert next_page_token == "next"

    def test_topic_exists(self, client):
        query, exists = client.topic_exists("topic_id", "project_id", "expresso")
        assert query == "query"
        assert exists is True

    def test_status(self, client):
        status, version, uptime, not_before, not_after = client.status()
        assert status is not None
        assert version is not None
        assert uptime is not None
        assert not_before is not None
        assert not_after is not None

    def test_status_live(self, live, ensignserver):
        if not live:
            pytest.skip("Skipping live test")
        if not ensignserver:
            pytest.fail("ENSIGN_SERVER environment variable not set")

        ensign = Client(Connection(ensignserver))
        status, version, uptime, not_before, not_after = ensign.status()
        assert status is not None
        assert version is not None
        assert uptime is not None
        assert not_before is not None
        assert not_after is not None
