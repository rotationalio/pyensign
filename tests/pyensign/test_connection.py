import os
import pytest
import asyncio
from datetime import timedelta
from asyncmock import patch

from ulid import ULID
from grpc import RpcError, StatusCode
from grpc.aio import AioRpcError
from grpc.aio._interceptor import InterceptedUnaryStreamCall

from pyensign.events import Event
from pyensign.utils.topics import Topic
from pyensign.connection import Client
from pyensign.utils.cache import Cache
from pyensign.api.v1beta1 import event_pb2
from pyensign.api.v1beta1 import topic_pb2, query_pb2
from pyensign.connection import Connection
from pyensign.auth.client import AuthClient
from pyensign.api.v1beta1 import ensign_pb2
from pyensign.api.v1beta1 import ensign_pb2_grpc

from pyensign.exceptions import (
    UnknownTopicError,
    EnsignTimeoutError,
    AuthenticationError,
    EnsignRPCError,
    QueryNoRows,
    NackError,
)


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


@pytest.fixture()
def client(grpc_addr, grpc_server, auth):
    """
    This defines a client fixture that connects to the mock gRPC service which is
    listening on grpc_addr, with reconnect timeouts which are intentionally very short
    to test reconnect logic.
    TODO: We currently have to include grpc_server here to force pytest to load the
    server fixture which is defined in the pytest-grpc library and start the server,
    need to figure out a better way of handling that.
    """
    return Client(
        Connection(grpc_addr, insecure=True, auth=auth),
        topic_cache=Cache(),
        reconnect_tick=timedelta(milliseconds=1),
        reconnect_timeout=timedelta(milliseconds=5),
    )


@pytest.fixture()
def client_reconnect_timeout(grpc_addr, grpc_server, auth):
    """
    This defines a client fixture that connects to the mock gRPC service but uses the
    default reconnect timeout, which should be long enough to cause reconnect timeouts
    in tests.
    """
    return Client(
        Connection(grpc_addr, insecure=True, auth=auth),
        topic_cache=Cache(),
    )


@pytest.fixture()
def client_no_reconnect(grpc_addr, grpc_server, auth):
    """
    This defines a client fixture that connects to the mock gRPC service but uses no
    reconnect timeout to test timeout logic.
    """
    return Client(
        Connection(grpc_addr, insecure=True, auth=auth),
        topic_cache=Cache(),
        reconnect_tick=timedelta(milliseconds=1),
        reconnect_timeout=timedelta(milliseconds=0),
    )


@pytest.fixture
def auth():
    return MockAuthClient(
        "localhost:5356", {"client_id": "id", "client_secret": "secret"}
    )


class MockAuthClient(AuthClient):
    def credentials(self):
        return ("authorization", "Bearer supersecretsquirrel")


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


def async_iter(items):
    async def next():
        for item in items:
            yield item

    return next()


class TestConnection:
    """
    Test establishing a connection to an Ensign server.
    """

    def test_connect(self):
        conn = Connection("localhost:5356")
        assert conn.create_channel() is not None

    def test_connect_insecure(self):
        conn = Connection("localhost:5356", insecure=True)
        assert conn.create_channel() is not None

    def test_connect_secure(self, auth):
        conn = Connection("localhost:5356", auth=auth)
        assert conn.create_channel() is not None

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


OTTERS_TOPIC = Topic(id=ULID(), name="otters")


class MockServicer(ensign_pb2_grpc.EnsignServicer):
    """
    Minimal mock of the Ensign service so we can exercise the client code directly in
    tests. This service checks that the correct types are being sent by the client. The
    asserts will manifest as AioRpcErrors in the client but will also be visible in the
    pytest output.
    """

    def authorize(fn):
        def wrap(*args, **kwargs):
            self = args[0]
            self.check_authorize(args[2])
            return fn(*args, **kwargs)

        return wrap

    def user_agent(fn):
        def wrap(*args, **kwargs):
            self = args[0]
            self.check_user_agent(args[2])
            return fn(*args, **kwargs)

        return wrap

    def check_authorize(self, context):
        """
        Check that the client is sending an authorization header.
        """
        for meta in context.invocation_metadata():
            if "authorization" in meta.key:
                assert meta.value == "Bearer supersecretsquirrel"
                return True

        assert False, "No authorization header sent by client"

    def check_user_agent(self, context):
        """
        Check that the client is setting the PyEnsign user agent.
        """
        for meta in context.invocation_metadata():
            if "user-agent" in meta.key:
                assert "pyensign" in meta.value
                return True

        assert False, "No user-agent header sent by client"

    @authorize
    @user_agent
    def Publish(self, request_iterator, context):
        # First client request should be an open_stream
        req = next(request_iterator)
        assert isinstance(req, ensign_pb2.PublisherRequest)
        assert req.WhichOneof("embed") == "open_stream"

        # Send back stream_ready to progress the client
        stream_ready = ensign_pb2.StreamReady(
            client_id="client_id",
            server_id="server_id",
            topics={OTTERS_TOPIC.name: OTTERS_TOPIC.id.bytes},
        )
        yield ensign_pb2.PublisherReply(ready=stream_ready)

        for i in range(3):
            # Ensure the client is only sending events
            req = next(request_iterator)
            assert isinstance(req, ensign_pb2.PublisherRequest)
            assert req.WhichOneof("embed") == "event"

            # Alternate acks/nacks to the client for full testability
            if i % 2 == 0:
                ack = ensign_pb2.Ack(id=req.event.local_id)
                yield ensign_pb2.PublisherReply(ack=ack)
            else:
                nack = ensign_pb2.Nack(id=req.event.local_id)
                yield ensign_pb2.PublisherReply(nack=nack)

        yield ensign_pb2.PublisherReply(close_stream=ensign_pb2.CloseStream())

    @authorize
    @user_agent
    def Subscribe(self, request_iterator, context):
        # First client request should be a subscription
        req = next(request_iterator)
        assert isinstance(req, ensign_pb2.SubscribeRequest)
        assert req.WhichOneof("embed") == "subscription"

        # Send back stream_ready to progress the client
        stream_ready = ensign_pb2.StreamReady(
            client_id="client_id",
            server_id="server_id",
            topics={OTTERS_TOPIC.name: OTTERS_TOPIC.id.bytes},
        )
        yield ensign_pb2.SubscribeReply(ready=stream_ready)

        for i in range(3):
            # Send back an event to the client
            ew = event_pb2.EventWrapper(
                id=ULID().bytes,
                event=event_pb2.Event(
                    data="event {}".format(i).encode(),
                    type=event_pb2.Type(
                        name="message",
                        major_version=1,
                        minor_version=2,
                        patch_version=3,
                    ),
                ).SerializeToString(),
            )
            yield ensign_pb2.SubscribeReply(event=ew)

            # Ensure the client is only sending acks or nacks
            req = next(request_iterator)
            assert isinstance(req, ensign_pb2.SubscribeRequest)
            assert req.WhichOneof("embed") in ("ack", "nack")

        yield ensign_pb2.SubscribeReply(close_stream=ensign_pb2.CloseStream())

    @authorize
    @user_agent
    def EnSQL(self, request, context):
        for i in range(3):
            yield event_pb2.EventWrapper(
                id=ULID().bytes,
                event=event_pb2.Event(
                    data="event {}".format(i).encode(),
                    type=event_pb2.Type(
                        name="message",
                        major_version=1,
                        minor_version=2,
                        patch_version=3,
                    ),
                ).SerializeToString(),
            )

    @authorize
    @user_agent
    def ListTopics(self, request, context):
        topics = [
            topic_pb2.Topic(name="expresso"),
            topic_pb2.Topic(name="arabica"),
        ]
        return topic_pb2.TopicsPage(topics=topics, next_page_token="next")

    @authorize
    @user_agent
    def CreateTopic(self, request, context):
        return topic_pb2.Topic(id=request.id)

    @authorize
    @user_agent
    def RetrieveTopic(self, request, context):
        return topic_pb2.Topic(id=request.id)

    @authorize
    @user_agent
    def DeleteTopic(self, request, context):
        return topic_pb2.TopicStatus(id=request.id)

    @authorize
    @user_agent
    def TopicNames(self, request, context):
        names = [
            topic_pb2.TopicName(name="expresso"),
            topic_pb2.TopicName(name="arabica"),
        ]
        return topic_pb2.TopicNamesPage(topic_names=names, next_page_token="next")

    @authorize
    @user_agent
    def TopicExists(self, request, context):
        return topic_pb2.TopicExistsInfo(query="query", exists=True)

    @authorize
    @user_agent
    def Info(self, request, context):
        return ensign_pb2.ProjectInfo(
            project_id=ULID().bytes,
            num_topics=3,
            num_readonly_topics=1,
            events=100,
        )

    @user_agent
    def Status(self, request, context):
        return ensign_pb2.ServiceState(
            status=1,
            version="version",
        )


class TestClient:
    """
    Test that the client uses the stub correctly.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "topic",
        [
            Topic(id=OTTERS_TOPIC.id),
            Topic(name=OTTERS_TOPIC.name),
            OTTERS_TOPIC,
        ],
    )
    async def test_publish(self, topic, client):
        events = [
            Event(data=b"event 1", mimetype="text/plain"),
            Event(data=b"event 2", mimetype="text/plain"),
        ]
        ack_ids = []
        nack_ids = []

        async def source_events(events):
            for event in events:
                yield event

        async def on_ack(ack):
            nonlocal ack_ids
            ack_ids.append(ack.id)

        async def on_nack(nack):
            nonlocal nack_ids
            nack_ids.append(nack.id)

        await client.publish(
            topic, source_events(events), on_ack=on_ack, on_nack=on_nack
        )

        # Should be able to resume publishing to an existing stream.
        more_events = [
            Event(data=b"event 3", mimetype="text/plain"),
        ]
        await client.publish(
            topic, source_events(more_events), on_ack=on_ack, on_nack=on_nack
        )

        # Wait for all the events to be acked or nacked.
        await events[0].wait_for_ack()
        assert events[0].acked()
        with pytest.raises(NackError):
            await events[1].wait_for_ack()
        assert events[1].nacked()
        await more_events[0].wait_for_ack()
        assert more_events[0].acked()

        await client.close()
        assert len(ack_ids) + len(nack_ids) == len(events) + len(more_events)

        # Topic IDs from the server should be saved in the client.
        id = client.topics.get(OTTERS_TOPIC.name)
        assert id == OTTERS_TOPIC.id

    @pytest.mark.asyncio
    async def test_publish_reconnect(self, client):
        ack_ids = []
        published = asyncio.Event()

        async def source_events():
            while True:
                await asyncio.sleep(0.1)
                yield Event(data=b"event", mimetype="text/plain")

        # Record acks, only close the stream after two successful connections.
        async def record_acks(ack):
            nonlocal ack_ids
            ack_ids.append(ack.id)
            if len(ack_ids) >= 6:
                published.set()

        # Publish events to a server that closes the stream every 3 events.
        await client.publish(OTTERS_TOPIC, source_events(), on_ack=record_acks)
        await published.wait()

        # The client should have reconnected at least once and the mock server sends 3
        # acks per connection.
        await client.close()
        assert len(ack_ids) % 3 == 0

        # Topic IDs from the server should be saved in the client.
        id = client.topics.get(OTTERS_TOPIC.name)
        assert id == OTTERS_TOPIC.id

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "topic",
        [
            Topic(id=ULID()),
            Topic(name="unknown-topic"),
        ],
    )
    async def test_publish_unknown_topic(self, topic, client):
        with pytest.raises(UnknownTopicError):
            await client.publish(topic, [])
        await client.close()

    @pytest.mark.asyncio
    async def test_publish_ack_error(self, caplog, client):
        """
        Test when an exception is raised inside an ack handler it is caught and logged.
        """

        async def on_ack(ack):
            raise Exception("I could not process this ack", ack)

        async def on_nack(nack):
            raise Exception("I could not process this nack", nack)

        acked_event = Event(data=b"event", mimetype="text/plain")
        nacked_event = Event(data=b"event", mimetype="text/plain")

        async def source_events():
            yield acked_event
            yield nacked_event

        await client.publish(
            OTTERS_TOPIC, source_events(), on_ack=on_ack, on_nack=on_nack
        )
        await acked_event.wait_for_ack()
        with pytest.raises(NackError):
            await nacked_event.wait_for_ack()
        await client.close()
        assert "I could not process this ack" in caplog.text
        assert "I could not process this nack" in caplog.text

    def test_publish_sync(self, client):
        """
        Test executing publish from synchronous code as a coroutine.
        """
        published = False

        async def handle_ack(ack):
            assert isinstance(ack, ensign_pb2.Ack)
            nonlocal published
            published = True

        async def publish():
            await client.publish(
                OTTERS_TOPIC,
                async_iter([Event(data=b"event", mimetype="text/plain")]),
                on_ack=handle_ack,
            )
            while not published:
                await asyncio.sleep(0.1)
            await client.close()

        asyncio.run(publish())

    def test_publish_cancelled(self, client_reconnect_timeout):
        """
        Test that async publish tasks exit gracefully when cancelled. On fail this test
        will hang. In the real world, we don't want code with PyEnsign in it to hang if
        the user code has already returned.
        """

        async def publish():
            await client_reconnect_timeout.publish(
                OTTERS_TOPIC,
                async_iter([Event(data=b"event", mimetype="text/plain")]),
            )

        asyncio.run(publish())

    @pytest.mark.asyncio
    async def test_subscribe(self, client):
        topic_ids = [str(ULID()), str(ULID())]
        event_ids = []

        # Consume 2 events from the stream.
        async for event in client.subscribe(topic_ids):
            assert isinstance(event, Event)
            await event.ack()
            event_ids.append(event.id)
            if len(event_ids) == 2:
                break

        # Resume consuming events from the stream.
        async for event in client.subscribe(topic_ids):
            assert isinstance(event, Event)
            await event.ack()
            event_ids.append(event.id)
            if len(event_ids) == 3:
                break

        # Event IDs should be unique.
        assert len(event_ids) == len(set(event_ids))

        # Topic IDs from the server should be saved in the client.
        id = client.topics.get(OTTERS_TOPIC.name)
        assert isinstance(id, ULID)

    @pytest.mark.asyncio
    async def test_subscribe_query(self, client):
        topic_ids = [str(ULID()), str(ULID())]
        event_ids = []

        # Consume events from the stream.
        query = query_pb2.Query(
            query="SELECT * FROM topic",
            params=[query_pb2.Parameter(name="intparam", i=42)],
        )
        async for event in client.subscribe(
            topic_ids,
            query=query,
        ):
            assert isinstance(event, Event)
            await event.ack()
            event_ids.append(event.id)
            if len(event_ids) == 3:
                break

        # Event IDs should be unique.
        assert len(event_ids) == len(set(event_ids))

        # Topic IDs from the server should be saved in the client.
        id = client.topics.get(OTTERS_TOPIC.name)
        assert isinstance(id, ULID)

    @pytest.mark.asyncio
    async def test_subscribe_reconnect(self, client):
        topic_ids = [str(ULID()), str(ULID())]
        event_ids = []

        # The mock server only sends 3 events per connection, so consuming more than 3
        # events will cause a reconnect which should be transparent to the client.
        async for event in client.subscribe(topic_ids):
            assert isinstance(event, Event)
            await event.ack()
            event_ids.append(event.id)
            if len(event_ids) == 6:
                break

        # Event IDs should be unique.
        assert len(event_ids) == len(set(event_ids))

        # Topic IDs from the server should be saved in the client.
        id = client.topics.get(OTTERS_TOPIC.name)
        assert isinstance(id, ULID)

    @pytest.mark.asyncio
    async def test_subscribe_timeout(self, client_no_reconnect):
        topic_ids = [str(ULID()), str(ULID())]

        # The mock server only sends 3 events per connection, so consuming more than 3
        # events will cause a reconnect which should timeout.
        with pytest.raises(EnsignTimeoutError):
            async for event in client_no_reconnect.subscribe(topic_ids):
                assert isinstance(event, Event)
                await event.ack()

    def test_subscribe_sync(self, client):
        """
        Test executing subscribe from synchronous code as a coroutine.
        """

        async def consume():
            async for event in client.subscribe([str(ULID())]):
                assert isinstance(event, Event)
                await event.ack()
                break
            await client.close()

        asyncio.run(consume())

    @pytest.mark.asyncio
    async def test_pub_sub(self, client):
        topic = OTTERS_TOPIC
        events = [
            Event(data=b"event 1", mimetype="text/plain"),
            Event(data=b"event 2", mimetype="text/plain"),
            Event(data=b"event 3", mimetype="text/plain"),
        ]
        ack_ids = []
        nack_ids = []
        event_ids = []

        async def on_ack(ack):
            nonlocal ack_ids
            ack_ids.append(ack.id)

        async def on_nack(nack):
            nonlocal nack_ids
            nack_ids.append(nack.id)

        async def source_events():
            for event in events:
                yield event

        await client.publish(topic, source_events(), on_ack=on_ack, on_nack=on_nack)

        # Consume all the events
        async for event in client.subscribe(iter(["expresso", "arabica"])):
            assert isinstance(event, Event)
            await event.ack()
            assert str(event.type) == "message v1.2.3"
            event_ids.append(event.id)
            if len(event_ids) == 3:
                break

        await client.close()
        assert len(ack_ids) + len(nack_ids) == len(events)
        assert len(event_ids) == len(events)
        assert events[0].acked()
        assert events[1].nacked()
        assert events[2].acked()

    @pytest.mark.asyncio
    async def test_en_sql(self, client):
        # Consume the entire cursor at once
        cursor = await client.en_sql("SELECT * FROM topic")
        events = await cursor.fetchall()
        assert len(events) == 3
        assert events[0].data == b"event 0"
        assert events[1].data == b"event 1"
        assert events[2].data == b"event 2"
        assert await cursor.fetchone() is None

        # Consume the cursor using fetchone() and fetchmany()
        cursor = await client.en_sql("SELECT * FROM topic")
        event = await cursor.fetchone()
        assert event.data == b"event 0"
        events = await cursor.fetchmany(3)
        assert len(events) == 2
        assert events[0].data == b"event 1"
        assert events[1].data == b"event 2"
        assert await cursor.fetchmany(3) == []

        # Iterate over the cursor
        cursor = await client.en_sql("SELECT * FROM topic")
        i = 0
        async for event in cursor:
            assert event.data == b"event %d" % i
            i += 1
        assert i == 3
        assert await cursor.fetchall() == []

    @pytest.mark.asyncio
    @patch.object(InterceptedUnaryStreamCall, "read")
    async def test_en_sql_no_rows(self, read, client):
        read.side_effect = AioRpcError(StatusCode.CANCELLED, None, None)
        with pytest.raises(QueryNoRows):
            await client.en_sql("SELECT * FROM topic")

    @pytest.mark.asyncio
    @patch.object(InterceptedUnaryStreamCall, "read")
    async def test_en_sql_invalid_query(self, read, client):
        read.side_effect = AioRpcError(StatusCode.INVALID_ARGUMENT, None, None)
        with pytest.raises(EnsignRPCError):
            await client.en_sql("SELECT * FROM topic")

    @pytest.mark.asyncio
    async def test_list_topics(self, client):
        topics, next_page_token = await client.list_topics()
        assert len(topics) == 2
        assert next_page_token == "next"

    @pytest.mark.asyncio
    async def test_create_topic(self, client):
        id = ULID().bytes
        topic = await client.create_topic(topic_pb2.Topic(id=id))
        assert topic.id == id

    @pytest.mark.asyncio
    async def test_retrieve_topic(self, client):
        id = ULID().bytes
        topic = await client.retrieve_topic(id)
        assert topic.id == id

    @pytest.mark.asyncio
    async def test_archive_topic(self, client):
        id, state = await client.archive_topic("1")
        assert id == "1"
        assert isinstance(state, int)

    @pytest.mark.asyncio
    async def test_destroy_topic(self, client):
        id, state = await client.destroy_topic("1")
        assert id == "1"
        assert isinstance(state, int)

    @pytest.mark.asyncio
    async def test_topic_names(self, client):
        names, next_page_token = await client.topic_names()
        assert len(names) == 2
        assert next_page_token == "next"

    @pytest.mark.asyncio
    async def test_topic_exists(self, client):
        query, exists = await client.topic_exists("topic_id", "project_id", "expresso")
        assert query == "query"
        assert exists is True

    @pytest.mark.asyncio
    async def test_info(self, client):
        topic_ids = [ULID().bytes, ULID().bytes]
        info = await client.info(topics=topic_ids)
        assert ULID.from_bytes(info.project_id) is not None
        assert info.num_topics > 0
        assert info.num_readonly_topics > 0
        assert info.events > 0

    @pytest.mark.asyncio
    async def test_status(self, client):
        status, version, uptime, not_before, not_after = await client.status()
        assert status is not None
        assert version is not None
        assert uptime is not None
        assert not_before is not None
        assert not_after is not None

    @pytest.mark.asyncio
    @patch.object(MockAuthClient, "credentials")
    async def test_auth_error(self, credentials, client):
        """
        Test that authentication errors are propagated back to the caller.
        """
        credentials.side_effect = AuthenticationError("wrong credentials")
        with pytest.raises(EnsignRPCError):
            await client.list_topics()

        with pytest.raises(EnsignRPCError):
            await client.publish(
                OTTERS_TOPIC, iter([Event(data=b"event 1", mimetype="text/plain")])
            )

    @pytest.mark.asyncio
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

    @pytest.mark.asyncio
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

    @pytest.mark.asyncio
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
