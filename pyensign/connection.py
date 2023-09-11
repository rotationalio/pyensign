import grpc
import asyncio
from ulid import ULID
from grpc import aio
from datetime import timedelta

from pyensign.events import from_proto
from pyensign.version import user_agent
from pyensign.utils.tasks import WorkerPool
from pyensign.api.v1beta1.event import unwrap
from pyensign.exceptions import catch_rpc_error
from pyensign.api.v1beta1 import ensign_pb2_grpc
from pyensign.api.v1beta1 import topic_pb2, ensign_pb2, query_pb2
from pyensign.auth.interceptor import (
    MetadataUnaryInterceptor,
    MetadataUnaryStreamInterceptor,
    MetadataStreamInterceptor,
)
from pyensign.stream import Publisher, Subscriber
from pyensign.exceptions import (
    QueryNoRows,
    EnsignClientClosingError,
)


class Connection:
    """
    Connection defines a gRPC connection to an Ensign server.
    """

    def __init__(self, addrport, insecure=False, auth=None):
        """
        Connect to an Ensign server.

        Parameters
        ----------
        addrport : str
            The address:port of the Ensign server (e.g. "localhost:5356")
        insecure : bool
            Set to True to use an insecure connection. This is only useful for testing
            against a local Ensign server and overrides the auth parameter.
        auth : grpc.AuthMetadataPlugin or None
            Plugin that provides an access token for per-request authentication.
        """

        addrParts = addrport.split(":", 2)
        if len(addrParts) != 2 or not addrParts[0] or not addrParts[1]:
            raise ValueError("Invalid address:port format")
        self.addrport = addrport

        self.insecure = insecure
        self.auth = auth

    def create_channel(self):
        """
        Create a new gRPC channel from the connection parameters. The caller is
        responsible for closing the channel.
        """

        # Create interceptors for authentication
        interceptors = []
        if self.auth is not None:
            creds_fn = self.auth.credentials
            interceptors.append(MetadataUnaryInterceptor(creds_fn))
            interceptors.append(MetadataUnaryStreamInterceptor(creds_fn))
            interceptors.append(MetadataStreamInterceptor(creds_fn))

        # Add the user agent to the options
        options = (("grpc.primary_user_agent", user_agent()),)

        if self.insecure:
            channel = aio.insecure_channel(
                self.addrport, interceptors=interceptors, options=options
            )
        else:
            channel = aio.secure_channel(
                self.addrport,
                credentials=grpc.ssl_channel_credentials(),
                interceptors=interceptors,
                options=options,
            )

        return channel


class Client:
    """
    Client defines the actual gRPC client that makes requests to an Ensign server.
    """

    def __init__(
        self,
        connection,
        client_id="",
        topic_cache=None,
        reconnect_tick=timedelta(milliseconds=750),
        reconnect_timeout=timedelta(minutes=5),
    ):
        """
        Create a new client for making requests to Ensign. The connection is not
        established until the connect() method is called.

        Parameters
        ----------
        connection : Connection
            The connection to the Ensign server
        """

        self.connection = connection
        self.channel = None
        self.publishers = {}
        self.subscribers = {}
        self.pool = None
        self.topics = topic_cache
        self.reconnect_tick = reconnect_tick
        self.reconnect_timeout = reconnect_timeout
        self.shutdown = None

        # Create client ID if not provided, which exists for the lifetime of the client
        # and persists across reconnects
        if client_id == "":
            self.client_id = str(ULID())
        else:
            self.client_id = client_id

    async def __aenter__(self):
        self._ensure_ready()

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    def _ensure_ready(self):
        """
        Create the gRPC channel and stub if not already connected. This is done at call
        time rather than when the client is created to ensure that user code runs in
        the same event loop as the gRPC library. This function also creates the worker
        pool and shutdown event signal if not already created.
        TODO: We could avoid this by requiring the Ensign client to be created in the
        event loop, e.g. async with Client(...) as client: ..., but the current syntax
        is a bit easier.
        """

        if not self.channel:
            self.channel = self.connection.create_channel()
            self.stub = ensign_pb2_grpc.EnsignStub(self.channel)

        if not self.pool:
            self.pool = WorkerPool(max_workers=10, max_queue_size=100)

        if not self.shutdown:
            self.shutdown = asyncio.Event()

    @catch_rpc_error
    async def publish(self, topic, events, on_ack=None, on_nack=None):
        # Ensure we have a gRPC channel
        self._ensure_ready()

        if self.shutdown.is_set():
            raise EnsignClientClosingError("client is closing")

        # Attempt to hash the topic, which fails if there is no topic ID
        try:
            topic_hash = hash(topic)
        except ValueError:
            topic_hash = None

        # Check if there is already an open publish stream for this topic
        if topic_hash and topic_hash in self.publishers:
            publisher = self.publishers[topic_hash]
        else:
            # Create the publish stream for this topic
            publisher = Publisher(
                self,
                topic,
                on_ack=on_ack,
                on_nack=on_nack,
                reconnect_tick=self.reconnect_tick,
                reconnect_timeout=self.reconnect_timeout,
            )

            # Connect to the publish stream
            # TODO: Distinguish between authentication errors, topic errors, and connection errors
            await publisher.connect()

            # After connect we should have the topic ID from the server, so compute the
            # hash and save the open publisher stream
            topic_hash = hash(topic)
            self.publishers[topic_hash] = publisher

            # Run the publisher as a concurrent task which handles reconnects
            await self.pool.schedule(
                publisher.run(),
                done_callback=lambda: self.publishers.pop(topic_hash, None),
            )

        # Create a concurrent task to queue the events from the user
        await self.pool.schedule(publisher.queue_events(events))

    @catch_rpc_error
    async def subscribe(self, topics, query="", consumer_group=None):
        # TODO: Support query parameters

        # Ensure we have a gRPC channel
        self._ensure_ready()

        if self.shutdown.is_set():
            raise EnsignClientClosingError("client is closing")

        # Create a hash of the topics
        topic_hash = frozenset([t for t in topics])

        # Check if there is already an open stream for these topics
        if topic_hash in self.subscribers:
            subscriber = self.subscribers[topic_hash]
        else:
            # Create the subscribe stream for these topics
            subscriber = Subscriber(
                self,
                topics,
                query=query_pb2.Query(query=query),
                consumer_group=consumer_group,
                reconnect_tick=self.reconnect_tick,
                reconnect_timeout=self.reconnect_timeout,
            )
            self.subscribers[topic_hash] = subscriber

            # Connect to the subscribe stream
            # TODO: Distinguish between authentication errors, topic errors, and connection errors
            await subscriber.connect()

            # Run the subscriber as a concurrent task which handles reconnects
            await self.pool.schedule(
                subscriber.run(),
                done_callback=lambda: self.subscribers.pop(topic_hash, None),
            )

        # Consume the events and yield them to the caller
        async for event in subscriber.consume():
            yield event

    @catch_rpc_error
    async def en_sql(self, query, params=None):
        self._ensure_ready()
        req = query_pb2.Query(query=query, params=params)
        return await Cursor.execute(self.stub.EnSQL(req))

    @catch_rpc_error
    async def explain(self, query="", params=None):
        self._ensure_ready()
        req = query_pb2.Query(query=query, params=params)
        return await self.stub.Explain(req)

    @catch_rpc_error
    async def list_topics(self, page_size=100, next_page_token=""):
        self._ensure_ready()
        params = ensign_pb2.PageInfo(
            page_size=page_size, next_page_token=next_page_token
        )
        rep = await self.stub.ListTopics(params)
        return rep.topics, rep.next_page_token

    @catch_rpc_error
    async def create_topic(self, topic):
        self._ensure_ready()
        return await self.stub.CreateTopic(topic)

    @catch_rpc_error
    async def retrieve_topic(self, id):
        self._ensure_ready()
        topic = topic_pb2.Topic(id=id)
        return await self.stub.RetrieveTopic(topic)

    @catch_rpc_error
    async def archive_topic(self, id):
        self._ensure_ready()
        params = topic_pb2.TopicMod(
            id=id, operation=topic_pb2.TopicMod.Operation.ARCHIVE
        )
        rep = await self.stub.DeleteTopic(params)
        return rep.id, rep.state

    @catch_rpc_error
    async def destroy_topic(self, id):
        self._ensure_ready()
        params = topic_pb2.TopicMod(
            id=id, operation=topic_pb2.TopicMod.Operation.DESTROY
        )
        rep = await self.stub.DeleteTopic(params)
        return rep.id, rep.state

    @catch_rpc_error
    async def topic_names(self, page_size=100, next_page_token=""):
        self._ensure_ready()
        params = ensign_pb2.PageInfo(
            page_size=page_size, next_page_token=next_page_token
        )
        rep = await self.stub.TopicNames(params)
        return rep.topic_names, rep.next_page_token

    @catch_rpc_error
    async def topic_exists(self, topic_id=None, project_id=None, topic_name=""):
        self._ensure_ready()
        params = topic_pb2.TopicName(
            topic_id=topic_id, project_id=project_id, name=topic_name
        )
        rep = await self.stub.TopicExists(params)
        return rep.query, rep.exists

    @catch_rpc_error
    async def info(self, topics=[]):
        self._ensure_ready()
        return await self.stub.Info(ensign_pb2.InfoRequest(topics=topics))

    @catch_rpc_error
    async def status(self, attempts=0, last_checked_at=None):
        self._ensure_ready()
        params = ensign_pb2.HealthCheck(
            attempts=attempts, last_checked_at=last_checked_at
        )
        rep = await self.stub.Status(params)
        return rep.status, rep.version, rep.uptime, rep.not_before, rep.not_after

    async def close(self):
        """
        Close the connection to the server and all ongoing streams.
        """
        if self.channel:
            await self.channel.close()
        await self._close_streams()
        self.channel = None

    async def _close_streams(self):
        # Prevent new streams from being created
        self.shutdown.set()

        # Close all ongoing publishers and subscribers
        while self.publishers:
            _, publisher = self.publishers.popitem()
            await publisher.close()

        while self.subscribers:
            _, subscriber = self.subscribers.popitem()
            await subscriber.close()

        await self.pool.release()


class Cursor:
    """
    Cursor defines an abstraction for server-side event streaming using familiar
    database cursor semantics.
    """

    @classmethod
    async def execute(cls, stream):
        self = Cursor()
        self._stream = stream
        self._result = None

        # Read the first event from the stream, which might raise an exception
        self._result = await self.fetchone()
        if self._result is None:
            raise QueryNoRows("no results returned by query")
        return self

    async def __aiter__(self):
        while True:
            event = await self.fetchone()
            if event is None:
                return
            yield event

    async def fetchone(self):
        """
        Fetch the next event from the stream. Returns None if there are no more events.
        """
        return await self._read()

    async def fetchmany(self, n):
        """
        Fetch the next n events from the stream. Returns an empty list if there are no
        more events.
        """
        events = []
        for _ in range(n):
            event = await self._read()
            if event is None:
                break
            events.append(event)
        return events

    async def fetchall(self):
        """
        Fetch all events from the stream. Returns an empty list if there are no more
        events.
        """
        events = []
        while True:
            event = await self._read()
            if event is None:
                break
            events.append(event)
        return events

    def close(self):
        """
        Close the cursor and the underlying stream.
        """
        if self._stream is not None:
            self._stream.cancel()
            self._stream = None

    async def _read(self):
        """
        Read the next event on the stream, handling gRPC errors and the end of the
        stream.
        """
        if self._stream is None:
            return None

        # Return the cached result if there is one
        if self._result is not None:
            result = self._result
            self._result = None
            return result

        # Read the next event from the stream
        try:
            event = await self._stream.read()
        except grpc.aio.AioRpcError as e:
            self.close()
            if e.code() == grpc.StatusCode.CANCELLED:
                return None
            else:
                raise e
        except (asyncio.CancelledError, StopAsyncIteration):
            self.close()
            return None

        # Handle unexpected end of stream
        if event is grpc.aio.EOF:
            self.close()
            return None

        # Convert the event to the user-facing type
        return from_proto(unwrap(event))
