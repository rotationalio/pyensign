import grpc
import asyncio
from grpc import aio

from pyensign.api.v1beta1 import topic_pb2
from pyensign.api.v1beta1 import ensign_pb2
from pyensign.utils.tasks import WorkerPool
from pyensign.api.v1beta1 import ensign_pb2_grpc
from pyensign.exceptions import catch_rpc_error
from pyensign.api.v1beta1.event import wrap, unwrap
from pyensign.exceptions import EnsignError, EnsignTypeError


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

        if insecure:
            if auth is not None:
                raise ValueError("Cannot use auth with insecure=True")
            self.channel = aio.insecure_channel(addrport)
        else:
            credentials = grpc.ssl_channel_credentials()
            if auth is not None:
                call_credentials = grpc.metadata_call_credentials(
                    auth, name="auth gateway"
                )
                credentials = grpc.composite_channel_credentials(
                    credentials, call_credentials
                )
            self.channel = aio.secure_channel(addrport, credentials)


class Client:
    """
    Client defines a high level client that makes requests to an Ensign server.
    """

    def __init__(self, connection):
        """
        Create a new client from an established connection.

        Parameters
        ----------
        connection : Connection
            The connection to the Ensign server
        """

        self.channel = connection.channel
        self.stub = ensign_pb2_grpc.EnsignStub(self.channel)
        self.publish_streams = {}
        self.subscribe_streams = {}
        self.pool = WorkerPool(max_workers=10, max_queue_size=100)

    @catch_rpc_error
    async def publish(
        self, topic_id, events, ack_callback=None, nack_callback=None, client_id=""
    ):
        # Create a hash of the topic ID
        topic_hash = str(topic_id)

        # Check if there is already an open stream for this topic
        if topic_hash in self.publish_streams:
            stream = self.publish_streams[topic_hash]
        else:
            # Create the stream for this topic
            stream = Stream()
            self.publish_streams[topic_hash] = stream

            async def next_request():
                # First message must be an OpenStream request
                # TODO: Should we send topics here?
                yield ensign_pb2.PublisherRequest(
                    open_stream=ensign_pb2.OpenStream(client_id=client_id)
                )

                # The rest of the messages should be wrapped events
                while True:
                    req = await stream.read_request()
                    if req is None:
                        break
                    yield req

            async def publish_stream():
                async for rep in self.stub.Publish(next_request()):
                    rep_type = rep.WhichOneof("embed")
                    if not stream.ready.is_set() and rep_type != "ready":
                        raise EnsignTypeError(
                            "expected ready response, got {}".format(rep_type)
                        )
                    if rep_type == "ready":
                        # TODO: Parse topic map from stream_ready response
                        stream.ready.set()
                    elif rep_type == "ack":
                        if ack_callback:
                            await ack_callback(rep.ack)
                    elif rep_type == "nack":
                        if nack_callback:
                            await nack_callback(rep.nack)
                    elif rep_type == "close_stream":
                        await stream.close()
                        break
                    else:
                        raise EnsignTypeError(f"unexpected response type: {rep_type}")

            # Create a concurrent task to handle the stream
            await self.pool.schedule(
                publish_stream(),
                done_callback=lambda: self.publish_streams.pop(topic_hash),
            )

        async def queue_events():
            for event in events:
                req = ensign_pb2.PublisherRequest(event=wrap(event, topic_id))
                await stream.write_request(req)

        # Create a concurrent task to queue the events from the user
        await self.pool.schedule(queue_events())

    @catch_rpc_error
    async def subscribe(self, topic_ids, client_id="", query="", consumer_group=None):
        # Create a hash of the topic IDs
        topic_hash = frozenset([id for id in topic_ids])

        # Check if there is already an open stream for these topics
        if topic_hash in self.subscribe_streams:
            stream = self.subscribe_streams[topic_hash]
        else:
            # Create the stream for these topics
            stream = Stream()
            self.subscribe_streams[topic_hash] = stream

            async def next_request():
                # First message must be a Subscription request with the topic
                sub = ensign_pb2.Subscription(
                    client_id=client_id,
                    topics=topic_ids,
                    query=query,
                    group=consumer_group,
                )
                yield ensign_pb2.SubscribeRequest(subscription=sub)

                # Return acks and nacks to the server
                while True:
                    yield await stream.read_request()

            async def subscribe_stream():
                async for rep in self.stub.Subscribe(next_request()):
                    rep_type = rep.WhichOneof("embed")
                    if not stream.ready.is_set() and rep_type != "ready":
                        stream.write_response(
                            EnsignTypeError(
                                "expected ready response, got {}".format(rep_type)
                            )
                        )
                        break
                    if rep_type == "ready":
                        stream.ready.set()
                    elif rep_type == "event":
                        await stream.write_response(rep.event)
                    elif rep_type == "close_stream":
                        await stream.close()
                        break
                    else:
                        stream.write_response(
                            EnsignTypeError(f"unexpected response type: {rep_type}")
                        )
                        break

            # Create a concurrent task to handle the stream
            await self.pool.schedule(
                subscribe_stream(),
                done_callback=lambda: self.subscribe_streams.pop(topic_hash),
            )

        # Yield events from the stream
        while True:
            rep = await stream.read_response()
            if rep is None:
                break
            elif isinstance(rep, EnsignError):
                raise rep
            else:
                # Ack back to the stream
                # TODO: Allow the user to ack or nack the event
                await stream.write_request(ensign_pb2.SubscribeRequest(ack=ensign_pb2.Ack(id=rep.id)))

                yield unwrap(rep)

    @catch_rpc_error
    async def list_topics(self, page_size=100, next_page_token=""):
        params = ensign_pb2.PageInfo(
            page_size=page_size, next_page_token=next_page_token
        )
        rep = await self.stub.ListTopics(params)
        return rep.topics, rep.next_page_token

    @catch_rpc_error
    async def create_topic(self, topic):
        return await self.stub.CreateTopic(topic)

    @catch_rpc_error
    async def retrieve_topic(self, id):
        topic = topic_pb2.Topic(id=id)
        return await self.stub.RetrieveTopic(topic)

    @catch_rpc_error
    async def archive_topic(self, id):
        params = topic_pb2.TopicMod(
            id=id, operation=topic_pb2.TopicMod.Operation.ARCHIVE
        )
        rep = await self.stub.DeleteTopic(params)
        return rep.id, rep.state

    @catch_rpc_error
    async def destroy_topic(self, id):
        params = topic_pb2.TopicMod(
            id=id, operation=topic_pb2.TopicMod.Operation.DESTROY
        )
        rep = await self.stub.DeleteTopic(params)
        return rep.id, rep.state

    @catch_rpc_error
    async def topic_names(self, page_size=100, next_page_token=""):
        params = ensign_pb2.PageInfo(
            page_size=page_size, next_page_token=next_page_token
        )
        rep = await self.stub.TopicNames(params)
        return rep.topic_names, rep.next_page_token

    @catch_rpc_error
    async def topic_exists(self, topic_id=None, project_id=None, topic_name=""):
        params = topic_pb2.TopicName(
            topic_id=topic_id, project_id=project_id, name=topic_name
        )
        rep = await self.stub.TopicExists(params)
        return rep.query, rep.exists

    @catch_rpc_error
    async def info(self, topics=[]):
        return await self.stub.Info(ensign_pb2.InfoRequest(topics=topics))

    @catch_rpc_error
    async def status(self, attempts=0, last_checked_at=None):
        params = ensign_pb2.HealthCheck(
            attempts=attempts, last_checked_at=last_checked_at
        )
        rep = await self.stub.Status(params)
        return rep.status, rep.version, rep.uptime, rep.not_before, rep.not_after

    async def close(self):
        """
        Close the connection to the server and all ongoing streams.
        """
        await self.pool.release()
        await self.channel.close()


class Stream:
    """
    Stream implements an in-memory bidirectional buffer for non-blocking communication
    between coroutines. The stream has a ready state that can be used to signal when the
    stream is ready to receive requests.
    """

    def __init__(self, max_queue_size=100):
        self._request_queue = asyncio.Queue(maxsize=max_queue_size)
        self._response_queue = asyncio.Queue(maxsize=max_queue_size)
        self.ready = asyncio.Event()

    async def write_request(self, message):
        """
        Write a request to the stream, blocks if the queue is full.
        """
        await self._request_queue.put(message)

    async def read_request(self):
        """
        Read a request from the stream, blocks if the queue is empty.
        """
        return await self._request_queue.get()

    async def write_response(self, message):
        """
        Write a response to the stream, blocks if the queue is full.
        """
        await self._response_queue.put(message)

    async def read_response(self):
        """
        Read a response from the stream, blocks if the queue is empty.
        """
        return await self._response_queue.get()

    async def close(self):
        """
        Close the stream by sending a message to both the request and response streams.
        The main purpose of this is to unblock pending consumers. Otherwise, consumers
        will block indefinitely on read_request() or read_response(). This means that
        consumers should be prepared to handle None responses from reads.
        """
        await self._request_queue.put(None)
        await self._response_queue.put(None)
