import grpc
from grpc import aio
from ulid import ULID

from pyensign.api.v1beta1 import topic_pb2
from pyensign.api.v1beta1 import ensign_pb2
from pyensign.api.v1beta1 import ensign_pb2_grpc
from pyensign.exceptions import catch_rpc_error
from pyensign.api.v1beta1.event import wrap, unwrap
from pyensign.exceptions import EnsignTypeError


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

        self.stub = ensign_pb2_grpc.EnsignStub(connection.channel)

    @catch_rpc_error
    async def publish(self, topic_id, events, client_id=""):
        def next():
            # First message must be an OpenStream request
            # TODO: Should we send topics here?
            yield ensign_pb2.PublisherRequest(
                open_stream=ensign_pb2.OpenStream(client_id=client_id)
            )

            # The rest of the messages should be wrapped events
            for event in events:
                yield ensign_pb2.PublisherRequest(event=wrap(event, topic_id))

        ready = False
        async for rep in self.stub.Publish(next()):
            rep_type = rep.WhichOneof("embed")
            if not ready and rep_type != "ready":
                raise EnsignTypeError(
                    "expected ready response, got {}".format(rep_type)
                )
            if rep_type == "ready":
                # TODO: Parse topic map from stream_ready response
                ready = True
            elif rep_type == "ack":
                yield rep.ack
            elif rep_type == "nack":
                yield rep.nack
            elif rep_type == "close_stream":
                break
            else:
                raise EnsignTypeError(f"unexpected response type: {rep_type}")

    @catch_rpc_error
    async def subscribe(self, topic_ids, client_id="", query="", consumer_group=None):
        # First message must be a Subscription request with the topic
        sub = ensign_pb2.Subscription(
            client_id=client_id,
            topics=topic_ids,
            query=query,
            group=consumer_group,
        )
        req = ensign_pb2.SubscribeRequest(subscription=sub)

        # TODO: This is a bidirectional stream, we should be a good citizen and return
        # acks and nacks to the server.
        ready = False
        async for rep in self.stub.Subscribe(iter([req])):
            rep_type = rep.WhichOneof("embed")
            if not ready and rep_type != "ready":
                raise EnsignTypeError(
                    "expected ready response, got {}".format(rep_type)
                )
            if rep_type == "ready":
                ready = True
            elif rep_type == "event":
                yield unwrap(rep.event)
            elif rep_type == "close_stream":
                break
            else:
                raise EnsignTypeError(f"unexpected response type: {rep_type}")

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
    async def status(self, attempts=0, last_checked_at=None):
        params = ensign_pb2.HealthCheck(
            attempts=attempts, last_checked_at=last_checked_at
        )
        rep = await self.stub.Status(params)
        return rep.status, rep.version, rep.uptime, rep.not_before, rep.not_after
