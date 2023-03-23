import grpc

from pyensign.api.v1beta1 import topic_pb2
from pyensign.api.v1beta1 import ensign_pb2
from pyensign.api.v1beta1 import ensign_pb2_grpc


class Connection:
    """
    Connection defines a gRPC connection to an Ensign server.
    """

    def __init__(self, addrport, auth=None):
        """
        Connect to an Ensign server.

        Parameters
        ----------
        addrport : str
            The address:port of the Ensign server (e.g. "localhost:5356")
        auth : AuthClient (optional)
            Authentication client for obtaining JWT tokens
        """

        addrParts = addrport.split(":", 2)
        if len(addrParts) != 2 or not addrParts[0] or not addrParts[1]:
            raise ValueError("Invalid address:port format")

        # TODO: If authentication is specified, create a channel with per-request creds
        if auth is not None:
            raise NotImplementedError("Authentication not yet implemented")

        self.channel = grpc.insecure_channel(addrport)


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

    # TODO: Publish should not block
    def publish(self, events):
        for rep in self.stub.Publish(events):
            yield rep

    # TODO: Subscribe should not block
    def subscribe(self, topics, consumer_id="", consumer_group=None):
        open_stream = ensign_pb2.OpenStream(
            topics=topics, consumer_id=consumer_id, group=consumer_group
        )
        subscription = ensign_pb2.Subscription(open_stream=open_stream)
        for event in self.stub.Subscribe(iter([subscription])):
            yield event

    def list_topics(self, page_size=100, next_page_token=""):
        params = ensign_pb2.PageInfo(
            page_size=page_size, next_page_token=next_page_token
        )
        rep = self.stub.ListTopics(params)
        return rep.topics, rep.next_page_token

    def create_topic(self, topic):
        return self.stub.CreateTopic(topic)

    def retrieve_topic(self, id):
        topic = topic_pb2.Topic(id=id)
        return self.stub.RetrieveTopic(topic)

    def archive_topic(self, id):
        params = topic_pb2.TopicMod(
            id=id, operation=topic_pb2.TopicMod.Operation.ARCHIVE
        )
        rep = self.stub.DeleteTopic(params)
        return rep.id, rep.state

    def destroy_topic(self, id):
        params = topic_pb2.TopicMod(
            id=id, operation=topic_pb2.TopicMod.Operation.DESTROY
        )
        rep = self.stub.DeleteTopic(params)
        return rep.id, rep.state

    def topic_names(self, page_size=100, next_page_token=""):
        params = ensign_pb2.PageInfo(
            page_size=page_size, next_page_token=next_page_token
        )
        rep = self.stub.TopicNames(params)
        return rep.topic_names, rep.next_page_token

    def topic_exists(self, topic_id, project_id, topic_name):
        params = topic_pb2.TopicName(
            topic_id=topic_id, project_id=project_id, name=topic_name
        )
        rep = self.stub.TopicExists(params)
        return rep.query, rep.exists

    # TODO: Error handling for gRPC errors
    def status(self, attempts=0, last_checked_at=None):
        params = ensign_pb2.HealthCheck(
            attempts=attempts, last_checked_at=last_checked_at
        )
        rep = self.stub.Status(params)
        return rep.status, rep.version, rep.uptime, rep.not_before, rep.not_after
