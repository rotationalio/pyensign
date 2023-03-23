import grpc

from pyensign.api.v1beta1 import ensign_pb2
from pyensign.api.v1beta1 import ensign_pb2_grpc


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
            self.channel = grpc.insecure_channel(addrport)
        else:
            credentials = grpc.ssl_channel_credentials()
            if auth is not None:
                call_credentials = grpc.metadata_call_credentials(
                    auth, name="auth gateway"
                )
                credentials = grpc.composite_channel_credentials(
                    credentials, call_credentials
                )
            self.channel = grpc.secure_channel(addrport, credentials)


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

    # TODO: Error handling for gRPC errors
    def status(self, attempts=0, last_checked_at=None):
        params = ensign_pb2.HealthCheck(
            attempts=attempts, last_checked_at=last_checked_at
        )
        rep = self.stub.Status(params)
        return rep.status, rep.version, rep.uptime, rep.not_before, rep.not_after

    def topic_names(self, page_size=100, next_page_token=""):
        params = ensign_pb2.PageInfo(
            page_size=page_size, next_page_token=next_page_token
        )
        rep = self.stub.TopicNames(params)
        return rep.topic_names, rep.next_page_token

    # TODO: Handle binary data (e.g. marshalled ULIDs)
    def list_topics(self, page_size=100, next_page_token=""):
        params = ensign_pb2.PageInfo(
            page_size=page_size, next_page_token=next_page_token
        )
        rep = self.stub.ListTopics(params)
        return rep.topics, rep.next_page_token
