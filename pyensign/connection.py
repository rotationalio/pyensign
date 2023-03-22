import grpc

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

    # TODO: Error handling for gRPC errors
    def status(self, attempts=0, last_checked_at=None):
        params = ensign_pb2.HealthCheck(
            attempts=attempts, last_checked_at=last_checked_at
        )
        rep = self.stub.Status(params)
        return rep.status, rep.version, rep.uptime, rep.not_before, rep.not_after
