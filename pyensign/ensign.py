import os

from pyensign.connection import Client
from pyensign.connection import Connection
from pyensign.auth.client import AuthClient
from pyensign.exceptions import EnsignResponseType


class Ensign:
    """
    Ensign connects to an Ensign server and provides access to the Ensign API.
    """

    def __init__(
        self,
        client_id=None,
        client_secret=None,
        endpoint="ensign.rotational.app:443",
        insecure=False,
        auth_url="https://auth.rotational.app",
    ):
        """
        Create a new Ensign client with API credentials.

        Parameters
        ----------
        client_id : str
            The client ID part of the API key. If not provided, the client ID is loaded
            from the ENSIGN_CLIENT_ID environment variable.
        client_secret : str
            The client secret part of the API key. If not provided, the client secret
            is loaded from the ENSIGN_CLIENT_SECRET environment variable.
        endpoint : str (optional)
            The endpoint of the Ensign server.
        insecure : bool (optional)
            Set to True to use an insecure connection.
        auth_url : str (optional)
            The URL of the Ensign authentication server.
        """

        if client_id is None:
            client_id = os.environ.get("ENSIGN_CLIENT_ID")
        if client_secret is None:
            client_secret = os.environ.get("ENSIGN_CLIENT_SECRET")
        if client_id is None:
            raise ValueError(
                "client_id is required but not provided or set in the ENSIGN_CLIENT_ID environment variable"
            )
        if client_secret is None:
            raise ValueError(
                "client_secret is required but not provided or set in the ENSIGN_CLIENT_SECRET environment variable"
            )
        if not isinstance(client_id, str):
            raise TypeError("client_id must be a string")
        if not isinstance(client_secret, str):
            raise TypeError("client_secret must be a string")

        creds = {"client_id": client_id, "client_secret": client_secret}
        auth = AuthClient(auth_url, creds)
        connection = Connection(addrport=endpoint, insecure=insecure, auth=auth)
        self.client = Client(connection)

    def publish(self, events):
        """
        Publish events to the Ensign server.

        Parameters
        ----------
        events : iterable of api.v1beta1.event_pb2.Event
            The events to publish.
        """

        errors = []
        for publication in self.client.publish(events):
            rep_type = publication.WhichOneof("embed")
            if rep_type == "ack":
                continue
            elif rep_type == "nack":
                errors.append(publication.nack)
            elif rep_type == "close_stream":
                break
            else:
                raise EnsignResponseType(f"unexpected response type: {rep_type}")
        return errors

    def subscribe(self, topics):
        """
        Subscribe to events from the Ensign server.

        Parameters
        ----------
        topics : iterable of api.v1beta1.topic_pb2.Topic
            The topics to subscribe to.

        Yields
        ------
        api.v1beta1.event_pb2.Event
            The events received from the Ensign server.
        """

        for event in self.client.subscribe(topics):
            yield event
