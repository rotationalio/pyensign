import os
from ulid import ULID

from pyensign.connection import Client
from pyensign.utils.cache import Cache
from pyensign.connection import Connection
from pyensign.api.v1beta1 import topic_pb2
from pyensign.api.v1beta1 import ensign_pb2
from pyensign.auth.client import AuthClient
from pyensign.exceptions import (
    CacheMissError,
    EnsignTypeError,
    EnsignTopicCreateError,
    EnsignTopicNotFoundError,
)


class Ensign:
    """
    Ensign connects to an Ensign server and provides access to the Ensign API.
    """

    def __init__(
        self,
        client_id="",
        client_secret="",
        endpoint="ensign.rotational.app:443",
        insecure=False,
        auth_url="https://auth.rotational.app",
        disable_topic_cache=False,
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
        disable_topic_cache: bool (optional)
            Set to True to disable topic ID caching.
        """

        if not client_id or client_id == "":
            client_id = os.environ.get("ENSIGN_CLIENT_ID")
        if not client_secret or client_secret == "":
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

        if disable_topic_cache:
            self.topics = None
        else:
            self.topics = Cache()

    async def publish(
        self, topic_name, *events, ack_callback=None, nack_callback=None, client_id=""
    ):
        """
        Publish events to an Ensign topic.

        Parameters
        ----------
        topic_name : str
            The name of the topic to publish events to.

        events : iterable of events
            The events to publish.

        ack_callback: callable (optional)
            A callback to be invoked when an ACK message is received from Ensign,
            indicating that an event was successfully published.

        nack_callback: callable (optional)
            A callback to be invoked when a NACK message is received from Ensign,
            indicating that the event could not be published.

        client_id : str (optional)
            The client ID to use for the publisher. If not provided, a new client ID is
            generated.
        """

        if topic_name == "":
            raise ValueError("topic_name is required")

        if len(events) == 0:
            raise ValueError("no events provided")

        if client_id == "":
            client_id = str(ULID())

        # Ensure the topic ID exists by getting or creating it
        topic_id = None
        try:
            id_str = await self.topic_id(topic_name)
            topic_id = ULID.from_str(id_str)
        except EnsignTopicNotFoundError:
            topic = await self.create_topic(topic_name)
            topic_id = ULID.from_bytes(topic.id)

        # TODO: Support user-defined generators
        def next():
            for event in events:
                yield event.proto()

        await self.client.publish(
            topic_id,
            next(),
            ack_callback=ack_callback,
            nack_callback=nack_callback,
            client_id=client_id,
        )

    async def subscribe(self, *topic_ids, client_id="", query="", consumer_group=None):
        """
        Subscribe to events from the Ensign server.

        Parameters
        ----------
        topic_ids : iterable of str
            The topic IDs to subscribe to.

        client_id : str (optional)
            The client ID to use for the subscriber. If not provided, a new client ID
            is generated.

        query : str (optional)
            EnSQL query to filter events.

        consumer_group : api.v1beta1.groups.ConsumerGroup (optional)
            The consumer group to use for the subscriber.

        Yields
        ------
        api.v1beta1.event_pb2.Event
            The events received from the Ensign server.
        """

        if len(topic_ids) == 0:
            raise ValueError("no topic IDs provided")

        if client_id == "":
            client_id = str(ULID())

        async for event in self.client.subscribe(
            topic_ids,
            client_id=client_id,
            query=query,
            consumer_group=consumer_group,
        ):
            yield event

    async def get_topics(self):
        """
        Get all topics.

        Yields
        ------
        api.v1beta1.topic_pb2.Topic
            The topics.
        """

        topics = []
        page = None
        token = ""
        while page is None or token != "":
            page, token = await self.client.list_topics(next_page_token=token)
            topics.extend(page)
        return topics

    async def create_topic(self, topic_name):
        """
        Create a topic.

        Parameters
        ----------
        topic_name : api.v1beta1.topic_pb2.Topic
            The name for the new topic, must be unique.

        Returns
        -------
        api.v1beta1.topic_pb2.Topic
            The topic that was created.
        """

        created = await self.client.create_topic(topic_pb2.Topic(name=topic_name))
        if not created:
            raise EnsignTopicCreateError("topic creation failed")
        return created

    async def retrieve_topic(self, id):
        raise NotImplementedError

    async def archive_topic(self, id):
        raise NotImplementedError

    async def destroy_topic(self, id):
        """
        Completely destroy a topic including all of its data. This operation is
        asynchronous so it may take some time for the topic to be destroyed.

        Caution! This operation is irreversible.

        Parameters
        ----------
        id : str
            The ID of the topic to destroy.

        Returns
        -------
        bool
            True if the request was accepted, False otherwise.
        """

        _, state = await self.client.destroy_topic(id)
        return state == topic_pb2.TopicTombstone.Status.DELETING

    async def topic_names(self):
        raise NotImplementedError

    async def topic_id(self, name):
        """
        Get the ID of a topic by name.

        Parameters
        ----------
        name: str
            The name of the topic.

        Returns
        -------
        str
            The ID of the topic.

        Raises
        ------
        EnsignTopicNotFoundError
        """

        # Attempt to get the topic ID from the cache
        if self.topics is not None:
            try:
                return self.topics.get(name)
            except CacheMissError:
                pass

        # Get the topic ID from Ensign
        page = None
        token = ""
        while page is None or token != "":
            # TODO: We could use topic_names but it currently requires hashing the
            # topic name with murmur3.
            page, token = await self.client.list_topics(next_page_token=token)
            for topic in page:
                if topic.name == name:
                    id = str(ULID(topic.id))
                    if self.topics is not None:
                        self.topics.add(name, id)
                    return id
        raise EnsignTopicNotFoundError(f"topic not found by name: {name}")

    async def topic_exists(self, name):
        """
        Check if a topic exists by name.

        Parameters
        ----------
        name : str
            The name of the topic to check.

        Returns
        -------
        bool
            True if the topic exists, False otherwise.
        """

        # Attempt to check existence using the cache
        if self.topics is not None and self.topics.exists(name):
            return True

        # Check existence using Ensign
        _, exists = await self.client.topic_exists(topic_name=name)
        return exists

    async def info(self, topic_ids=[]):
        """
        Get aggregated statistics for topics in the project.

        Parameters
        ----------
        topic_ids: list of str (optional)
            If provided, only aggregate info for the specified topic IDs. Otherwise,
            the info includes all of the topics in the project.

        Returns
        -------
        api.v1beta1.ProjectInfo
            The aggregated statistics for the topics in the project.
        """

        if not isinstance(topic_ids, list):
            raise TypeError(f"expected list of topic IDs, got {type(topic_ids)}")

        # Ensure that only topic IDs are provided
        topics = []
        for id in topic_ids:
            try:
                topics.append(ULID.from_str(id).bytes)
            except ValueError:
                raise ValueError(f"not parseable as a topic ID: {id}")

        return await self.client.info(topics)

    async def status(self):
        """
        Check the status of the Ensign server.

        Returns
        -------
        str
            Status of the server.
        """

        status, version, uptime, _, _ = await self.client.status()
        return "status: {}\nversion: {}\nuptime: {}".format(status, version, uptime)
