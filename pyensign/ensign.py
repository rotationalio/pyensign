import os
import json

from ulid import ULID

from pyensign.connection import Client
from pyensign.utils.topics import Topic, TopicCache
from pyensign.connection import Connection
from pyensign.api.v1beta1 import topic_pb2
from pyensign.auth.client import AuthClient
from pyensign.exceptions import (
    CacheMissError,
    UnknownTopicError,
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
        cred_path="",
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
        cred_path : str (optional)
            Load a JSON file containing the 'ClientID' and 'ClientSecret' key.
        endpoint : str (optional)
            The endpoint of the Ensign server.
        insecure : bool (optional)
            Set to True to use an insecure connection.
        auth_url : str (optional)
            The URL of the Ensign authentication server.
        disable_topic_cache: bool (optional)
            Set to True to disable topic ID caching.
        """

        if cred_path:
            try:
                with open(cred_path, "r") as file:
                    data = json.load(file)
                    client_id = data["ClientID"]
                    client_secret = data["ClientSecret"]
            except FileNotFoundError as e:
                raise ValueError("Credentials file not found", str(e))
            except UnicodeDecodeError as e:
                raise ValueError("Error decoding credentials file", str(e))
            except json.JSONDecodeError as e:
                raise ValueError("Credentials file has invalid JSON", str(e))
            except IOError as e:
                raise ValueError("IO error while reading credentials file:", str(e))

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

        self.topics = TopicCache(read_only=disable_topic_cache)
        self.client = Client(connection, topic_cache=self.topics)

    async def publish(
        self, topic, *events, on_ack=None, on_nack=None, ensure_exists=False
    ):
        """
        Publish events to an Ensign topic. This function is asynchronous; it publishes
        events to an outgoing queue and returns immediately. Publishers can configure
        the `on_ack` and `on_nack` callbacks to implement per-event success or failure
        handling. Alternatively, publishers can check the status of events at any time
        using the `acked()` and `nacked()` methods on the events themselves.

        Parameters
        ----------
        topic: str
            The name or ID of the topic to publish events to.

        events : iterable of events.Event objects
            The events to publish.

        on_ack: coroutine (optional)
            A coroutine to be invoked when an ACK message is received from Ensign,
            indicating that an event was successfully published. The first argument of
            the coroutine is an Ack object with a `committed` timestamp that indicates
            when the event was committed.

        on_nack: coroutine (optional)
            A coroutine to be invoked when a NACK message is received from Ensign,
            indicating that the event could not be published. The first argument of the
            coroutine is a Nack object with a `code` field that indicates the reason
            why the event was not published.

        ensure_exists: bool (optional)
            Create the topic if it does not exist. With this option, a topic name must
            be provided, not an ID.

        Raises
        ------
        ValueError
            If a topic or no events are provided.

        TypeError
            If the topic is not a string.

        UnknownTopicError
            If the topic does not exist and ensure_exists is False.
        """

        if topic == "":
            raise ValueError("topic is required")

        if not isinstance(topic, str):
            raise TypeError(
                "expected type 'str' for topic, got '{}'".format(type(topic).__name__)
            )

        # Handle a list of events provided as a single argument
        if len(events) == 1 and (
            isinstance(events[0], list) or isinstance(events[0], tuple)
        ):
            events = events[0]

        if len(events) == 0:
            raise ValueError("no events provided")

        if ensure_exists:
            # Get or create the topic if it doesn't exist
            id = ULID.from_str(await self.ensure_topic_exists(topic))
            topic = Topic(id=id, name=topic)
        else:
            try:
                # Try to resolve the topic ID from the string
                topic = Topic(id=self.topics.resolve(topic))
            except CacheMissError:
                # Assume the topic string is a name, the publisher will make a best
                # effort to resolve the ID from the name using the response from the
                # server
                topic = Topic(name=topic)

        # TODO: Support user-defined generators
        async def next():
            for event in events:
                yield event

        await self.client.publish(
            topic,
            next(),
            on_ack=on_ack,
            on_nack=on_nack,
        )

    async def subscribe(self, *topics, query="", consumer_group=None):
        """
        Subscribe to events from the Ensign server. This method returns an async
        generator that yields Event objects, so the `async for` syntax can be used to
        process events as they are received.

        Parameters
        ----------
        topics : iterable of str
            The topic names or IDs to subscribe to.

        query : str (optional)
            EnSQL query to filter events.

        consumer_group : api.v1beta1.groups.ConsumerGroup (optional)
            The consumer group to use for the subscriber.

        Raises
        ------
        ValueError
            If no topics are provided.

        UnknownTopicError
            If a topic is provided that does not exist.

        Yields
        ------
        events.Event
            The event objects. Subscribers should call ack() or nack() on each event
            object to signal to the server if the event was successfully processed or
            it needs to be redelivered to another subscriber in the consumer group.
        """

        # Handle a list of topics passed as a single argument
        if len(topics) == 1 and (
            isinstance(topics[0], list) or isinstance(topics[0], tuple)
        ):
            topics = topics[0]

        if len(topics) == 0:
            raise ValueError("no topics provided")

        # Parse topic names into ID strings
        topic_ids = []
        for topic in topics:
            if not isinstance(topic, str):
                raise TypeError(
                    "expected type 'str' for topic, got {}".format(type(topic))
                )

            # Attempt to lookup or parse the ID
            try:
                topic_ids.append(str(self.topics.resolve(topic)))
            except CacheMissError:
                # Ignore unknown names for now, the server will decide if they are
                # valid
                topic_ids.append(topic)

        # Yield events from the subscribe stream
        async for event in self.client.subscribe(
            topic_ids,
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
        topic_name : str
            The name for the new topic, must be unique to the project.

        Returns
        -------
        str
            The ID of the topic that was created.
        """

        created = await self.client.create_topic(topic_pb2.Topic(name=topic_name))
        if not created:
            # TODO: Return more specific errors
            raise EnsignTopicCreateError("topic creation failed")
        id = ULID(created.id)
        if self.topics:
            self.topics.add(created.name, id)
        return str(id)

    async def ensure_topic_exists(self, topic_name):
        """
        Check if a topic exists and create it if it does not. This is a shortcut for
        topic_exists and create_topic but also returns the topic ID.

        Parameters
        ----------
        topic_name : str
            The name of the topic to check.

        Returns
        -------
        str
            The ID of the topic.
        """

        if await self.topic_exists(topic_name):
            return await self.topic_id(topic_name)
        else:
            return await self.create_topic(topic_name)

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
                return str(self.topics.get(name))
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
                    id = ULID(topic.id)
                    if self.topics is not None:
                        self.topics.add(name, id)
                    return str(id)
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

    def _resolve_topic(self, topic):
        """
        Resolve a topic string into a ULID by looking it up in the cache, otherwise
        assume it's a topic ID and try to parse it.
        """

        # Attempt to retrieve the topic ID from the cache
        if self.topics is not None:
            try:
                return self.topics.get(topic)
            except CacheMissError:
                pass

        # Otherwise attempt to parse as a ULID
        try:
            return ULID.from_str(topic)
        except ValueError:
            # TODO: Might need to return the name of the project here
            raise UnknownTopicError(
                f"unknown topic '{topic}', please provide the name or ID of a topic in your project"
            )
