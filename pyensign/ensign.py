import os
import json
import inspect

from ulid import ULID

from pyensign.connection import Client
from pyensign.events import from_object
from pyensign.status import ServerStatus
from pyensign.utils.topics import Topic, TopicCache
from pyensign.connection import Connection
from pyensign.api.v1beta1 import topic_pb2, query_pb2
from pyensign.auth.client import AuthClient
from pyensign.exceptions import (
    CacheMissError,
    UnknownTopicError,
    InvalidQueryError,
    EnsignInvalidArgument,
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

    async def query(self, query, params=None):
        """
        Execute an EnSQL query. This method returns a cursor that can be used to fetch
        the results of the query, via `fetchone()`, `fetchmany()`, or `fetchall()`
        methods.

        Parameters
        ----------
        query : str
            A valid EnSQL query. EnSQL queries are similar to SQL queries on relational
            databases, but instead of returning rows in tables they return events in
            topic streams.

            For example, the following query returns the first 10 events in a topic:

            SELECT * FROM <topic> LIMIT 10

            See https://ensign.rotational.dev/ensql for more information on the EnSQL syntax.

        params : dict (optional)
            Parameters to be substituted into the query. Only primitive types are
            supported, i.e. strings, integers, floats, and booleans.

        Returns
        -------
        connection.Cursor
            The cursor object that can be used to fetch the results of the query.

        Raises
        ------
        InvalidQueryError
            If the provided query is invalid.

        QueryNoRows
            If the query returned no results.

        TypeError
            If an unsupported parameter type is provided.
        """

        # Parse the args into the protobuf parameters
        parameters = []
        if params:
            for name, value in params.items():
                if isinstance(value, int):
                    parameters.append(query_pb2.Parameter(name=name, i=value))
                elif isinstance(value, float):
                    parameters.append(query_pb2.Parameter(name=name, d=value))
                elif isinstance(value, bool):
                    parameters.append(query_pb2.Parameter(name=name, b=value))
                elif isinstance(value, bytes):
                    parameters.append(query_pb2.Parameter(name=name, y=value))
                elif isinstance(value, str):
                    parameters.append(query_pb2.Parameter(name=name, s=value))
                else:
                    raise TypeError(
                        "unsupported parameter type: {}".format(type(value).__name__)
                    )

        # Execute the query and return the cursor to the user
        try:
            cursor = await self.client.en_sql(query, params=parameters)
        except EnsignInvalidArgument as e:
            raise InvalidQueryError(e.details)

        return cursor

    async def explain_query(self, query, params):
        raise NotImplementedError

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
        ServerStatus:
            Status of the server.
        """

        status, version, uptime, _, _ = await self.client.status()
        return ServerStatus(status, version, uptime)

    async def close(self):
        """
        Close the Ensign client.
        """
        await self.client.close()

    async def __aenter__(self):
        await self.client.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

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


##########################################################################
## Decorators
##########################################################################

# Global Ensign client. This is not thread-safe but it's safe to use concurrently in
# asyncio coroutines.
_client = None


def authenticate(*auth_args, **auth_kwargs):
    """
    Decorator function to authenticate with Ensign. This function ideally should only
    be called once, usually at the entry point in the application. Duplicate calls to
    the decorator are ignored.

    By default credentials are loaded from the ENSIGN_CLIENT_ID and ENSIGN_CLIENT_SECRET
    environment variables. Alternatively, credentials can be provided as keyword
    arguments or a JSON file path, see the Ensign class for more details.

    Usage
    -----
    ```
    # Use credentials from the environment
    @authenticate()
    async def main():
        ...

    # Use credentials from kwargs
    @authenticate(client_id="...", client_secret="...")
    async def main():
        ...

    # Use credentials from a JSON file
    @authenticate(cred_path="...")
    async def main():
        ...
    ```
    """

    def wrap_coroutine(coro):
        async def wrapper(*args, **kwargs):
            global _client
            if _client is not None:
                return await coro(*args, **kwargs)

            # Set the global client for the duration of the coroutine
            try:
                async with Ensign(*auth_args, **auth_kwargs) as client:
                    _client = client
                    res = await coro(*args, **kwargs)
            except Exception as e:
                _client = None
                raise e

            _client = None
            return res

        return wrapper

    def wrap_async_generator(coro):
        async def wrapper(*args, **kwargs):
            global _client
            if _client is not None:
                async for res in coro(*args, **kwargs):
                    yield res

            # Set the global client for the duration of the generator
            try:
                async with Ensign(*auth_args, **auth_kwargs) as client:
                    _client = client
                    async for res in coro(*args, **kwargs):
                        yield res
            except Exception as e:
                _client = None
                raise e

            _client = None

        return wrapper

    # Return either a coroutine or an async generator wrapper to match the marked
    # function
    def decorator(coro):
        if inspect.iscoroutinefunction(coro):
            return wrap_coroutine(coro)
        elif inspect.isasyncgenfunction(coro):
            return wrap_async_generator(coro)
        else:
            raise TypeError(
                "decorated function must be a coroutine or async generator, got {}".format(
                    type(coro)
                )
            )

    return decorator


def publish(topic, mimetype=None, encoder=None):
    """
    Decorator to mark a publish function. The return value of the function will be
    published as an event to the topic. If the function is a generator, the events are
    published as they are yielded from the generator. This method assumes that you have
    already authenticated with Ensign using the `@authenticate` decorator.

    Parameters
    ----------
    topic: str
        The name or ID of the topic to publish events to.

    mimetype: str or int (optional)
        The mimetype indicating how the return value will be encoded. If not provided,
        the mimetype is inferred from the return value of the function.

    encoder: events.BytesEncoder (optional)
        The encoder to use to encode the return value into bytes for the event payload.
        If not provided, a best effort is made to encode the value, for most data types
        this will be JSON.

    Usage
    -----
    ```
    # Publish a JSON event to the topic 'my-topic'
    @publish("my-topic")
    async def my_function():
        return {"foo": "bar"}

    # Publish multiple events using a generator
    @publish("my-topic")
    async def my_generator():
        for i in range(3):
            yield {"event_id": i}
    ```
    """

    def wrap_coroutine(coro):
        async def wrapper(*args, **kwargs):
            # Create the Ensign client if not already created
            global _client
            if _client is None:
                raise RuntimeError(
                    "publish requires a connection to Ensign, please use the authenticate decorator to provide your credentials"
                )

            # Call the function and get the return value
            val = await coro(*args, **kwargs)
            if val is None:
                return val

            # Create the event to publish
            event = from_object(val, mimetype=mimetype, encoder=encoder)

            # Publish the event
            await _client.publish(topic, event)

            return val

        return wrapper

    def wrap_async_generator(coro):
        async def wrapper(*args, **kwargs):
            # Create the Ensign client if not already created
            global _client
            if _client is None:
                raise RuntimeError(
                    "publish requires a connection to Ensign, please use the authenticate decorator to provide your credentials"
                )

            # Yield the values from the generator
            async for val in coro(*args, **kwargs):
                if val is None:
                    yield val
                    continue

                # Create the event to publish
                event = from_object(val, mimetype=mimetype, encoder=encoder)

                # Publish the event
                await _client.publish(topic, event)

                yield val

        return wrapper

    # Return either a coroutine or an async generator wrapper to match the marked
    # function
    def decorator(coro):
        if inspect.iscoroutinefunction(coro):
            return wrap_coroutine(coro)
        elif inspect.isasyncgenfunction(coro):
            return wrap_async_generator(coro)
        else:
            raise TypeError(
                "decorated function must be a coroutine or async generator, got {}".format(
                    type(coro)
                )
            )

    return decorator
