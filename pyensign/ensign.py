import os
import json
import inspect
from datetime import timedelta
from typing import (
    List,
    Optional,
    Union,
    AsyncGenerator,
    Dict,
    Any,
    Coroutine,
    Callable,
)

from ulid import ULID
from pyensign.topics import Topic
from pyensign.projects import Project
from pyensign.connection import Client
from pyensign.events import from_object
from pyensign.status import ServerStatus
from pyensign.api.v1beta1.query import format_query
from pyensign.utils.topics import TopicCache
from pyensign.connection import Connection
from pyensign.api.v1beta1 import topic_pb2
from pyensign.auth.client import AuthClient
from pyensign.sync import sync_to_async, async_to_sync
from pyensign.enum import (
    TopicState,
    DeduplicationStrategy,
    OffsetPosition,
    ShardingStrategy,
)
from pyensign.exceptions import (
    CacheMissError,
    UnknownTopicError,
    InvalidQueryError,
    EnsignInvalidArgument,
    EnsignTopicCreateError,
    EnsignTopicDestroyError,
    EnsignTopicNotFoundError,
)


class Ensign:
    """
    Ensign connects to an Ensign server and provides access to the Ensign API.
    """

    def __init__(
        self,
        client_id: str = "",
        client_secret: str = "",
        cred_path: str = "",
        endpoint: str = "ensign.rotational.app:443",
        insecure: bool = False,
        auth_url: str = "https://auth.rotational.app",
        disable_topic_cache: bool = False,
    ) -> None:
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
        self,
        topic: str,
        *events: Any,
        on_ack: Optional[Coroutine] = None,
        on_nack: Optional[Coroutine] = None,
        ensure_exists: bool = False,
    ) -> None:
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

    async def subscribe(
        self,
        *topics: str,
        query: str = "",
        params: Optional[Dict[str, Any]] = None,
        consumer_group: Optional[Any] = None,
    ) -> AsyncGenerator[Any, None]:
        """
        Subscribe to realtime events from a set of Ensign topics. This method returns
        an async generator that yields Event objects, so the `async for` syntax can be
        used to process events as they are published. To retrieve events published
        before now, use the `query()` method instead.

        Parameters
        ----------
        topics : iterable of str
            The topic names or IDs to subscribe to.

        query : str (optional)
            EnSQL query to filter events.

        params : dict (optional)
            If a query is provided, optional parameters to be substituted into the
            query. Only primitive types are supported, i.e. strings, integers, floats,
            and booleans.

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

        # Parse the query into the protobuf message
        if query != "":
            query = format_query(query, params)
        else:
            query = None

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

    async def query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        include_duplicates: Optional[bool] = False,
    ) -> Any:
        """
        Execute an EnSQL query to retrieve events from the topic no matter when they
        were published. This method returns a cursor that can be used to fetch the
        results of the query, via `fetchone()`, `fetchmany()`, or `fetchall()` methods.
        Alternatively, `async for` syntax can be used to iterate over the results.

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

        include_duplicates : bool (optional)
            If set the query results will include events that were marked as duplicates
            via the topic deduplication policy. This flag will increase query processing
            time because duplicates have to be dereferenced from the database, but this
            flag may be useful in limited queries to debug your deduplication policy.

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

        # Execute the query and return the cursor to the user
        try:
            cursor = await self.client.en_sql(
                format_query(query, params, include_duplicates)
            )
        except EnsignInvalidArgument as e:
            raise InvalidQueryError(e.details)

        return cursor

    async def explain_query(self, query, params, include_duplicates=False):
        raise NotImplementedError

    async def get_topics(self) -> List[Any]:
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

    async def create_topic(self, topic_name: str) -> str:
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

    async def ensure_topic_exists(self, topic_name: str) -> str:
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

    async def destroy_topic(self, topic: str):
        """
        Completely destroy a topic including all of its data. This operation is
        asynchronous so it may take some time for the topic to be destroyed.

        Caution! This operation is irreversible.

        Parameters
        ----------
        topic : str
            The name or ID of the topic to destroy.

        Raises
        ------
        EnsignTopicNotFoundError
            If the topic was not found.

        EnsignTopicDestroyError
            If the topic could not be destroyed.
        """

        # Resolve the topic ID from the string
        try:
            id = self.topics.resolve(topic)
        except CacheMissError:
            # If the ID is not in the cache, look it up from the server
            id = await self.topic_id(topic)

        _, state = await self.client.destroy_topic(id)
        if state != TopicState.DELETING:
            raise EnsignTopicDestroyError(
                "failed to destroy topic, topic state is {}".format(state)
            )

    async def topic_names(self):
        raise NotImplementedError

    async def topic_id(self, name: str) -> str:
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

    async def topic_exists(self, name: str) -> bool:
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

    async def set_topic_deduplication_policy(
        self,
        id: str,
        strategy: Union[DeduplicationStrategy, str],
        offset: Union[OffsetPosition, str] = OffsetPosition.OFFSET_EARLIEST,
        keys: Optional[List[str]] = None,
        fields: Optional[List[str]] = None,
        overwrite_duplicate: Optional[bool] = False,
    ) -> TopicState:
        """
        Change the deduplication policy of a topic.

        Deduplication detects events that are duplicates of previously published events
        and marks them as such, omitting them from query results and any online
        subscribers. Deduplication can significantly reduce storage costs as well as
        prevent unnecessary processing.

        Parameters
        ----------
        id : str
            The ID of the topic to set the deduplication policy for.

        strategy : DeduplicationStrategy or str
            The deduplication strategy to set as policy. See DeduplicationStrategy for
            the various options and more information.

        offset : OffsetPosition or str (default: "earliest")
            The offset position policy. See OffsetPosition for options and more info.

        keys : list (optional, depending on strategy)
            A list of strings to evaluate in the metadata for the key grouped and
            unique keys deduplication policies.

        fields : list (optional, depending on strategy)
            A list of strings to evaluate in the data for the unique fields policy.

        overwrite_duplicate : bool (optional, default: False)
            When set on the policy, duplicates are completely overwitten and cannot be
            recovered by changing the topic policy. There is still a duplicate
            placeholder in the stream but the original data is not retrievable. Set this
            on your policy only if you will never change your policy and want to take
            advantage of storage space savings.

        Returns
        -------
        state : TopicState
            The state of the topic, if READY, the deduplication policy was already set
            on the topic, if PENDING or REPAIRING, then the policy is being applied.
        """
        if isinstance(strategy, str):
            strategy = DeduplicationStrategy.parse(strategy)

        if isinstance(offset, str):
            offset = OffsetPosition.parse(offset)

        state = await self.client.set_topic_deduplication_policy(
            id, strategy, offset, keys, fields, overwrite_duplicate
        )
        return TopicState.convert(state.state)

    async def set_topic_sharding_strategy(
        self, id: str, strategy: str = "no_sharding"
    ) -> TopicState:
        """
        Change the sharding strategy of a topic.

        The sharding strategy determines how Ensign nodes will distribute events between
        themselves in a multi-node context, which can increase performance or ensure
        that event processing happens in parallel.

        WARNING: sharding is only available on multi-node topics. If the topic has not
        been allocated as a multi-node topic than an exception will be raised.

        Parameters
        ----------
        id : str
            The ID of the topic to change the sharding strategy for.

        strategy : ShardingStrategy or str
            The strategy to update the topic with. See ShardingStrategy for more info.

        Returns
        -------
        state : TopicState
            The state of the topic, if READY, the sharding strategy was already set on
            the topic, if PENDING or ALLOCATING, then the sharding strategy is being
            applied to the topic.
        """
        if isinstance(strategy, str):
            strategy = ShardingStrategy.parse(strategy)

        state = await self.client.set_topic_sharding_strategy(id, strategy)
        return TopicState.convert(state.state)

    async def info(self, topic_ids: List[str] = []) -> Any:
        """
        Get information about the project and topics in the project.

        Parameters
        ----------
        topic_ids: list of str (optional)
            If provided, only aggregate info for the specified topic IDs. Otherwise,
            the info includes all of the topics in the project.

        Returns
        -------
        projects.Project
            The project info.
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

        return Project.from_info(await self.client.info(topics))

    async def status(self) -> ServerStatus:
        """
        Check the status of the Ensign server.

        Returns
        -------
        ServerStatus:
            Status of the server.
        """

        status, version, uptime, _, _ = await self.client.status()
        return ServerStatus(status, version, uptime)

    async def flush(self, timeout=2.0):
        """
        Flush all pending events to and from the server. This method blocks until
        either all pending events have been published or subscribed or the timeout is
        reached. An exception is raised if the timeout is reached before all events
        have been flushed.

        Parameters
        ----------
        timeout: float (optional) (default: 2.0)
            Specify the timeout in seconds.

        Raises
        ------
        asyncio.TimeoutError
            If the timeout is reached before all events have been flushed.
        """

        if timeout <= 0:
            raise ValueError("timeout must be greater than 0")

        await self.client.flush(timeout=timedelta(seconds=timeout))

    async def close(self):
        """
        Close the Ensign client. This method blocks until all pending events have been
        flushed and should be called before exiting the application. After the client
        is closed it cannot be used. The `flush()` method should be used instead if the
        client still needs to be used.
        """

        await self.client.close()

    async def __aenter__(self):
        await self.client.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    def _resolve_topic(self, topic: str) -> ULID:
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


def authenticate(
    *auth_args: Any, **auth_kwargs: Any
) -> Callable[
    [Union[Coroutine[Any, Any, Any], AsyncGenerator[Any, None]]],
    Union[Coroutine[Any, Any, Any], AsyncGenerator[Any, None]],
]:
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

    def decorator(
        fn: Union[Coroutine[Any, Any, Any], AsyncGenerator[Any, None]]
    ) -> Union[Coroutine[Any, Any, Any], AsyncGenerator[Any, None]]:
        """
        The function can be a regular function or a generator function, and it can also
        be synchronous or async. This decorator will handle all of those cases by
        returning the appropriate wrapper function.
        """
        if inspect.isgeneratorfunction(fn):
            # If we have a generator function, make it async. Also wrap the resulting
            # generator in a sync wrapper so that it can be called from sync code.
            return async_to_sync(wrap_async_generator(sync_to_async(fn)))
        elif inspect.isasyncgenfunction(fn):
            # If we have an async generator function we can wrap it in an async
            # generator.
            return wrap_async_generator(fn)
        elif inspect.iscoroutinefunction(fn):
            # If we have a coroutine function we can just await it in the wrapper.
            return wrap_coroutine(fn)
        elif inspect.isfunction(fn):
            # If we have a regular function, make it async. Also wrap the resulting
            # coroutine in a sync wrapper so that it can be called from sync code.
            return async_to_sync(wrap_coroutine(sync_to_async(fn)))
        else:
            raise TypeError(
                "expected a function or generator function, got {}".format(type(fn))
            )

    return decorator


def publisher(
    topic: str,
    mimetype: Optional[str] = None,
    encoder: Optional[Callable[[Any], Any]] = None,
) -> Callable[
    [Union[Coroutine[Any, Any, Any], AsyncGenerator[Any, None]]],
    Union[Coroutine[Any, Any, Any], AsyncGenerator[Any, None]],
]:
    """
    Decorator to mark a publisher function. The return value of the function will be
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
    @publisher("my-topic")
    async def my_function():
        return {"foo": "bar"}

    # Publish multiple events using a generator
    @publisher("my-topic")
    async def my_generator():
        for i in range(3):
            yield {"event_id": i}
    ```
    """

    def check_client():
        global _client
        if _client is None:
            raise RuntimeError(
                "publisher requires a connection to Ensign, please use the authenticate decorator to provide your credentials"
            )

    def wrap_coroutine(coro):
        async def wrapper(*args, **kwargs):
            # Ensign client must already exist in this context
            check_client()

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
            # Ensign client must already exist in this context
            check_client()

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

    def decorator(
        fn: Union[Coroutine[Any, Any, Any], AsyncGenerator[Any, None]]
    ) -> Union[Coroutine[Any, Any, Any], AsyncGenerator[Any, None]]:
        """
        The function can be a regular function or a generator function, and it can also
        be synchronous or async. This decorator will handle all of those cases by
        returning the appropriate wrapper function.
        """
        if inspect.isgeneratorfunction(fn):
            # If we have a generator function, make it async. Also wrap the resulting
            # generator in a sync wrapper so that it can be called from sync code.
            return async_to_sync(wrap_async_generator(sync_to_async(fn)))
        elif inspect.isasyncgenfunction(fn):
            # If we have an async generator function we can wrap it in an async
            # generator.
            return wrap_async_generator(fn)
        elif inspect.iscoroutinefunction(fn):
            # If we have a coroutine function we can just await it in the wrapper.
            return wrap_coroutine(fn)
        elif inspect.isfunction(fn):
            # If we have a regular function, make it async. Also wrap the resulting
            # coroutine in a sync wrapper so that it can be called from sync code.
            return async_to_sync(wrap_coroutine(sync_to_async(fn)))
        else:
            raise TypeError(
                "expected a function or generator function, got {}".format(type(fn))
            )

    return decorator


def subscriber(
    *topics: str,
) -> Callable[
    [Union[Coroutine[Any, Any, Any], AsyncGenerator[Any, None]]],
    Union[Coroutine[Any, Any, Any], AsyncGenerator[Any, None]],
]:
    """
    Decorator to mark a subscriber function. If the function is a coroutine then the
    events are passed to the function as an async generator. If the function is not a
    coroutine then the function is treated like a callback - it will be invoked with
    each event that is received. This method assumes that you have already
    authenticated with Ensign using the `@authenticate` decorator.

    Parameters
    ----------
    *topics : iterable of str
        The topic names or IDs to consume events from.

    Usage
    -----
    ```
    # Subscribe to a topic using a generator
    @subscriber("my-topic")
    async def process_events(events):
        async for event in events:
            # Process the event

    # Subscribe to a topic using a callback
    @subscriber("my-topic")
    def process_event(event):
        # Process the event
    ```
    """

    def subscribe():
        global _client
        if _client is None:
            raise RuntimeError(
                "subscriber requires a connection to Ensign, please use the authenticate decorator to provide your credentials"
            )

        # Get the iterator for the events
        return _client.subscribe(topics)

    def wrap_function(fn):
        async def wrapper(*args, **kwargs):
            # Treat the function like a callback
            async for event in subscribe():
                fn(event, *args, **kwargs)

        return wrapper

    def wrap_coroutine(coro):
        async def wrapper(*args, **kwargs):
            # Subscribe to the topics and pass the generator to the coroutine
            await coro(subscribe(), *args, **kwargs)

        return wrapper

    def wrap_async_generator(coro):
        async def wrapper(*args, **kwargs):
            # Create the Ensign client if not already created
            global _client
            if _client is None:
                raise RuntimeError(
                    "subscriber requires a connection to Ensign, please use the authenticate decorator to provide your credentials"
                )

            # Subscribe to the topic and pass the generator to the async generator
            events = subscribe()
            async for res in coro(events, *args, **kwargs):
                yield res

        return wrapper

    def decorator(
        fn: Union[Coroutine[Any, Any, Any], AsyncGenerator[Any, None]]
    ) -> Union[Coroutine[Any, Any, Any], AsyncGenerator[Any, None]]:
        """
        The function can be a regular function or a generator function, and it can also
        be synchronous or async. This decorator will handle all of those cases by
        returning the appropriate wrapper function.
        """
        if inspect.isgeneratorfunction(fn):
            raise TypeError(
                "subscriber does not support synchronous generator functions, please use a coroutine or async generator"
            )
        elif inspect.isasyncgenfunction(fn):
            # If we have an async generator function we can wrap it in an async
            # generator.
            return wrap_async_generator(fn)
        elif inspect.iscoroutinefunction(fn):
            # If we have a coroutine function we can just await it in the wrapper.
            return wrap_coroutine(fn)
        elif inspect.isfunction(fn):
            # If we have a regular function, make it async. Also wrap the resulting
            # coroutine in a sync wrapper so that it can be called from sync code.
            return async_to_sync(wrap_function(fn))
        else:
            raise TypeError(
                "expected a function or generator function, got {}".format(type(fn))
            )

    return decorator
