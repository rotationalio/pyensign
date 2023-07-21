import asyncio

from pyensign.ensign import Ensign


class Subscriber(Ensign):
    """
    A Subscriber is a high-level abstraction for consuming events from an Ensign topic.
    Subscribers consume events from a topic and pass them to a callback function.
    """

    def __init__(self, *topics, **kwargs):
        """
        Create a Subscriber to consume events from a set of topics. Once initialized,
        call run() to start consuming events from the topics.

        Parameters
        ----------
        *topics : iterable of str
            The topic names or IDs to consume events from.

        **kwargs
            Credentials and configuration for the Ensign client, see the Ensign class
            for more details.

        Raises
        ------
        ValueError
            If no topics are provided.
        """

        super().__init__(**kwargs)

        if len(topics) == 0:
            raise ValueError("no topics provided")
        self.subscribed_topics = topics

    async def on_event(self, event):
        """
        Implement this method to process events as they are received. By default, this
        method simply ACKs the events back to the server.
        """

        await event.ack()

    def run(self, **kwargs):
        """
        Consume events from the topics and invoke the callback for each event. This
        method blocks but may raise an EnsignError if the subscriber encounters an
        error communicating with Ensign (e.g. a connection timeout or topic not found error).

        Parameters
        ----------

        **kwargs
            Keyword arguments to pass to the subscribe() method, e.g. enSQL queries or
            consumer group IDs.
        """

        exception = asyncio.run(self.consume(), **kwargs)
        if exception:
            raise exception

    async def consume(self, **kwargs):
        """
        Consume events from the subscriber and execute the callback for each event.
        """

        async for event in self.subscribe(self.subscribed_topics, **kwargs):
            await self.on_event(event)
