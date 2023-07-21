import asyncio
import inspect

from pyensign.ensign import Ensign


class Publisher(Ensign):
    """
    A Publisher is a high-level abstraction for publishing events to Ensign. Publishers
    consume an iterator of events and publish them to an Ensign topic.
    """

    def __init__(self, topic, **kwargs):
        """
        Create a Publisher to publish events to a topic. Once initialized, call run()
        to publish events to the topic.

        Parameters
        ----------
        topic : str
            The topic name or ID to publish events to.

        **kwargs
            Credentials and configuration for the Ensign client, see the Ensign class
            for more details.

        Raises
        ------
        ValueError
            If a topic is not provided.
        """

        super().__init__(**kwargs)

        if not topic:
            raise ValueError("topic is required")
        self.topic = topic

    def run(self, event_iterator, **kwargs):
        """
        Publish all the events from the iterator to the topic. This method blocks until
        the iterator is exhausted.

        Parameters
        ----------
        event_iterator : Iterator or AsyncIterator
            An iterator of events to publish to the topic.

        **kwargs
            Additional keyword arguments to pass to the publish() method.
        """

        exception = asyncio.run(self.publish_from(event_iterator, **kwargs))
        if exception:
            raise exception

    async def publish_from(self, event_iterator, **kwargs):
        """
        Publish all the events from the iterator to the topic. This method blocks until
        all the events have been published.

        Parameters
        ----------
        event_iterator : Iterator or AsyncIterator
            An iterator of events to publish to the topic.

        **kwargs
            Additional keyword arguments to pass to the publish() method.
        """

        if inspect.isasyncgen(event_iterator):
            async for event in event_iterator:
                try:
                    await self.publish(self.topic, event, **kwargs)
                except Exception as e:
                    return e
        else:
            for event in event_iterator:
                try:
                    await self.publish(self.topic, event, **kwargs)
                except Exception as e:
                    return e
