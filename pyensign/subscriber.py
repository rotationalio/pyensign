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
        """

        super().__init__(**kwargs)

        if len(topics) == 0:
            raise ValueError("no topics provided")
        self.subscribed_topics = topics

    def run(self, **kwargs):
        """
        Consume events from the topics and invoke the callback for each event. This
        method starts the subscriber in the background and returns immediately.

        Parameters
        ----------

        **kwargs
            Keyword arguments to pass to the subscribe() method, e.g. on_event can be
            used to specify an async callback when an event is received.
        """

        exception = asyncio.get_event_loop().run_until_complete(
            self.subscribe(self.subscribed_topics, **kwargs)
        )
        if exception:
            raise exception
