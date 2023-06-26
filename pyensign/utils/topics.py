from ulid import ULID

from pyensign.utils.cache import Cache
from pyensign.exceptions import CacheMissError


class Topic:
    """
    Topics have a user-defined name but are also unique by ULID. This class stores both
    representations to make topics easier to work with.
    """

    def __init__(self, id=None, name="", topic_str=""):
        self.id = id
        self.name = name
        self.topic_str = topic_str

    def __hash__(self):
        if self.id is None:
            raise ValueError("cannot hash topic with no ID")
        return hash(str(self.id))

    def __eq__(self, other):
        return self.id == other.id


class TopicCache(Cache):
    """
    TopicCache extends the functionality of the Cache class to support topic ID parsing.
    """

    def resolve(self, topic_str):
        """
        Resolve a topic string into a ULID by looking it up in the cache, otherwise
        assume it's a topic ID and try to parse it.
        """

        # Attempt to retrieve the topic ID from the cache
        try:
            return self.get(topic_str)
        except CacheMissError:
            pass

        # Otherwise attempt to parse as a ULID
        try:
            return ULID.from_str(topic_str)
        except ValueError:
            raise CacheMissError(topic_str)
