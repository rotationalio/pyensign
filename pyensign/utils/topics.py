from ulid import ULID

from pyensign.utils.cache import Cache
from pyensign.exceptions import CacheMissError


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
