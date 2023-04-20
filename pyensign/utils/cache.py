from pyensign.exceptions import CacheMissError


class Cache:
    """
    Cache is a simple cache that maps keys to values to avoid repeated RPC calls to
    Ensign. This is not thread-safe.
    TODO: Implement max size and evictions.
    """

    def __init__(self):
        self._index = {}

    def get(self, key):
        """
        Get a value by name, an exception is raised if the key does not exist in the
        cache.
        """
        try:
            return self._index[key]
        except KeyError as e:
            raise CacheMissError(key) from e

    def add(self, key, value):
        """
        Add a value by key to the cache, overwriting the existing key.
        """
        self._index[key] = value

    def exists(self, key):
        """
        Returns True if the key exists in the cache.
        """
        return key in self._index

    def clear(self):
        """
        Reset the cache, deleting all keys.
        """
        self._index.clear()
