import pytest

from pyensign.utils.cache import Cache
from pyensign.exceptions import CacheMissError


class TestCache:
    """
    Tests for the cache utility.
    """

    def test_get(self):
        cache = Cache()
        cache.add("key", "value")
        assert cache.get("key") == "value"

    def test_get_error(self):
        cache = Cache()
        with pytest.raises(CacheMissError, match="key"):
            cache.get("key")

    def test_clear(self):
        cache = Cache()
        cache.add("key", "value")
        cache.clear()
        with pytest.raises(CacheMissError, match="key"):
            cache.get("key")
