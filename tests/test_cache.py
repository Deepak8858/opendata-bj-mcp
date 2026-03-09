"""Tests for caching utilities."""

import asyncio
import time
import pytest
from opendata_bj.cache import CacheEntry, TTLCache, MultiLevelCache


class TestCacheEntry:
    """Tests for CacheEntry dataclass."""

    def test_cache_entry_creation(self):
        entry = CacheEntry(value="test_value")
        assert entry.value == "test_value"
        assert entry.timestamp <= time.time()

    def test_cache_entry_with_custom_timestamp(self):
        custom_time = time.time() - 100
        entry = CacheEntry(value="test", timestamp=custom_time)
        assert entry.timestamp == custom_time


class TestTTLCache:
    """Tests for TTLCache implementation."""

    @pytest.mark.asyncio
    async def test_basic_get_set(self):
        cache = TTLCache[str](max_size=10, ttl_seconds=60)
        
        await cache.set("key1", "value1")
        result = await cache.get("key1")
        
        assert result == "value1"

    @pytest.mark.asyncio
    async def test_get_nonexistent_key(self):
        cache = TTLCache[str](max_size=10, ttl_seconds=60)
        
        result = await cache.get("nonexistent")
        
        assert result is None

    @pytest.mark.asyncio
    async def test_ttl_expiration(self):
        cache = TTLCache[str](max_size=10, ttl_seconds=0.1)  # 100ms TTL
        
        await cache.set("key1", "value1")
        
        # Should return value immediately
        result = await cache.get("key1")
        assert result == "value1"
        
        # Wait for expiration
        await asyncio.sleep(0.15)
        
        # Should return None after expiration
        result = await cache.get("key1")
        assert result is None

    @pytest.mark.asyncio
    async def test_lru_eviction(self):
        cache = TTLCache[int](max_size=3, ttl_seconds=60)
        
        await cache.set("key1", 1)
        await cache.set("key2", 2)
        await cache.set("key3", 3)
        await cache.set("key4", 4)  # Should evict key1
        
        assert await cache.get("key1") is None
        assert await cache.get("key2") == 2
        assert await cache.get("key3") == 3
        assert await cache.get("key4") == 4

    @pytest.mark.asyncio
    async def test_lru_update_on_access(self):
        cache = TTLCache[int](max_size=3, ttl_seconds=60)
        
        await cache.set("key1", 1)
        await cache.set("key2", 2)
        await cache.set("key3", 3)
        
        # Access key1 to make it most recently used
        await cache.get("key1")
        
        # Add key4, should evict key2 (least recently used)
        await cache.set("key4", 4)
        
        assert await cache.get("key1") == 1  # Should still exist
        assert await cache.get("key2") is None  # Should be evicted
        assert await cache.get("key3") == 3
        assert await cache.get("key4") == 4

    @pytest.mark.asyncio
    async def test_delete_existing_key(self):
        cache = TTLCache[str](max_size=10, ttl_seconds=60)
        
        await cache.set("key1", "value1")
        deleted = await cache.delete("key1")
        
        assert deleted is True
        assert await cache.get("key1") is None

    @pytest.mark.asyncio
    async def test_delete_nonexistent_key(self):
        cache = TTLCache[str](max_size=10, ttl_seconds=60)
        
        deleted = await cache.delete("nonexistent")
        
        assert deleted is False

    @pytest.mark.asyncio
    async def test_clear_cache(self):
        cache = TTLCache[str](max_size=10, ttl_seconds=60)
        
        await cache.set("key1", "value1")
        await cache.set("key2", "value2")
        await cache.clear()
        
        assert await cache.get("key1") is None
        assert await cache.get("key2") is None
        stats = await cache.get_stats()
        assert stats["size"] == 0

    @pytest.mark.asyncio
    async def test_get_stats(self):
        cache = TTLCache[str](max_size=100, ttl_seconds=300)
        
        await cache.set("key1", "value1")
        await cache.set("key2", "value2")
        
        stats = await cache.get_stats()
        
        assert stats["size"] == 2
        assert stats["max_size"] == 100
        assert stats["ttl_seconds"] == 300
        assert stats["valid_entries"] == 2

    @pytest.mark.asyncio
    async def test_get_stats_with_expired_entries(self):
        cache = TTLCache[str](max_size=10, ttl_seconds=0.1)
        
        await cache.set("key1", "value1")
        await asyncio.sleep(0.15)
        await cache.set("key2", "value2")
        
        stats = await cache.get_stats()
        
        assert stats["size"] == 2  # Both entries exist
        assert stats["valid_entries"] == 1  # Only key2 is valid

    @pytest.mark.asyncio
    async def test_thread_safety_concurrent_access(self):
        """Test that cache handles concurrent access safely."""
        cache = TTLCache[int](max_size=100, ttl_seconds=60)
        
        async def writer(start: int, count: int):
            for i in range(count):
                await cache.set(f"key_{start + i}", start + i)
                await asyncio.sleep(0.001)
        
        async def reader(keys: list):
            for key in keys:
                await cache.get(key)
                await asyncio.sleep(0.001)
        
        # Run concurrent operations
        await asyncio.gather(
            writer(0, 20),
            writer(100, 20),
            reader([f"key_{i}" for i in range(30)]),
            reader([f"key_{i}" for i in range(100, 130)]),
        )
        
        # Cache should be in a consistent state
        stats = await cache.get_stats()
        assert stats["size"] <= 100


class TestMultiLevelCache:
    """Tests for MultiLevelCache implementation."""

    @pytest.mark.asyncio
    async def test_initialization(self):
        cache = MultiLevelCache(
            enable_cache=True,
            dataset_ttl=300,
            organization_ttl=3600,
            resource_ttl=120,
            max_size=50,
        )
        
        assert cache.enable_cache is True
        assert cache.datasets.max_size == 50
        assert cache.datasets.ttl_seconds == 300
        assert cache.organizations.ttl_seconds == 3600
        assert cache.resources.ttl_seconds == 120

    @pytest.mark.asyncio
    async def test_disabled_cache(self):
        cache = MultiLevelCache(enable_cache=False)
        
        assert cache.enable_cache is False
        # Can still use cache operations, but client should check enable_cache
        await cache.datasets.set("key", "value")
        result = await cache.datasets.get("key")
        assert result == "value"

    @pytest.mark.asyncio
    async def test_clear_all(self):
        cache = MultiLevelCache()
        
        await cache.datasets.set("ds1", "dataset1")
        await cache.organizations.set("org1", "org1")
        await cache.resources.set("res1", "resource1")
        
        await cache.clear_all()
        
        assert await cache.datasets.get("ds1") is None
        assert await cache.organizations.get("org1") is None
        assert await cache.resources.get("res1") is None

    @pytest.mark.asyncio
    async def test_get_stats(self):
        cache = MultiLevelCache()
        
        await cache.datasets.set("ds1", "dataset1")
        await cache.organizations.set("org1", "org1")
        
        stats = await cache.get_stats()
        
        assert stats["enabled"] is True
        assert stats["datasets"]["size"] == 1
        assert stats["organizations"]["size"] == 1
        assert stats["resources"]["size"] == 0

    @pytest.mark.asyncio
    async def test_different_ttl_behavior(self):
        """Test that different caches have independent TTLs."""
        cache = MultiLevelCache(
            dataset_ttl=0.1,  # 100ms
            organization_ttl=60,  # 60s
            resource_ttl=0.1,  # 100ms
        )
        
        await cache.datasets.set("ds1", "dataset1")
        await cache.organizations.set("org1", "org1")
        
        # Wait for dataset TTL to expire
        await asyncio.sleep(0.15)
        
        # Dataset should be expired
        assert await cache.datasets.get("ds1") is None
        # Organization should still exist
        assert await cache.organizations.get("org1") == "org1"


class TestCacheIntegration:
    """Integration tests simulating real-world usage patterns."""

    @pytest.mark.asyncio
    async def test_simulate_dataset_caching(self):
        """Simulate caching dataset queries."""
        cache = TTLCache[list](max_size=10, ttl_seconds=300)
        
        # First query - cache miss
        datasets = [{"id": "1", "title": "Dataset 1"}, {"id": "2", "title": "Dataset 2"}]
        await cache.set("query:health:10:0", datasets)
        
        # Second query - cache hit
        cached = await cache.get("query:health:10:0")
        assert cached == datasets
        
        # Different query - cache miss
        cached = await cache.get("query:education:10:0")
        assert cached is None

    @pytest.mark.asyncio
    async def test_cache_key_pattern(self):
        """Test typical cache key patterns."""
        cache = TTLCache(max_size=10, ttl_seconds=60)
        
        # Dataset details cache key
        await cache.set("details:dataset123", {"id": "dataset123", "title": "Test"})
        
        # Search results cache key
        await cache.set("search:santé:limit10:offset0", [{"id": "1"}])
        
        assert await cache.get("details:dataset123") is not None
        assert await cache.get("search:santé:limit10:offset0") is not None
