"""Caching utilities for API responses.

Provides a thread-safe, TTL-based cache with LRU eviction policy.
"""

import asyncio
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Dict, Generic, Optional, TypeVar

T = TypeVar("T")


@dataclass
class CacheEntry(Generic[T]):
    """A single cache entry with value and timestamp."""

    value: T
    timestamp: float = field(default_factory=time.time)


class TTLCache(Generic[T]):
    """Thread-safe cache with TTL and LRU eviction.

    This cache implementation provides:
    - Time-based expiration (TTL)
    - Size-based eviction (LRU)
    - Thread-safe operations using asyncio.Lock

    Attributes:
        max_size: Maximum number of entries in cache
        ttl_seconds: Time-to-live in seconds for cached entries
    """

    def __init__(self, max_size: int = 100, ttl_seconds: float = 300):
        """Initialize the TTL cache.

        Args:
            max_size: Maximum number of entries (default: 100)
            ttl_seconds: Time-to-live in seconds (default: 300 = 5 minutes)
        """
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self._cache: OrderedDict[str, CacheEntry[T]] = OrderedDict()
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[T]:
        """Get value from cache if present and not expired.

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found or expired
        """
        async with self._lock:
            entry = self._cache.get(key)

            if entry is None:
                return None

            if time.time() - entry.timestamp > self.ttl_seconds:
                del self._cache[key]
                return None

            self._cache.move_to_end(key)
            return entry.value

    async def set(self, key: str, value: T) -> None:
        """Set value in cache.

        Args:
            key: Cache key
            value: Value to cache
        """
        async with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)

            self._cache[key] = CacheEntry(value=value)

            while len(self._cache) > self.max_size:
                self._cache.popitem(last=False)

    async def delete(self, key: str) -> bool:
        """Delete a specific key from cache.

        Args:
            key: Cache key to delete

        Returns:
            True if key was found and deleted, False otherwise
        """
        async with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    async def clear(self) -> None:
        """Clear all entries from cache."""
        async with self._lock:
            self._cache.clear()

    async def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dictionary with size, max_size, ttl, and entry count
        """
        async with self._lock:
            now = time.time()
            valid_entries = sum(
                1
                for entry in self._cache.values()
                if now - entry.timestamp <= self.ttl_seconds
            )

            return {
                "size": len(self._cache),
                "max_size": self.max_size,
                "ttl_seconds": self.ttl_seconds,
                "valid_entries": valid_entries,
            }


class MultiLevelCache:
    """Multi-level cache with different TTLs for different data types.

    Provides preconfigured caches for different types of data:
    - datasets: Short TTL (5 min) as data changes frequently
    - organizations: Long TTL (1 hour) as they rarely change
    - resources: Short TTL (2 min) as they may change
    """

    def __init__(
        self,
        enable_cache: bool = True,
        dataset_ttl: float = 300,
        organization_ttl: float = 3600,
        resource_ttl: float = 120,
        max_size: int = 100,
    ):
        """Initialize multi-level cache.

        Args:
            enable_cache: Whether caching is enabled (default: True)
            dataset_ttl: TTL for dataset cache in seconds (default: 300)
            organization_ttl: TTL for organization cache in seconds (default: 3600)
            resource_ttl: TTL for resource cache in seconds (default: 120)
            max_size: Maximum entries per cache (default: 100)
        """
        self.enable_cache = enable_cache
        self.datasets = TTLCache(max_size=max_size, ttl_seconds=dataset_ttl)
        self.organizations = TTLCache(max_size=max_size, ttl_seconds=organization_ttl)
        self.resources = TTLCache(max_size=max_size, ttl_seconds=resource_ttl)

    async def clear_all(self) -> None:
        """Clear all caches."""
        await self.datasets.clear()
        await self.organizations.clear()
        await self.resources.clear()

    async def get_stats(self) -> Dict[str, Any]:
        """Get statistics for all caches.

        Returns:
            Dictionary with stats for each cache level
        """
        return {
            "enabled": self.enable_cache,
            "datasets": await self.datasets.get_stats(),
            "organizations": await self.organizations.get_stats(),
            "resources": await self.resources.get_stats(),
        }
