"""Idempotency Service — Redis SETNX with TTL"""
import redis.asyncio as redis
from typing import Optional
import structlog

from consumer_app.config import settings

logger = structlog.get_logger(__name__)


class IdempotencyService:
    """Deduplication via Redis SETNX — exactly-once consumer side"""

    def __init__(self, redis_url: str = None, ttl_seconds: int = None):
        self._redis_url = redis_url or settings.redis_url
        self._ttl = ttl_seconds or settings.idempotency_ttl_seconds
        self._client: Optional[redis.Redis] = None

    async def connect(self) -> None:
        self._client = redis.from_url(self._redis_url, decode_responses=True)
        await self._client.ping()
        logger.info("idempotency_redis_connected", url=self._redis_url)

    async def close(self) -> None:
        if self._client:
            await self._client.close()

    async def try_acquire(self, key: str) -> bool:
        """
        Try to acquire lock for idempotency key.
        Returns True if acquired (first time), False if already exists (duplicate).
        """
        if not self._client:
            raise RuntimeError("IdempotencyService not connected")

        # SET key value NX EX ttl — atomic check-and-set
        acquired = await self._client.set(
            f"idempotency:{key}",
            "1",
            nx=True,      # only set if not exists
            ex=self._ttl  # TTL in seconds
        )
        return acquired is True

    async def is_duplicate(self, key: str) -> bool:
        """Check if key exists (without acquiring)"""
        if not self._client:
            raise RuntimeError("IdempotencyService not connected")
        exists = await self._client.exists(f"idempotency:{key}")
        return exists > 0