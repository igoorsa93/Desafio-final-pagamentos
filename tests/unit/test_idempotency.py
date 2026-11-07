"""Unit Tests — Idempotency Service (mocked)"""
import pytest
from unittest.mock import AsyncMock

from consumer_app.services.idempotency import IdempotencyService


@pytest.mark.asyncio
class TestIdempotencyService:
    """Test IdempotencyService with mocked Redis"""

    @pytest.fixture
    async def service(self):
        svc = IdempotencyService(redis_url="redis://localhost:6379", ttl_seconds=3600)
        # Mock Redis client
        svc._client = AsyncMock()
        yield svc
        await svc.close()

    async def test_try_acquire_first_time(self, service):
        """First acquire should return True"""
        service._client.set.return_value = True
        result = await service.try_acquire("test-key-123")
        assert result is True
        service._client.set.assert_called_once_with(
            "idempotency:test-key-123", "1", nx=True, ex=3600
        )

    async def test_try_acquire_duplicate(self, service):
        """Duplicate acquire should return False"""
        service._client.set.return_value = False
        result = await service.try_acquire("test-key-123")
        assert result is False

    async def test_is_duplicate_exists(self, service):
        """Check duplicate when key exists"""
        service._client.exists.return_value = 1
        result = await service.is_duplicate("test-key-123")
        assert result is True
        service._client.exists.assert_called_once_with("idempotency:test-key-123")

    async def test_is_duplicate_not_exists(self, service):
        """Check duplicate when key doesn't exist"""
        service._client.exists.return_value = 0
        result = await service.is_duplicate("test-key-123")
        assert result is False

    async def test_not_connected_raises(self):
        """Operations on unconnected service should raise"""
        svc = IdempotencyService()
        with pytest.raises(RuntimeError, match="not connected"):
            await svc.try_acquire("test-key")

    async def test_close(self, service):
        """Close should call redis close"""
        await service.close()
        service._client.close.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
