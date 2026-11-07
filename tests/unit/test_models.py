"""Unit Tests — Domain Models"""
import pytest
from datetime import datetime
from decimal import Decimal

from consumer_app.models import Money, EnrichedTransaction, FraudAlert, HealthResponse


class TestMoney:
    """Test Money value object"""

    def test_money_creation(self):
        m = Money(cents=12345)
        assert m.cents == 12345
        assert m.decimal == Decimal("123.45")

    def test_money_str(self):
        m = Money(cents=100)
        assert str(m) == "$1.00"

    def test_money_zero(self):
        m = Money(cents=0)
        assert m.decimal == Decimal("0.00")

    def test_money_negative_raises(self):
        with pytest.raises(ValueError):
            Money(cents=-1)


class TestEnrichedTransaction:
    """Test EnrichedTransaction model"""

    def test_minimal_valid(self):
        tx = EnrichedTransaction(
            transaction_id=1,
            account_id=10,
            card_id=100,
            merchant_id=1000,
            amount=Money(cents=5000),
            status="authorized",
            event_ts=datetime(2026, 1, 1, 12, 0, 0),
            idempotency_key="test-key-123",
            account_balance=Money(cents=100000),
            account_status="active",
            card_number="****1234",
            card_brand="visa",
            card_status="active",
            merchant_mcc="5411",
            merchant_name="Test Merchant",
            merchant_risk_score=10,
        )
        assert tx.transaction_id == 1
        assert tx.amount.decimal == Decimal("50.00")

    def test_metadata_optional(self):
        tx = EnrichedTransaction(
            transaction_id=1,
            account_id=10,
            card_id=100,
            merchant_id=1000,
            amount=Money(cents=5000),
            status="authorized",
            event_ts=datetime(2026, 1, 1, 12, 0, 0),
            idempotency_key="test-key-123",
            account_balance=Money(cents=100000),
            account_status="active",
            card_number="****1234",
            card_brand="visa",
            card_status="active",
            merchant_mcc="5411",
            merchant_name="Test Merchant",
            merchant_risk_score=10,
            metadata={"device_id": "dev-123", "channel": "mobile"},
        )
        assert tx.metadata["device_id"] == "dev-123"


class TestFraudAlert:
    """Test FraudAlert model"""

    def test_velocity_alert(self):
        alert = FraudAlert(
            alert_id="VEL-1001-123",
            card_id=1001,
            transaction_ids=[1, 2, 3],
            pattern="velocity",
            severity="high",
            detected_at=datetime.utcnow(),
            details={"window_start": "2026-01-01T12:00:00Z", "hit_count": 3},
        )
        assert alert.pattern == "velocity"
        assert alert.severity == "high"
        assert len(alert.transaction_ids) == 3


class TestHealthResponse:
    """Test HealthResponse model"""

    def test_healthy(self):
        health = HealthResponse(
            status="healthy",
            kafka_connected=True,
            redis_connected=True,
            schema_registry_connected=True,
        )
        assert health.status == "healthy"
        assert all(
            [
                health.kafka_connected,
                health.redis_connected,
                health.schema_registry_connected,
            ]
        )

    def test_degraded(self):
        health = HealthResponse(
            status="degraded",
            kafka_connected=True,
            redis_connected=False,
            schema_registry_connected=True,
        )
        assert health.status == "degraded"
        assert not health.redis_connected


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
