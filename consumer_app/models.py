"""Domain Models — Consumer App"""
from pydantic import BaseModel, Field
from typing import Optional, Literal
from datetime import datetime
from decimal import Decimal


class Money(BaseModel):
    """Value object for monetary amounts"""

    cents: int = Field(ge=0)

    @property
    def decimal(self) -> Decimal:
        return Decimal(self.cents) / 100

    def __str__(self) -> str:
        return f"${self.decimal:.2f}"


class EnrichedTransaction(BaseModel):
    """Enriched transaction from Flink SQL temporal join"""

    transaction_id: int
    account_id: int
    card_id: int
    merchant_id: int
    amount: Money
    currency: str = "USD"
    status: Literal["pending", "authorized", "settled", "failed", "refunded"]
    event_ts: datetime
    idempotency_key: str
    metadata: dict = Field(default_factory=dict)

    # Enriched fields (temporal join)
    account_balance: Money
    account_status: Literal["active", "frozen", "closed"]
    card_number: str  # masked
    card_brand: Literal["visa", "mastercard", "amex", "elo", "other"]
    card_status: Literal["active", "expired", "blocked", "cancelled"]
    merchant_mcc: str
    merchant_name: str
    merchant_risk_score: int = Field(ge=0, le=100)


class FraudAlert(BaseModel):
    """Fraud alert from Flink MATCH_RECOGNIZE"""

    alert_id: str
    card_id: int
    transaction_ids: list[int]
    pattern: Literal["velocity", "high_risk_merchant", "amount_anomaly"]
    severity: Literal["low", "medium", "high", "critical"]
    detected_at: datetime
    details: dict = Field(default_factory=dict)


class HealthResponse(BaseModel):
    status: Literal["healthy", "degraded", "unhealthy"]
    kafka_connected: bool
    redis_connected: bool
    schema_registry_connected: bool
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class MetricsSnapshot(BaseModel):
    transactions_processed: int = 0
    fraud_alerts_generated: int = 0
    dedup_hits: int = 0
    processing_latency_ms_avg: float = 0.0
    last_event_ts: Optional[datetime] = None
