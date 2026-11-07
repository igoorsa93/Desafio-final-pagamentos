"""Metrics Service — Prometheus exposition"""
from prometheus_client import (
    Counter,
    Histogram,
    Gauge,
    CollectorRegistry,
    generate_latest,
)
from datetime import datetime
import structlog

logger = structlog.get_logger(__name__)


# Custom registry for isolation
registry = CollectorRegistry()

# Counters
transactions_processed = Counter(
    "consumer_transactions_processed_total",
    "Total enriched transactions processed",
    ["status"],
    registry=registry,
)

fraud_alerts_received = Counter(
    "consumer_fraud_alerts_received_total",
    "Total fraud alerts received from Flink",
    ["severity"],
    registry=registry,
)

dedup_hits = Counter(
    "consumer_dedup_hits_total",
    "Total duplicate transactions detected via idempotency",
    registry=registry,
)

dlq_messages = Counter(
    "consumer_dlq_messages_total",
    "Total messages sent to dead letter queue",
    ["topic"],
    registry=registry,
)

# Histograms
processing_latency = Histogram(
    "consumer_processing_latency_seconds",
    "Time from event_ts to processing complete",
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0],
    registry=registry,
)

# Gauges
last_event_timestamp = Gauge(
    "consumer_last_event_timestamp",
    "Unix timestamp of last processed event",
    registry=registry,
)

consumer_lag = Gauge(
    "consumer_kafka_lag",
    "Consumer lag per partition",
    ["topic", "partition"],
    registry=registry,
)


class MetricsService:
    """Application metrics collector"""

    def __init__(self):
        self._snapshot = {
            "transactions_processed": 0,
            "fraud_alerts_generated": 0,
            "dedup_hits": 0,
            "processing_latency_ms_sum": 0.0,
            "processing_latency_count": 0,
            "last_event_ts": None,
        }

    def record_transaction(
        self, status: str, latency_ms: float, event_ts: datetime
    ) -> None:
        transactions_processed.labels(status=status).inc()
        processing_latency.observe(latency_ms / 1000.0)
        last_event_timestamp.set(event_ts.timestamp())

        self._snapshot["transactions_processed"] += 1
        self._snapshot["processing_latency_ms_sum"] += latency_ms
        self._snapshot["processing_latency_count"] += 1
        self._snapshot["last_event_ts"] = event_ts

    def record_fraud_alert(self, severity: str) -> None:
        fraud_alerts_received.labels(severity=severity).inc()
        self._snapshot["fraud_alerts_generated"] += 1

    def record_dedup_hit(self) -> None:
        dedup_hits.inc()
        self._snapshot["dedup_hits"] += 1

    def record_dlq(self, topic: str) -> None:
        dlq_messages.labels(topic=topic).inc()

    def update_lag(self, topic: str, partition: int, lag: int) -> None:
        consumer_lag.labels(topic=topic, partition=partition).set(lag)

    def get_snapshot(self) -> dict:
        avg_latency = 0.0
        if self._snapshot["processing_latency_count"] > 0:
            avg_latency = (
                self._snapshot["processing_latency_ms_sum"]
                / self._snapshot["processing_latency_count"]
            )

        return {
            "transactions_processed": self._snapshot["transactions_processed"],
            "fraud_alerts_generated": self._snapshot["fraud_alerts_generated"],
            "dedup_hits": self._snapshot["dedup_hits"],
            "processing_latency_ms_avg": round(avg_latency, 2),
            "last_event_ts": self._snapshot["last_event_ts"].isoformat()
            if self._snapshot["last_event_ts"]
            else None,
        }

    def prometheus_output(self) -> bytes:
        """Generate Prometheus metrics exposition"""
        return generate_latest(registry)
