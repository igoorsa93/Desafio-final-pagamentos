"""Main FastAPI Application — Consumer App"""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional
import structlog
from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import PlainTextResponse

from consumer_app.config import settings
from consumer_app.models import HealthResponse, MetricsSnapshot
from consumer_app.services.idempotency import IdempotencyService
from consumer_app.services.kafka_gateway import KafkaGateway
from consumer_app.services.metrics import MetricsService

# Structured logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(settings.log_level),
)

logger = structlog.get_logger(__name__)

# Global services
idempotency: Optional[IdempotencyService] = None
kafka: Optional[KafkaGateway] = None
metrics: Optional[MetricsService] = None
_consumer_tasks: list[asyncio.Task] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    global idempotency, kafka, metrics, _consumer_tasks

    logger.info("starting_consumer_app")

    # Initialize services
    idempotency = IdempotencyService()
    await idempotency.connect()

    kafka = KafkaGateway()
    await kafka.connect()

    metrics = MetricsService()

    # Start background consumers
    _consumer_tasks = [
        asyncio.create_task(consume_transactions_loop()),
        asyncio.create_task(consume_fraud_alerts_loop()),
    ]

    logger.info("consumer_app_started")
    yield

    # Shutdown
    logger.info("stopping_consumer_app")
    for task in _consumer_tasks:
        task.cancel()
    await asyncio.gather(*_consumer_tasks, return_exceptions=True)

    await idempotency.close()
    await kafka.close()
    logger.info("consumer_app_stopped")


app = FastAPI(
    title="Desafio Final — Payment Consumer",
    version="1.0.0",
    lifespan=lifespan,
)


async def consume_transactions_loop():
    """Background task: consume enriched transactions"""
    global kafka, idempotency, metrics
    async for tx in kafka.consume_transactions():
        try:
            # Idempotency check
            if not await idempotency.try_acquire(tx.idempotency_key):
                metrics.record_dedup_hit()
                logger.info(
                    "duplicate_transaction",
                    transaction_id=tx.transaction_id,
                    key=tx.idempotency_key,
                )
                continue

            # Process transaction (business logic here)
            latency_ms = (datetime.utcnow() - tx.event_ts).total_seconds() * 1000
            metrics.record_transaction(tx.status, latency_ms, tx.event_ts)

            logger.info(
                "transaction_processed",
                transaction_id=tx.transaction_id,
                amount=str(tx.amount),
                card_id=tx.card_id,
                merchant_id=tx.merchant_id,
            )
        except Exception as e:
            logger.error(
                "process_transaction_failed",
                error=str(e),
                transaction_id=tx.transaction_id,
            )
            await kafka.send_dlq(
                type(
                    "Msg",
                    (),
                    {
                        "topic": settings.topic_transactions,
                        "partition": 0,
                        "offset": 0,
                        "value": tx.model_dump_json().encode(),
                    },
                )(),
                str(e),
            )


async def consume_fraud_alerts_loop():
    """Background task: consume fraud alerts"""
    global kafka, metrics
    async for alert in kafka.consume_fraud_alerts():
        try:
            metrics.record_fraud_alert(alert.severity)
            logger.warning(
                "fraud_alert_received",
                alert_id=alert.alert_id,
                card_id=alert.card_id,
                pattern=alert.pattern,
                severity=alert.severity,
                transaction_ids=alert.transaction_ids,
            )
            # TODO: Webhook, notification, case creation
        except Exception as e:
            logger.error(
                "process_fraud_alert_failed", error=str(e), alert_id=alert.alert_id
            )


@app.get("/health", response_model=HealthResponse)
async def health():
    """Health check endpoint"""
    kafka_ok = await kafka.health_check() if kafka else False
    redis_ok = False
    if idempotency and idempotency._client:
        try:
            await idempotency._client.ping()
            redis_ok = True
        except Exception:
            pass
    sr_ok = False
    if kafka and kafka._schema_registry:
        try:
            kafka._schema_registry.get_subjects()
            sr_ok = True
        except Exception:
            pass

    status = "healthy" if all([kafka_ok, redis_ok, sr_ok]) else "degraded"
    return HealthResponse(
        status=status,
        kafka_connected=kafka_ok,
        redis_connected=redis_ok,
        schema_registry_connected=sr_ok,
    )


@app.get("/metrics", response_class=PlainTextResponse)
async def prometheus_metrics():
    """Prometheus metrics exposition"""
    if not metrics:
        raise HTTPException(503, "Metrics not initialized")
    return Response(content=metrics.prometheus_output(), media_type="text/plain")


@app.get("/metrics/snapshot", response_model=MetricsSnapshot)
async def metrics_snapshot():
    """JSON metrics snapshot for dashboards"""
    if not metrics:
        raise HTTPException(503, "Metrics not initialized")
    return MetricsSnapshot(**metrics.get_snapshot())


@app.post("/admin/reprocess/{topic}")
async def reprocess_dlq(topic: str):
    """Admin: reprocess messages from DLQ (manual trigger)"""
    # Implementation: read from {topic}.dlq, re-produce to original topic
    return {"status": "not_implemented", "topic": topic}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=settings.host, port=settings.port)
