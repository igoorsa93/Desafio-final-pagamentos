"""Kafka Gateway — Consumer/Producer with Avro"""
import json
from typing import AsyncIterator, Optional
import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from consumer_app.config import settings
from consumer_app.models import EnrichedTransaction, FraudAlert

logger = structlog.get_logger(__name__)


class KafkaGateway:
    """Kafka consumer/producer with Schema Registry Avro support"""

    def __init__(self):
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._producer: Optional[AIOKafkaProducer] = None
        self._schema_registry: Optional[SchemaRegistryClient] = None
        self._avro_deserializer: Optional[AvroDeserializer] = None

    async def connect(self) -> None:
        # Schema Registry client
        self._schema_registry = SchemaRegistryClient({"url": settings.schema_registry_url})

        # Consumer
        self._consumer = AIOKafkaConsumer(
            settings.topic_transactions,
            settings.topic_fraud_alerts,
            bootstrap_servers=settings.kafka_bootstrap,
            group_id=settings.kafka_group_id,
            auto_offset_reset=settings.kafka_auto_offset_reset,
            enable_auto_commit=False,  # manual commit after processing
            value_deserializer=lambda m: m,  # raw bytes, deserialize manually
        )
        await self._consumer.start()
        logger.info("kafka_consumer_started", topics=[settings.topic_transactions, settings.topic_fraud_alerts])

        # Producer (for dead letter / reprocessing)
        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap,
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        await self._producer.start()
        logger.info("kafka_producer_started")

    async def close(self) -> None:
        if self._consumer:
            await self._consumer.stop()
        if self._producer:
            await self._producer.stop()

    async def consume_transactions(self) -> AsyncIterator[EnrichedTransaction]:
        """Consume enriched transactions from Flink output topic"""
        async for msg in self._consumer:
            if msg.topic == settings.topic_transactions:
                try:
                    # Deserialize Avro (simplified - in production use proper deserializer)
                    transaction = EnrichedTransaction.model_validate_json(msg.value.decode())
                    yield transaction
                except Exception as e:
                    logger.error("deserialize_failed", error=str(e), topic=msg.topic, offset=msg.offset)
                    # Send to DLQ
                    await self.send_dlq(msg, str(e))
            await self._consumer.commit()

    async def consume_fraud_alerts(self) -> AsyncIterator[FraudAlert]:
        """Consume fraud alerts from Flink MATCH_RECOGNIZE output"""
        async for msg in self._consumer:
            if msg.topic == settings.topic_fraud_alerts:
                try:
                    alert = FraudAlert.model_validate_json(msg.value.decode())
                    yield alert
                except Exception as e:
                    logger.error("deserialize_fraud_failed", error=str(e), offset=msg.offset)
                    await self.send_dlq(msg, str(e))
            await self._consumer.commit()

    async def send_dlq(self, msg, error: str) -> None:
        """Send failed message to dead letter queue"""
        dlq_topic = f"{msg.topic}.dlq"
        await self._producer.send_and_wait(dlq_topic, {
            "original_topic": msg.topic,
            "original_partition": msg.partition,
            "original_offset": msg.offset,
            "error": error,
            "payload": msg.value.decode() if msg.value else None,
        })
        logger.warning("sent_to_dlq", topic=dlq_topic, original_topic=msg.topic)

    async def health_check(self) -> bool:
        try:
            # Simple metadata fetch
            await self._consumer._client.fetch_api_versions()
            return True
        except Exception:
            return False