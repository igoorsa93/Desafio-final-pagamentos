"""Configuration — Consumer App"""
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    # Kafka
    kafka_bootstrap: str = Field(
        default="localhost:9092", validation_alias="KAFKA_BOOTSTRAP"
    )
    kafka_group_id: str = "desafio-consumer"
    kafka_auto_offset_reset: str = "earliest"

    # Schema Registry
    schema_registry_url: str = Field(
        default="http://localhost:8081", validation_alias="SCHEMA_REGISTRY_URL"
    )

    # Topics
    topic_transactions: str = "desafio.payments.transactions"
    topic_fraud_alerts: str = "desafio.payments.fraud_alerts"

    # Redis (idempotency)
    redis_url: str = Field(
        default="redis://localhost:6379/0", validation_alias="REDIS_URL"
    )
    idempotency_ttl_seconds: int = 86400  # 24h

    # Prometheus
    prometheus_url: str = Field(
        default="http://localhost:9090", validation_alias="PROMETHEUS_URL"
    )

    # App
    host: str = "0.0.0.0"
    port: int = 8090
    log_level: str = "INFO"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


settings = Settings()
