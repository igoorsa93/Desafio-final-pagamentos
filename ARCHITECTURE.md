# Architecture Decision Record — Pipeline de Pagamentos

**Status**: Accepted  
**Date**: 2026-11-06  
**Deciders**: Igor Cardoso  
**Tags**: streaming, cdc, fraud-detection, flink, kafka, postgres

---

## Context

Pipeline de pagamentos end-to-end: **Neon (Postgres) → CDC Debezium → Kafka (Avro) → Flink SQL (enrichment + fraud) → Consumer**.

Requisitos não-funcionais:
- **Throughput**: 2.000 tx/seed, escala para 10k+/s
- **Latência**: < 5s end-to-end (CDC → alerta)
- **Confiabilidade**: exactly-once semantics, idempotência no consumer
- **Observabilidade**: metrics, logs, traces
- **Segurança**: least-privilege, PII/PCI tags, secrets management
- **Custo**: otimizado para Confluent Basic + Neon Basic

---

## Decision: Arquitetura em Camadas (Clean Architecture + Event-Driven)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        EXTERNAL SYSTEMS                                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │   Neon      │  │  Confluent  │  │  Schema     │  │  Monitoring │        │
│  │  (Postgres) │  │   Cloud     │  │  Registry   │  │  (Datadog/  │        │
│  │             │  │  (Kafka/    │  │  (Avro)     │  │   Prometheus)       │
│  │             │  │   Flink)    │  │             │  │             │        │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘        │
└─────────│────────────────│────────────────│────────────────│───────────────┘
          │                │                │                │
          ▼                ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         INFRASTRUCTURE LAYER                                │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  DOCKER COMPOSE (local) / TERRAFORM (cloud)                        │   │
│  │  • postgres (Neon-compatible)                                       │   │
│  │  • kafka + zookeeper (KRaft mode)                                   │   │
│  │  • schema-registry                                                  │   │
│  │  • connect (Debezium PostgresCdcSourceV2)                          │   │
│  │  • flink-jobmanager + taskmanager                                   │   │
│  │  • prometheus + grafana                                             │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
          │                │                │                │
          ▼                ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DOMAIN LAYER (Pure Business Logic)                  │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────┐ │
│  │  Entities       │  │  Value Objects  │  │  Domain Events              │ │
│  │  • Customer     │  │  • Money        │  │  • TransactionCreated       │ │
│  │  • Account      │  │  • CardNumber   │  │  • AccountUpdated           │ │
│  │  • Card         │  │  • Email        │  │  • FraudDetected            │ │
│  │  • Merchant     │  │  • MCC          │  │  • VelocityRuleTriggered    │ │
│  │  • Transaction  │  └─────────────────┘  └─────────────────────────────┘ │
│  └─────────────────┘                                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Domain Services (Pure Functions)                                   │   │
│  │  • VelocityRuleEngine     → MATCH_RECOGNIZE pattern                 │   │
│  │  • EnrichmentService      → Temporal Join logic                     │   │
│  │  • IdempotencyService     → Deduplication by transaction_id         │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
          │                │                │                │
          ▼                ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         APPLICATION LAYER (Use Cases)                       │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Use Cases (Orchestration)                                          │   │
│  │  • ProcessTransactionUseCase    → CDC event → enrich → detect       │   │
│  │  • DetectFraudUseCase           → Velocity rule evaluation          │   │
│  │  • EnrichTransactionUseCase     → Account lookup (temporal)         │   │
│  │  • SeedDatabaseUseCase          → Deterministic seed (seed=42)      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
          │                │                │                │
          ▼                ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         INTERFACE ADAPTERS                                  │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────┐ │
│  │  Controllers    │  │  Gateways       │  │  Presenters                 │ │
│  │  • CDCHandler   │  │  • KafkaGateway │  │  • AvroSerializer           │ │
│  │  • FraudAlert   │  │  │  • FlinkSQL   │  │  • JSONSerializer           │ │
│  │    Webhook      │  │    Gateway      │  │  • MetricsExporter          │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Component Decisions

### 1. CDC Source: Debezium PostgresCdcSourceV2
- **Why**: Native Postgres logical replication, exactly-once, schema evolution support
- **Config**: `heartbeat.interval.ms=60000`, `slot.name=desafio_cdc_slot`
- **User**: `cdc_user` (least privilege: `REPLICATION`, `SELECT` on public schema only)

### 2. Message Format: Avro + Schema Registry
- **Namespace**: `com.bootcamp.payments.*`
- **Money**: `bytes` (int64 cents) + `decimal(15,2)` string — evita floating point
- **PII/PCI Tags**: `@tags` em campos sensíveis (`card_number`, `customer_email`)
- **Evolution**: BACKWARD compatibility (testado em `tests/test_compatibility.py`)

### 3. Stream Processing: Flink SQL
- **Temporal Join**: `FOR SYSTEM_TIME AS OF t.event_ts` — point-in-time consistency
- **Fraud Detection**: `MATCH_RECOGNIZE PATTERN (T{3,}) WITHIN INTERVAL '60' SECOND PARTITION BY card_id`
- **Idle Timeout**: `sql.tables.scan.idle-timeout=5s` — evita watermarks travados
- **Parallelism**: 2 (configurável via `FLINK_PARALLELISM`)

### 4. Idempotency Strategy
- **Key**: `transaction_id` (PK em `transactions` topic)
- **Consumer**: Dedupe via Redis SETNX com TTL 24h + checkpoint Flink
- **Exactly-once**: Flink checkpointing (10s interval) + Kafka transactional producer

### 5. Observability Stack
- **Metrics**: Prometheus (JMX exporter para Kafka/Flink/Connect) + Grafana dashboards
- **Logs**: Structured JSON → Loki
- **Traces**: OpenTelemetry → Tempo (opcional)
- **Alerts**: PrometheusRule para lag > 10k, fraud rate spike, CDC lag

---

## Data Flow (Event Sourcing)

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Neon       │     │   Kafka      │     │   Flink      │     │   Consumer   │
│  (Postgres)  │────▶│  Connect     │────▶│   SQL        │────▶│  (App/Alert) │
│              │     │  (Debezium)  │     │  (Enrich+    │     │              │
│  • INSERT    │     │              │     │   Fraud)     │     │  • Enriched  │
│  • UPDATE    │     │  Topics:     │     │              │     │  • Fraud     │
│  • DELETE    │     │  desafio.    │     │  • Temporal  │     │    Alerts    │
│              │     │  payments.*  │     │    Join      │     │  • Metrics   │
└──────────────┘     └──────────────┘     │  • MATCH_    │     └──────────────┘
                                           │    RECOGNIZE │
                                           └──────────────┘
```

---

## Security Model (Zero Trust)

| Component | Identity | Permissions | Network |
|-----------|----------|-------------|---------|
| Neon CDC | `cdc_user` | `REPLICATION`, `SELECT` | VPC peering / PrivateLink |
| Kafka Connect | `desafio-producer` | WRITE `desafio.*` | Private subnet |
| Flink | `desafio-flink` | READ `desafio.*`, WRITE `desafio.fraud.*` | Private subnet |
| Consumer | `desafio-consumer` | READ `desafio.*`, GROUP `desafio.*` | Private subnet |
| Schema Registry | Internal | READ/WRITE schemas | Private subnet |

**Secrets**: Zero secrets em código — `.env` apenas para setup local; produção usa Confluent Cloud secrets / AWS Secrets Manager.

---

## Error Handling & Reliability

| Failure Mode | Detection | Recovery |
|--------------|-----------|----------|
| CDC lag > 5min | Prometheus alert | Auto-restart connector, backfill via snapshot |
| Flink job fail | Checkpoint failure metric | Restart from last checkpoint (exactly-once) |
| Schema incompatibility | Registry compatibility check | Block deploy, run `test_compatibility.py` |
| Consumer crash | Lag alert | Restart consumer, dedupe via Redis |
| Network partition | Health check failure | Circuit breaker, retry with exponential backoff |

---

## Cost Optimization Levers

1. **Confluent Basic → Spot instances** (quando disponível): -40% compute
2. **Topic retention**: 7 dias (padrão) → 3 dias para topics não-críticos
3. **Partition count**: 3 (seed) → auto-scale via Flink parallelism
4. **Flink parallelism**: 2 → 1 fora de horário comercial (cron)
5. **Neon**: Auto-suspend após 5min idle (Basic tier)

---

## Migration Path (Future)

| Phase | Target | Effort |
|-------|--------|--------|
| 1 | Confluent Cloud Dedicated | Low (config only) |
| 2 | Flink Managed (AWS KDA / GCP Dataflow) | Medium (SQL port) |
| 3 | TimescaleDB (Neon) para hypertables | Medium (DDL) |
| 4 | Temporal.io para workflow orchestration | High (refactor) |

---

## References

- [Debezium Postgres Connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- [Flink SQL MATCH_RECOGNIZE](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/queries/match_recognize/)
- [Confluent Schema Registry Compatibility](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html)
- [Clean Architecture](https://blog.cleancoder.com/uncle-bob/2012/08/13/the-clean-architecture.html)