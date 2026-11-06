# Desafio Final — Pipeline de Pagamentos

Neon (Postgres) → CDC Debezium → Kafka (Avro) → Flink SQL (Enrichment + Fraude) → Consumer App

Roda local com `docker compose up -d` ou na Confluent Cloud com `./setup.sh` (precisa de `.env` preenchido a partir do `.env.example`).

---

## Arquitetura

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        EXTERNAL SYSTEMS                                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │   Neon      │  │  Confluent  │  │  Schema     │  │  Monitoring │        │
│  │  (Postgres) │  │   Cloud     │  │  Registry   │  │  (Prometheus│        │
│  │             │  │  (Kafka/    │  │  (Avro)     │  │   Grafana)  │        │
│  │             │  │   Flink)    │  │             │  │             │        │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘        │
└─────────│────────────────│────────────────│────────────────│───────────────┘
          │                │                │                │
          ▼                ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         INFRASTRUCTURE LAYER (Docker/Terraform)             │
│  • postgres (compatível Neon)                                               │
│  • kafka + zookeeper (KRaft mode)                                           │
│  • schema-registry                                                          │
│  • connect (Debezium PostgresCdcSourceV2)                                   │
│  • flink-jobmanager + taskmanager                                           │
│  • prometheus + grafana                                                     │
│  • redis (idempotency)                                                      │
└─────────────────────────────────────────────────────────────────────────────┘
          │                │                │                │
          ▼                ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DOMAIN LAYER (Pure Business Logic)                  │
│  Entities: Customer, Account, Card, Merchant, Transaction                   │
│  Value Objects: Money, CardNumber, Email, MCC                               │
│  Domain Events: TransactionCreated, AccountUpdated, FraudDetected           │
│  Domain Services: VelocityRuleEngine, EnrichmentService, IdempotencyService │
└─────────────────────────────────────────────────────────────────────────────┘
```

Diagrama completo em `ARCHITECTURE.md`.

---

## Quick Start (Local)

```bash
# 1. Configure
cp .env.example .env
# edite .env com suas credenciais (ou use defaults locais)

# 2. Suba tudo
docker compose up -d

# 3. Verifique saúde
curl http://localhost:8090/health      # Consumer App
curl http://localhost:8081/overview    # Flink UI
curl http://localhost:9090             # Prometheus
curl http://localhost:3001             # Grafana (admin/admin)

# 4. Seed determinístico (seed=42)
docker compose exec neon-postgres psql -U desafio_pg -d desafio-final -f /seed/seed.sql
# ou: node seed/seed.js

# 5. Deploy Flink SQL (nessa ordem)
docker exec -i desafio-flink-jm /opt/flink/bin/sql-client.sh < sql/01-tables.sql
docker exec -i desafio-flink-jm /opt/flink/bin/sql-client.sh < sql/02-temporal-join.sql
docker exec -i desafio-flink-jm /opt/flink/bin/sql-client.sh < sql/03-fraud-velocity.sql

# 6. Verifique
docker exec -i desafio-flink-jm /opt/flink/bin/sql-client.sh < sql/04-verify.sql
```

---

## Estrutura do Projeto

```
desafio-final/
├── .env.example              # Template de variáveis de ambiente
├── .env                      # Secrets locais (NÃO commitar)
├── docker-compose.yml        # Stack completa local
├── docker-compose.test.yml   # Stack leve para CI
├── Dockerfile.consumer       # Consumer App (multi-stage)
├── requirements.txt          # Python deps
├── flink-conf.yaml           # Flink config (parallelism, checkpoints)
├── ARCHITECTURE.md           # ADR + diagramas
├── sql/
│   ├── 00-schema.sql         # PostgreSQL schema otimizado (partitions, domains, RLS)
│   ├── 01-tables.sql         # Flink Kafka tables (CDC sources + sinks)
│   ├── 02-temporal-join.sql  # Temporal join + enrichment
│   ├── 03-fraud-velocity.sql # Multi-rule fraud (MATCH_RECOGNIZE)
│   └── 04-verify.sql         # Queries de verificação
├── schemas/
│   ├── schema-payment-events-v1.avsc
│   ├── schema-payment-events-v2.avsc
│   └── schema-fraud-alert.avsc
├── connectors/
│   └── cdc.json              # Debezium PostgresCdcSourceV2 config
├── seed/
│   ├── seed.js               # Deterministic seed (LCG, seed=42)
│   └── package.json
├── consumer_app/             # FastAPI Consumer
│   ├── main.py               # App + lifespan + endpoints
│   ├── config.py             # Pydantic Settings
│   ├── models.py             # Domain models (Pydantic)
│   └── services/
│       ├── idempotency.py    # Redis SETNX + TTL
│       ├── kafka_gateway.py  # AIOKafka + Avro
│       └── metrics.py        # Prometheus metrics
├── monitoring/
│   ├── prometheus.yml        # Scrape configs
│   └── rules/pipeline.yml    # Alerting rules
├── scripts/
│   ├── integration_test_full_pipeline.sh
│   ├── cost_report.py
│   └── check_schema_registry_compatibility.py
├── tests/
│   ├── test_compatibility.py # Avro backward compatibility
│   ├── unit/                 # Unit tests
│   └── integration/          # Integration tests
├── evidences/
│   ├── 01-envelopescdc.json  # CDC envelope samples
│   └── 02-fraud-alert.json   # Fraud alert sample
└── .github/workflows/
    └── ci-cd.yml             # GitHub Actions CI/CD
```

---

## 1. Fundação (Identity & Network)

| Item | Configuração |
|------|--------------|
| Environment | `desafio-final` (Confluent Cloud) |
| Cluster Kafka | Basic `desafio-basic` (aws/us-east-1) |
| Service Accounts | `desafio-producer` (WRITE `desafio.*`), `desafio-consumer` (READ `desafio.*` + GROUP `desafio.*`) |
| Network | VPC Peering / PrivateLink para Neon |
| Secrets | Zero no código — Confluent Cloud Secrets / AWS Secrets Manager |

**Evidência:**
```bash
confluent iam service-account list
confluent iam access-control list --service-account desafio-producer
confluent iam access-control list --service-account desafio-consumer
```

---

## 2. Contrato de Dados (Avro + Schema Registry)

- **Namespace**: `com.bootcamp.payments.*`
- **Money**: `bytes` (int64 cents) + `decimal(15,2)` string — evita floating point
- **PII/PCI Tags**: `@tags` em `card_number`, `customer_email`, `metadata`
- **Evolução**: BACKWARD compatibility (testado em `tests/test_compatibility.py`)

| Schema | Subject | Compatibility |
|--------|---------|---------------|
| `schema-payment-events-v1.avsc` | `desafio.payments.transactions-value` | BACKWARD |
| `schema-payment-events-v2.avsc` | `desafio.payments.transactions-value` | BACKWARD |
| `schema-fraud-alert.avsc` | `desafio.payments.fraud_alerts-value` | BACKWARD |

**Testes de compatibilidade:**
```bash
python tests/test_compatibility.py
# ACCEPT: novo campo opcional com default (tx_currency_code → USD)
# REJECT: remover campo obrigatório (amount_cents ausente → erro)
```

---

## 3. Ingestão (CDC Debezium)

- **Connector**: `PostgresCdcSourceV2` (Debezium 2.5+)
- **User**: `cdc_user` (least privilege: `REPLICATION`, `SELECT` on public)
- **Config**: `heartbeat.interval.ms=60000`, `slot.name=desafio_cdc_slot`
- **Slot**: `desafio_cdc_slot` (replication slot dedicado)

**Seed Determinístico (seed=42):**

| Entidade | Count |
|----------|-------|
| customers | 200 |
| accounts | 240 |
| cards | 320 |
| merchants | 60 |
| transactions | 2.000 |

**Tipos de evento CDC:**
- `op=c` (INSERT): `before=null`, `after=nova linha`
- `op=u` (UPDATE): `before=estado anterior`, `after=estado novo`
- `op=d` (DELETE): `after=null`, tombstone na sequência

Ver: `evidences/01-envelopescdc.json`

---

## 4. Processamento (Flink SQL)

### 01-tables.sql — Kafka Tables (CDC Sources + Sinks)
- Watermarks: `event_ts - INTERVAL '10' SECOND` (bounded out-of-orderness)
- PK `NOT ENFORCED` em ambos os lados (transações + dimensões)
- Formato: `debezium-avro-avro` com Schema Registry

### 02-temporal-join.sql — Point-in-Time Enrichment
```sql
LEFT JOIN accounts FOR SYSTEM_TIME AS OF t.event_ts AS a
  ON t.account_id = a.account_id
```
- Garante: saldo da conta, status do cartão, risk do merchant **no momento da transação**
- Watermarks alinhados (10s) em todas as tabelas de dimensão
- Output: `desafio.payments.transactions.enriched` (Avro)

### 03-fraud-velocity.sql — Multi-Rule Fraud Detection
| Regra | Padrão | Severidade |
|-------|--------|------------|
| `RULE_VELOCITY` | `PATTERN (T{3,}) WITHIN INTERVAL '60' SECOND PARTITION BY card_id` | high |
| `RULE_HIGH_RISK_MERCHANT` | Single tx at `risk_score >= 80` | high/critical |
| `RULE_AMOUNT_ANOMALY` | Amount > 3x rolling avg (last 10 tx) | medium/high/critical |
| `RULE_GEO_VELOCITY` | Impossible travel (placeholder) | — |

Output: `desafio.payments.fraud_alerts` (Avro)

Ver alerta exemplo: `evidences/02-fraud-alert.json`

### 04-verify.sql — Verification Queries
- Enriched transactions sample
- Fraud alerts por regra/severidade
- Data quality: duplicatas, null enrichments, processamento travado
- Performance: throughput, latência end-to-end (p50/p95/p99)

---

## 5. Operação

**Observabilidade:** Prometheus + Grafana dashboards; metrics: `consumer_transactions_processed_total`, `consumer_fraud_alerts_received_total`, `consumer_dedup_hits_total`, `consumer_processing_latency_seconds`; alerts: CDC lag > 5min, Flink checkpoint failed, fraud spike, consumer lag > 10k

**Segurança:** least-privilege SAs, PII/PCI tags no schema, TLS em trânsito, secrets fora do código, RLS no Postgres multi-tenancy ready

**Custos:** **US$ 28,47/mês** (Confluent Basic + Neon Basic, 7 dias); alavancas: tiered storage (-40%), Flink autoscale (-50%), spot instances, retention 3d para topics não-críticos — ver `scripts/cost_report.py`

**Confiabilidade:** Exactly-once: Flink checkpointing (10s) + Kafka transactional producer + consumer dedup (Redis SETNX TTL 24h); idempotency key por `transaction_id`; DLQ para falhas de deserialização; restart from checkpoint

---

## Consumer App (FastAPI + AIOKafka + Redis + Prometheus)

Endpoints:
- `GET /health` — Health check (Kafka, Redis, Schema Registry)
- `GET /metrics` — Prometheus exposition
- `GET /metrics/snapshot` — JSON snapshot para dashboards
- `POST /admin/reprocess/{topic}` — Reprocess DLQ (manual)

Features:
- **Idempotency**: Redis `SET key NX EX 86400` — exatamente-uma-vez no consumer
- **Avro Deserialization**: Schema Registry client
- **DLQ**: Failed messages → `{topic}.dlq` com error context
- **Structured Logging**: structlog JSON
- **Multi-stage Docker**: non-root, minimal runtime

---

## CI/CD (GitHub Actions)

Pipeline: `.github/workflows/ci-cd.yml`

| Stage | Jobs |
|-------|------|
| **Lint** | Ruff, MyPy, Pre-commit |
| **Schema** | Avro syntax + backward compatibility tests |
| **Test** | Unit + Integration (docker-compose.test.yml) |
| **Build** | Multi-arch Docker → GHCR + Docker Hub |
| **Deploy Staging** | Auto on `develop` branch (K8s) |
| **Deploy Production** | Manual approval on `main` (Blue/Green) |
| **Nightly** | Full integration test against Confluent Cloud |

---

## Custo Total e Teardown

**Custo estimado: US$ 28,47/mês** (Confluent Basic + Neon Basic, 7 dias de retenção)

```bash
# Gerar relatório detalhado
python scripts/cost_report.py --env basic --days 30

# Teardown completo (Confluent Cloud)
./teardown.sh

# Verificação de limpeza
confluent cluster list            → None found.
confluent connect cluster list   → None found.
confluent flink statement list   → None found.
TEARDOWN_OK
```

---

## Referências

- [ARCHITECTURE.md](ARCHITECTURE.md) — ADR completo + diagramas
- [sql/00-schema.sql](sql/00-schema.sql) — PostgreSQL schema otimizado
- [monitoring/rules/pipeline.yml](monitoring/rules/pipeline.yml) — Alerting rules
- [scripts/cost_report.py](scripts/cost_report.py) — Cost modeling
- [consumer_app/](consumer_app/) — Consumer app source