-- ============================================================================
-- Flink SQL — Pipeline de Pagamentos (Production Grade)
-- Source: senior-architect + postgresql + workflow-automation skills
-- ============================================================================
-- Run order: 01-tables.sql → 02-temporal-join.sql → 03-fraud-velocity.sql → 04-verify.sql
-- Config: flink-conf.yaml (parallelism, idle-timeout, checkpointing)
-- ============================================================================

-- ============================================================================
-- 01-tables.sql — Kafka Source/Sink Tables (CDC Debezium Avro)
-- ============================================================================

-- Transactions topic (CDC from Neon Postgres via Debezium)
-- Debezium envelope: {before, after, source, op, ts_ms, transaction}
-- We extract 'after' fields as the canonical record
CREATE TABLE transactions (
  transaction_id  BIGINT,
  account_id      BIGINT,
  card_id         BIGINT,
  merchant_id     BIGINT,
  amount_cents    BIGINT,
  currency        STRING,
  status          STRING,
  event_ts        TIMESTAMP_LTZ(3),      -- business time (from DB)
  idempotency_key STRING,
  metadata        STRING,                -- JSONB as STRING for flexibility
  -- Watermark for event-time processing (bounded out-of-orderness: 10s)
  WATERMARK FOR event_ts AS event_ts - INTERVAL '10' SECOND,
  PRIMARY KEY (transaction_id) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'desafio.payments.transactions',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'properties.group.id'          = 'desafio-flink-tx',
  'format'                       = 'debezium-avro-avro',
  'value.topic.format.version'   = '2.5',
  'debezium-avro-avro.schema-registry.url' = '${SCHEMA_REGISTRY_URL}',
  'scan.startup.mode'            = 'earliest',
  'json.ignore-parse-errors'     = 'true',
  'scan.bounded.mode'            = 'unbounded'
);

-- Accounts topic (CDC) — dimension table for temporal join
CREATE TABLE accounts (
  account_id      BIGINT,
  customer_id     BIGINT,
  balance_cents   BIGINT,
  status          STRING,
  version         BIGINT,
  updated_at      TIMESTAMP_LTZ(3),
  -- Watermark for temporal join alignment
  WATERMARK FOR updated_at AS updated_at - INTERVAL '10' SECOND,
  PRIMARY KEY (account_id) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'desafio.payments.accounts',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'properties.group.id'          = 'desafio-flink-acct',
  'format'                       = 'debezium-avro-avro',
  'value.topic.format.version'   = '2.5',
  'debezium-avro-avro.schema-registry.url' = '${SCHEMA_REGISTRY_URL}',
  'scan.startup.mode'            = 'earliest'
);

-- Cards topic (CDC) — dimension table for enrichment
CREATE TABLE cards (
  card_id         BIGINT,
  account_id      BIGINT,
  card_number     STRING,       -- masked: ****1234
  card_brand      STRING,
  status          STRING,
  expires_at      DATE,
  updated_at      TIMESTAMP_LTZ(3),
  WATERMARK FOR updated_at AS updated_at - INTERVAL '10' SECOND,
  PRIMARY KEY (card_id) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'desafio.payments.cards',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'properties.group.id'          = 'desafio-flink-cards',
  'format'                       = 'debezium-avro-avro',
  'value.topic.format.version'   = '2.5',
  'debezium-avro-avro.schema-registry.url' = '${SCHEMA_REGISTRY_URL}',
  'scan.startup.mode'            = 'earliest'
);

-- Merchants topic (CDC) — dimension table for enrichment + risk scoring
CREATE TABLE merchants (
  merchant_id     BIGINT,
  mcc             STRING,
  merchant_name   STRING,
  risk_score      INT,
  updated_at      TIMESTAMP_LTZ(3),
  WATERMARK FOR updated_at AS updated_at - INTERVAL '10' SECOND,
  PRIMARY KEY (merchant_id) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'desafio.payments.merchants',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'properties.group.id'          = 'desafio-flink-merchants',
  'format'                       = 'debezium-avro-avro',
  'value.topic.format.version'   = '2.5',
  'debezium-avro-avro.schema-registry.url' = '${SCHEMA_REGISTRY_URL}',
  'scan.startup.mode'            = 'earliest'
);

-- ============================================================================
-- SINK: Enriched Transactions (output to Kafka for consumers)
-- ============================================================================
CREATE TABLE transactions_enriched_sink (
  transaction_id          BIGINT,
  account_id              BIGINT,
  card_id                 BIGINT,
  merchant_id             BIGINT,
  amount_cents            BIGINT,
  currency                STRING,
  status                  STRING,
  event_ts                TIMESTAMP_LTZ(3),
  idempotency_key         STRING,
  metadata                STRING,
  -- Enriched fields
  account_balance_cents   BIGINT,
  account_status          STRING,
  customer_id             BIGINT,
  card_number             STRING,
  card_brand              STRING,
  card_status             STRING,
  merchant_mcc            STRING,
  merchant_name           STRING,
  merchant_risk_score     INT,
  -- Processing metadata
  processed_at            TIMESTAMP_LTZ(3),
  PRIMARY KEY (transaction_id) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'desafio.payments.transactions.enriched',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'properties.group.id'          = 'desafio-flink-enriched-sink',
  'format'                       = 'avro',
  'avro.schema-registry.url'     = '${SCHEMA_REGISTRY_URL}',
  'key.format'                   = 'avro',
  'key.avro.schema-registry.url' = '${SCHEMA_REGISTRY_URL}',
  'sink.partitioner'             = 'fixed',
  'sink.partitioner-field-delimiter' = '|',
  'key.fields'                   = 'transaction_id'
);

-- ============================================================================
-- SINK: Fraud Alerts (output to Kafka)
-- ============================================================================
CREATE TABLE fraud_alerts_sink (
  alert_id         STRING,
  rule_name        STRING,
  severity         STRING,
  card_id          BIGINT,
  account_id       BIGINT,
  transaction_ids  ARRAY<BIGINT>,
  hit_count        INT,
  total_amount_cents BIGINT,
  details          STRING,         -- JSON with match details
  detected_at      TIMESTAMP_LTZ(3),
  PRIMARY KEY (alert_id) NOT ENFORCED
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'desafio.payments.fraud_alerts',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'properties.group.id'          = 'desafio-flink-fraud-sink',
  'format'                       = 'avro',
  'avro.schema-registry.url'     = '${SCHEMA_REGISTRY_URL}',
  'key.format'                   = 'avro',
  'key.avro.schema-registry.url' = '${SCHEMA_REGISTRY_URL}',
  'key.fields'                   = 'alert_id'
);