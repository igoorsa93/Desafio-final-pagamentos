-- ============================================================================
-- 04-verify.sql — Verification Queries (Run in Flink SQL CLI)
-- ============================================================================
-- Use: docker exec -it desafio-flink-jm /opt/flink/bin/sql-client.sh
-- Then: SOURCE '/sql/04-verify.sql';
-- ============================================================================

-- ============================================================================
-- 1. Verify Source Tables (check CDC data flowing)
-- ============================================================================
-- Raw transactions from Kafka (Debezium envelope)
SELECT * FROM transactions LIMIT 10;

-- Check watermark generation
SELECT
  transaction_id,
  event_ts,
  CURRENT_WATERMARK(event_ts) AS watermark
FROM transactions LIMIT 5;

-- ============================================================================
-- 2. Verify Temporal Join Results
-- ============================================================================
-- Enriched transactions (check joins working)
SELECT
  transaction_id,
  amount_cents,
  account_balance_cents,
  account_status,
  card_number,
  card_brand,
  merchant_name,
  merchant_risk_score
FROM transactions_enriched
WHERE card_id = 1001
LIMIT 20;

-- Check for null enrichments (data quality)
SELECT
  COUNT(*) AS total,
  SUM(CASE WHEN account_balance_cents IS NULL THEN 1 ELSE 0 END) AS missing_account,
  SUM(CASE WHEN card_number IS NULL THEN 1 ELSE 0 END) AS missing_card,
  SUM(CASE WHEN merchant_name IS NULL THEN 1 ELSE 0 END) AS missing_merchant
FROM transactions_enriched;

-- ============================================================================
-- 3. Verify Fraud Detection
-- ============================================================================
-- Recent fraud alerts
SELECT
  alert_id,
  rule_name,
  severity,
  card_id,
  hit_count,
  total_amount_cents,
  detected_at
FROM fraud_alerts_sink
ORDER BY detected_at DESC
LIMIT 20;

-- Fraud alerts by rule
SELECT
  rule_name,
  severity,
  COUNT(*) AS alert_count,
  SUM(hit_count) AS total_hits,
  SUM(total_amount_cents) AS total_amount_cents
FROM fraud_alerts_sink
GROUP BY rule_name, severity
ORDER BY alert_count DESC;

-- ============================================================================
-- 4. Verify Sink Topics (Kafka consumer check)
-- ============================================================================
-- Enriched transactions sink
CREATE TABLE enriched_verify (
  transaction_id BIGINT,
  amount_cents BIGINT,
  account_balance_cents BIGINT,
  merchant_name STRING,
  processed_at TIMESTAMP_LTZ(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'desafio.payments.transactions.enriched',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'properties.group.id' = 'desafio-verify-enriched',
  'format' = 'avro',
  'avro.schema-registry.url' = '${SCHEMA_REGISTRY_URL}',
  'scan.startup.mode' = 'earliest'
);

SELECT * FROM enriched_verify LIMIT 10;

-- Fraud alerts sink
CREATE TABLE fraud_verify (
  alert_id STRING,
  rule_name STRING,
  severity STRING,
  card_id BIGINT,
  hit_count INT,
  detected_at TIMESTAMP_LTZ(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'desafio.payments.fraud_alerts',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'properties.group.id' = 'desafio-verify-fraud',
  'format' = 'avro',
  'avro.schema-registry.url' = '${SCHEMA_REGISTRY_URL}',
  'scan.startup.mode' = 'earliest'
);

SELECT * FROM fraud_verify LIMIT 20;

-- ============================================================================
-- 5. Data Quality Checks
-- ============================================================================
-- Duplicate transaction_ids in enriched sink (should be 0)
SELECT
  transaction_id,
  COUNT(*) AS cnt
FROM enriched_verify
GROUP BY transaction_id
HAVING COUNT(*) > 1;

-- Orphan transactions (no account join)
SELECT COUNT(*) FROM transactions_enriched WHERE account_balance_cents IS NULL;

-- Stalled processing (no new events in 5 min)
SELECT
  MAX(event_ts) AS last_event,
  CURRENT_TIMESTAMP - MAX(event_ts) AS lag
FROM transactions_enriched;

-- ============================================================================
-- 6. Performance Metrics
-- ============================================================================
-- Throughput (tx/min)
SELECT
  TUMBLE_START(event_ts, INTERVAL '1' MINUTE) AS window_start,
  COUNT(*) AS tx_per_min
FROM transactions_enriched
GROUP BY TUMBLE(event_ts, INTERVAL '1' MINUTE)
ORDER BY window_start DESC
LIMIT 10;

-- End-to-end latency (event_ts to processed_at)
SELECT
  AVG(processed_at - event_ts) AS avg_latency_ms,
  PERCENTILE(processed_at - event_ts, 0.5) AS p50_latency_ms,
  PERCENTILE(processed_at - event_ts, 0.95) AS p95_latency_ms,
  PERCENTILE(processed_at - event_ts, 0.99) AS p99_latency_ms
FROM enriched_verify
WHERE processed_at > CURRENT_TIMESTAMP - INTERVAL '10' MINUTE;