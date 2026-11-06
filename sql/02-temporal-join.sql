-- ============================================================================
-- 02-temporal-join.sql — Temporal Join + Enrichment Pipeline
-- ============================================================================
-- Pattern: FOR SYSTEM_TIME AS OF t.event_ts → point-in-time consistency
-- Watermarks aligned across all dimension tables (10s bounded)
-- Output: transactions_enriched_sink (Kafka Avro)
-- ============================================================================

-- ============================================================================
-- Enriched Transactions View (Temporal Join)
-- ============================================================================
-- Left join transactions with accounts/cards/merchants at event_ts
-- Guarantees: account balance, card status, merchant risk as OF transaction time
-- Handles late-arriving dimension updates via watermark alignment
-- ============================================================================
CREATE TEMPORARY VIEW transactions_enriched AS
SELECT
  t.transaction_id,
  t.account_id,
  t.card_id,
  t.merchant_id,
  t.amount_cents,
  t.currency,
  t.status,
  t.event_ts,
  t.idempotency_key,
  t.metadata,
  -- Account enrichment (temporal)
  a.balance_cents   AS account_balance_cents,
  a.status          AS account_status,
  a.customer_id     AS customer_id,
  -- Card enrichment (temporal)
  c.card_number,
  c.card_brand,
  c.status          AS card_status,
  -- Merchant enrichment (temporal)
  m.mcc             AS merchant_mcc,
  m.merchant_name,
  m.risk_score      AS merchant_risk_score,
  -- Processing timestamp (wall clock)
  CURRENT_TIMESTAMP AS processed_at
FROM transactions t
-- Temporal join: account state AS OF transaction event_ts
LEFT JOIN accounts FOR SYSTEM_TIME AS OF t.event_ts AS a
  ON t.account_id = a.account_id
-- Temporal join: card state AS OF transaction event_ts
LEFT JOIN cards FOR SYSTEM_TIME AS OF t.event_ts AS c
  ON t.card_id = c.card_id
-- Temporal join: merchant state AS OF transaction event_ts
LEFT JOIN merchants FOR SYSTEM_TIME AS OF t.event_ts AS m
  ON t.merchant_id = m.merchant_id
-- Filter: only process valid transactions (not deletes, not failed pre-auth)
WHERE t.status NOT IN ('failed', 'refunded')
  AND t.amount_cents > 0;

-- ============================================================================
-- Sink Enriched Transactions
-- ============================================================================
INSERT INTO transactions_enriched_sink
SELECT
  transaction_id,
  account_id,
  card_id,
  merchant_id,
  amount_cents,
  currency,
  status,
  event_ts,
  idempotency_key,
  metadata,
  account_balance_cents,
  account_status,
  customer_id,
  card_number,
  card_brand,
  card_status,
  merchant_mcc,
  merchant_name,
  merchant_risk_score,
  processed_at
FROM transactions_enriched;

-- ============================================================================
-- Materialized View for Dashboards (optional, requires Flink Table Store or external sink)
-- ============================================================================
-- CREATE TABLE dashboard_tx_summary (
--   window_start   TIMESTAMP_LTZ(3),
--   window_end     TIMESTAMP_LTZ(3),
--   tx_count       BIGINT,
--   total_amount   BIGINT,
--   avg_amount     DOUBLE,
--   unique_cards   BIGINT,
--   unique_merchants BIGINT,
--   PRIMARY KEY (window_start) NOT ENFORCED
-- ) WITH (
--   'connector' = 'upsert-kafka',
--   'topic' = 'desafio.dashboard.tx_summary',
--   ...
-- );
--
-- INSERT INTO dashboard_tx_summary
-- SELECT
--   TUMBLE_START(event_ts, INTERVAL '1' MINUTE) AS window_start,
--   TUMBLE_END(event_ts, INTERVAL '1' MINUTE) AS window_end,
--   COUNT(*) AS tx_count,
--   SUM(amount_cents) AS total_amount,
--   AVG(amount_cents) AS avg_amount,
--   COUNT(DISTINCT card_id) AS unique_cards,
--   COUNT(DISTINCT merchant_id) AS unique_merchants
-- FROM transactions_enriched
-- GROUP BY TUMBLE(event_ts, INTERVAL '1' MINUTE);