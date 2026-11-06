-- ============================================================================
-- 03-fraud-velocity.sql — Multi-Rule Fraud Detection (MATCH_RECOGNIZE)
-- ============================================================================
-- Rules implemented:
-- 1. RULE_VELOCITY:        3+ tx in 60s per card (classic velocity)
-- 2. RULE_HIGH_RISK_MCC:   Transaction at merchant with risk_score >= 80
-- 3. RULE_AMOUNT_ANOMALY:  Amount > 3x avg last 10 tx per card
-- 4. RULE_GEO_VELOCITY:    Impossible travel (if geo in metadata) — placeholder
-- ============================================================================

-- ============================================================================
-- RULE 1: Velocity — 3+ transactions within 60 seconds per card
-- Pattern: T{3,} WITHIN INTERVAL '60' SECOND PARTITION BY card_id
-- ============================================================================
INSERT INTO fraud_alerts_sink
SELECT
  CAST('VEL-' || mr.card_id || '-' || mr.first_tx_id AS STRING) AS alert_id,
  'RULE_VELOCITY'                               AS rule_name,
  'high'                                        AS severity,
  mr.card_id                                    AS card_id,
  mr.first_account_id                           AS account_id,
  mr.tx_ids                                     AS transaction_ids,
  mr.hit_count                                  AS hit_count,
  mr.total_amount_cents                         AS total_amount_cents,
  CAST(
    MAP[
      'pattern', 'T{3,} within 60s',
      'window_start', CAST(mr.window_start AS STRING),
      'window_end', CAST(mr.window_end AS STRING),
      'merchants', mr.merchant_ids
    ] AS STRING
  ) AS details,
  mr.emitted_at                                 AS detected_at
FROM MATCH_RECOGNIZE (
  PARTITION BY card_id
  ORDER BY event_ts ASC, transaction_id ASC
  MEASURES
    FIRST(transaction_id) AS first_tx_id,
    FIRST(account_id) AS first_account_id,
    COUNT(*) AS hit_count,
    SUM(amount_cents) AS total_amount_cents,
    COLLECT(transaction_id) AS tx_ids,
    COLLECT(merchant_id) AS merchant_ids,
    FIRST(event_ts) AS window_start,
    LAST(event_ts) AS window_end,
    CURRENT_TIMESTAMP AS emitted_at
  ONE ROW PER MATCH
  AFTER MATCH SKIP TO NEXT ROW
  PATTERN (T{3,})
  WITHIN INTERVAL '60' SECOND
  DEFINE
    T AS TRUE  -- any transaction counts
) AS mr
FROM transactions_enriched;

-- ============================================================================
-- RULE 2: High Risk Merchant — Single transaction at risk_score >= 80
-- ============================================================================
INSERT INTO fraud_alerts_sink
SELECT
  CAST('HRM-' || t.transaction_id AS STRING) AS alert_id,
  'RULE_HIGH_RISK_MERCHANT'                  AS rule_name,
  CASE
    WHEN m.risk_score >= 95 THEN 'critical'
    WHEN m.risk_score >= 80 THEN 'high'
    ELSE 'medium'
  END                                        AS severity,
  t.card_id                                  AS card_id,
  t.account_id                               AS account_id,
  ARRAY[t.transaction_id]                    AS transaction_ids,
  1                                          AS hit_count,
  t.amount_cents                             AS total_amount_cents,
  CAST(
    MAP[
      'merchant_id', CAST(t.merchant_id AS STRING),
      'merchant_name', t.merchant_name,
      'mcc', t.merchant_mcc,
      'risk_score', CAST(m.risk_score AS STRING)
    ] AS STRING
  ) AS details,
  CURRENT_TIMESTAMP                          AS detected_at
FROM transactions_enriched t
JOIN merchants FOR SYSTEM_TIME AS OF t.event_ts AS m
  ON t.merchant_id = m.merchant_id
WHERE m.risk_score >= 80
  AND t.status IN ('authorized', 'settled');

-- ============================================================================
-- RULE 3: Amount Anomaly — Transaction > 3x rolling avg (last 10 tx)
-- Uses windowed aggregation + comparison
-- ============================================================================
-- First: compute rolling avg per card (last 10 authorized/settled tx)
CREATE TEMPORARY VIEW card_amount_stats AS
SELECT
  card_id,
  transaction_id,
  amount_cents,
  event_ts,
  AVG(amount_cents) OVER (
    PARTITION BY card_id
    ORDER BY event_ts
    ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING  -- exclude current
  ) AS rolling_avg_cents
FROM transactions_enriched
WHERE status IN ('authorized', 'settled');

-- Then: detect anomalies
INSERT INTO fraud_alerts_sink
SELECT
  CAST('AMT-' || t.transaction_id AS STRING) AS alert_id,
  'RULE_AMOUNT_ANOMALY'                      AS rule_name,
  CASE
    WHEN t.amount_cents > t.rolling_avg_cents * 10 THEN 'critical'
    WHEN t.amount_cents > t.rolling_avg_cents * 5  THEN 'high'
    ELSE 'medium'
  END                                        AS severity,
  t.card_id                                  AS card_id,
  t.account_id                               AS account_id,
  ARRAY[t.transaction_id]                    AS transaction_ids,
  1                                          AS hit_count,
  t.amount_cents                             AS total_amount_cents,
  CAST(
    MAP[
      'amount_cents', CAST(t.amount_cents AS STRING),
      'rolling_avg_cents', CAST(ROUND(t.rolling_avg_cents) AS STRING),
      'ratio', CAST(ROUND(t.amount_cents / NULLIF(t.rolling_avg_cents, 0), 2) AS STRING)
    ] AS STRING
  ) AS details,
  CURRENT_TIMESTAMP                          AS detected_at
FROM card_amount_stats t
WHERE t.rolling_avg_cents IS NOT NULL
  AND t.amount_cents > t.rolling_avg_cents * 3
  AND t.rolling_avg_cents > 0;

-- ============================================================================
-- RULE 4: Geo Velocity (Placeholder — requires geo in metadata)
-- Pattern: tx in country A, then tx in country B within impossible travel time
-- ============================================================================
-- INSERT INTO fraud_alerts_sink
-- SELECT ...
-- FROM (
--   SELECT
--     t.*,
--     JSON_VALUE(t.metadata, '$.geo.country') AS country,
--     LAG(JSON_VALUE(t.metadata, '$.geo.country')) OVER (PARTITION BY t.card_id ORDER BY t.event_ts) AS prev_country,
--     LAG(t.event_ts) OVER (PARTITION BY t.card_id ORDER BY t.event_ts) AS prev_event_ts
--   FROM transactions_enriched t
-- ) t
-- WHERE t.country != t.prev_country
--   AND t.prev_country IS NOT NULL
--   AND t.event_ts < t.prev_event_ts + INTERVAL '2' HOUR  -- impossible travel
--   AND t.status IN ('authorized', 'settled');

-- ============================================================================
-- Summary View: Fraud Alerts per Card (for dashboarding)
-- ============================================================================
-- CREATE TABLE fraud_alerts_summary_sink (
--   window_start   TIMESTAMP_LTZ(3),
--   window_end     TIMESTAMP_LTZ(3),
--   card_id        BIGINT,
--   rule_name      STRING,
--   alert_count    BIGINT,
--   max_severity   STRING,
--   PRIMARY KEY (window_start, card_id, rule_name) NOT ENFORCED
-- ) WITH (
--   'connector' = 'upsert-kafka',
--   'topic' = 'desafio.dashboard.fraud_summary',
--   ...
-- );
--
-- INSERT INTO fraud_alerts_summary_sink
-- SELECT
--   TUMBLE_START(detected_at, INTERVAL '5' MINUTE) AS window_start,
--   TUMBLE_END(detected_at, INTERVAL '5' MINUTE) AS window_end,
--   card_id,
--   rule_name,
--   COUNT(*) AS alert_count,
--   MAX(severity) AS max_severity
-- FROM fraud_alerts_sink
-- GROUP BY TUMBLE(detected_at, INTERVAL '5' MINUTE), card_id, rule_name;