-- ============================================================================
-- PostgreSQL Schema — Pipeline de Pagamentos (Neon/PostgreSQL 15+)
-- Otimizado seguindo postgresql skill: tipos corretos, constraints, indexes, partitioning
-- ============================================================================

-- Extensions necessárias
CREATE EXTENSION IF NOT EXISTS "pgcrypto";      -- gen_random_uuid(), crypt()
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";     -- uuid_generate_v4() fallback

-- ============================================================================
-- ENUMS (pequenos, estáveis)
-- ============================================================================
CREATE TYPE account_status AS ENUM ('active', 'frozen', 'closed');
CREATE TYPE card_status AS ENUM ('active', 'expired', 'blocked', 'cancelled');
CREATE TYPE transaction_status AS ENUM ('pending', 'authorized', 'settled', 'failed', 'refunded');

-- ============================================================================
-- DOMAIN TYPES (reutilizáveis, validados)
-- ============================================================================
CREATE DOMAIN money_cents AS BIGINT
  CHECK (VALUE >= 0);

CREATE DOMAIN money_decimal AS NUMERIC(15,2)
  CHECK (VALUE >= 0);

CREATE DOMAIN email_addr AS TEXT
  CHECK (VALUE ~* '^[^@\s]+@[^@\s]+\.[^@\s]+$');

CREATE DOMAIN mcc_code AS CHAR(4)
  CHECK (VALUE ~ '^\d{4}$');

CREATE DOMAIN card_number_masked AS TEXT
  CHECK (VALUE ~ '^\*{4}\d{4}$');  -- ex: ****1234

-- ============================================================================
-- CORE TABLES
-- ============================================================================

-- customers: identidade do cliente
CREATE TABLE customers (
  customer_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  customer_name   TEXT NOT NULL,
  customer_email  email_addr NOT NULL UNIQUE,
  customer_segment TEXT NOT NULL CHECK (customer_segment IN ('retail','food','fuel','health','other')),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_customers_segment ON customers (customer_segment);
CREATE INDEX idx_customers_created_at ON customers (created_at);

-- accounts: conta financeira do cliente
CREATE TABLE accounts (
  account_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  customer_id     BIGINT NOT NULL REFERENCES customers(customer_id) ON DELETE RESTRICT,
  balance_cents   money_cents NOT NULL DEFAULT 0,
  status          account_status NOT NULL DEFAULT 'active',
  version         BIGINT NOT NULL DEFAULT 1,          -- optimistic locking
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_accounts_customer_id ON accounts (customer_id);
CREATE INDEX idx_accounts_status ON accounts (status);
CREATE INDEX idx_accounts_balance ON accounts (balance_cents) WHERE status = 'active';

-- cards: cartões vinculados à conta
CREATE TABLE cards (
  card_id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  account_id      BIGINT NOT NULL REFERENCES accounts(account_id) ON DELETE RESTRICT,
  card_number     card_number_masked NOT NULL,        -- PCI: apenas últimos 4 dígitos
  card_brand      TEXT NOT NULL CHECK (card_brand IN ('visa','mastercard','amex','elo','other')),
  status          card_status NOT NULL DEFAULT 'active',
  expires_at      DATE NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_cards_account_id ON cards (account_id);
CREATE INDEX idx_cards_status ON cards (status);
CREATE INDEX idx_cards_expires_at ON cards (expires_at) WHERE status = 'active';

-- merchants: estabelecimentos
CREATE TABLE merchants (
  merchant_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  mcc             mcc_code NOT NULL,
  merchant_name   TEXT NOT NULL,
  risk_score      SMALLINT NOT NULL DEFAULT 0 CHECK (risk_score BETWEEN 0 AND 100),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_merchants_mcc ON merchants (mcc);
CREATE INDEX idx_merchants_risk ON merchants (risk_score);

-- transactions: evento principal (append-only, alta escrita)
-- PARTITION BY RANGE (event_ts) — TimescaleDB automatizaria; aqui manual para Neon Basic
CREATE TABLE transactions (
  transaction_id  BIGINT GENERATED ALWAYS AS IDENTITY,
  account_id      BIGINT NOT NULL REFERENCES accounts(account_id) ON DELETE RESTRICT,
  card_id         BIGINT NOT NULL REFERENCES cards(card_id) ON DELETE RESTRICT,
  merchant_id     BIGINT NOT NULL REFERENCES merchants(merchant_id) ON DELETE RESTRICT,
  amount_cents    money_cents NOT NULL,
  amount_decimal  money_decimal GENERATED ALWAYS AS (amount_cents / 100.0) STORED,
  currency        CHAR(3) NOT NULL DEFAULT 'USD' CHECK (currency ~ '^[A-Z]{3}$'),
  status          transaction_status NOT NULL DEFAULT 'pending',
  event_ts        TIMESTAMPTZ NOT NULL,               -- timestamp do evento (business time)
  idempotency_key UUID NOT NULL DEFAULT gen_random_uuid(),  -- dedup chave
  metadata        JSONB NOT NULL DEFAULT '{}',        -- extensível
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(), -- timestamp de ingestão (system time)
  
  PRIMARY KEY (transaction_id, event_ts)              -- PK composta para partitioning
) PARTITION BY RANGE (event_ts);

-- Partitions mensais (exemplo: 2026-11, 2026-12, etc.)
-- Criar via script de manutenção ou pg_partman
-- CREATE TABLE transactions_2026_11 PARTITION OF transactions
--   FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');

-- Indexes para access paths reais
CREATE INDEX idx_transactions_account_id ON transactions (account_id);
CREATE INDEX idx_transactions_card_id ON transactions (card_id);
CREATE INDEX idx_transactions_merchant_id ON transactions (merchant_id);
CREATE INDEX idx_transactions_event_ts ON transactions (event_ts DESC);
CREATE INDEX idx_transactions_status ON transactions (status) WHERE status IN ('pending','authorized');
CREATE UNIQUE INDEX idx_transactions_idempotency_key ON transactions (idempotency_key);
-- Partial index para fraud detection (card_id + event_ts window)
CREATE INDEX idx_transactions_card_event_ts ON transactions (card_id, event_ts DESC);

-- ============================================================================
-- CDC / AUDIT TABLES (para Debezium capturar mudanças completas)
-- ============================================================================

-- REPLICA IDENTITY FULL nas tabelas de domínio (já é default para PK)
-- ALTER TABLE customers REPLICA IDENTITY FULL;
-- ALTER TABLE accounts REPLICA IDENTITY FULL;
-- ALTER TABLE cards REPLICA IDENTITY FULL;
-- ALTER TABLE merchants REPLICA IDENTITY FULL;
-- ALTER TABLE transactions REPLICA IDENTITY FULL;  -- não necessário (append-only)

-- ============================================================================
-- VIEWS PARA CONSUMO (Flink SQL / Consumers)
-- ============================================================================

-- View enriquecida para temporal join (account state at event_ts)
CREATE VIEW v_transactions_enriched AS
SELECT
  t.transaction_id,
  t.account_id,
  t.card_id,
  t.merchant_id,
  t.amount_cents,
  t.amount_decimal,
  t.currency,
  t.status,
  t.event_ts,
  t.idempotency_key,
  t.metadata,
  a.balance_cents AS account_balance_cents,
  a.status AS account_status,
  c.card_number,
  c.card_brand,
  c.status AS card_status,
  m.mcc,
  m.merchant_name,
  m.risk_score AS merchant_risk_score
FROM transactions t
JOIN accounts a ON t.account_id = a.account_id
JOIN cards c ON t.card_id = c.card_id
JOIN merchants m ON t.merchant_id = m.merchant_id;

-- View para fraud detection (janela deslizante)
CREATE VIEW v_velocity_candidates AS
SELECT
  card_id,
  transaction_id,
  merchant_id,
  amount_cents,
  event_ts,
  COUNT(*) OVER (PARTITION BY card_id ORDER BY event_ts
    RANGE BETWEEN INTERVAL '60 seconds' PRECEDING AND CURRENT ROW) AS tx_count_60s
FROM transactions
WHERE status IN ('authorized', 'settled')
  AND event_ts >= now() - INTERVAL '1 hour';  -- janela móvel

-- ============================================================================
-- TRIGGERS PARA updated_at (auto-manutenção)
-- ============================================================================

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;$$;

CREATE TRIGGER trigger_customers_updated_at
  BEFORE UPDATE ON customers FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trigger_accounts_updated_at
  BEFORE UPDATE ON accounts FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trigger_cards_updated_at
  BEFORE UPDATE ON cards FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trigger_merchants_updated_at
  BEFORE UPDATE ON merchants FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- ROW LEVEL SECURITY (multi-tenancy ready)
-- ============================================================================

ALTER TABLE customers ENABLE ROW LEVEL SECURITY;
ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;
ALTER TABLE cards ENABLE ROW LEVEL SECURITY;
ALTER TABLE merchants ENABLE ROW LEVEL SECURITY;
ALTER TABLE transactions ENABLE ROW LEVEL SECURITY;

-- Policy exemplo: usuário só vê seus dados (via current_setting('app.current_user_id'))
CREATE POLICY customer_isolation ON customers
  FOR ALL TO app_user
  USING (customer_id = current_setting('app.current_user_id')::BIGINT);

CREATE POLICY account_isolation ON accounts
  FOR ALL TO app_user
  USING (customer_id = current_setting('app.current_user_id')::BIGINT);

-- ============================================================================
-- GRANTS (least privilege para cdc_user)
-- ============================================================================

-- cdc_user: apenas SELECT + REPLICATION (configurado no Neon)
-- GRANT USAGE ON SCHEMA public TO cdc_user;
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_user;

-- app_user: CRUD nas tabelas de domínio
-- GRANT USAGE ON SCHEMA public TO app_user;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO app_user;

-- ============================================================================
-- COMENTÁRIOS PARA DOCUMENTAÇÃO (Schema Registry / Data Catalog)
-- ============================================================================

COMMENT ON TABLE customers IS 'Clientes do sistema de pagamentos. PII: customer_email.';
COMMENT ON TABLE accounts IS 'Contas financeiras. Saldo em cents (money_cents).';
COMMENT ON TABLE cards IS 'Cartões de pagamento. PCI: card_number mascarado (****1234).';
COMMENT ON TABLE merchants IS 'Estabelecimentos comerciais. MCC + risk_score para fraud.';
COMMENT ON TABLE transactions IS 'Transações de pagamento (append-only). Partitioned by event_ts. Idempotency via idempotency_key.';

COMMENT ON COLUMN transactions.amount_cents IS 'Valor em centavos (int64) — money type seguro.';
COMMENT ON COLUMN transactions.amount_decimal IS 'Generated column: amount_cents / 100.0 como NUMERIC(15,2).';
COMMENT ON COLUMN transactions.idempotency_key IS 'Chave de idempotência UUID v4 para dedup consumer.';
COMMENT ON COLUMN transactions.metadata IS 'JSONB extensível: device_id, ip, geo, channel, etc.';

-- ============================================================================
-- SEED DETERMINÍSTICO (seed=42) — via script Python/Node, não SQL inline
-- Ver: seed/seed.py (LCG determinístico)
-- ============================================================================

-- Counts esperados após seed:
-- customers: 200
-- accounts: 240  
-- cards: 320
-- merchants: 60
-- transactions: 2000