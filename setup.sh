#!/usr/bin/env bash
# setup.sh — Desafio Final: Neon -> CDC -> Kafka -> Flink -> consumo
# Pré-requisito: .env preenchido a partir de .env.example
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

export $(grep -v '^#' .env | xargs)

echo "== [1/7] Environment: desafio-final + cluster Basic desafio-basic (aws/us-east-1)"
confluent cluster create --name desafio-basic --provider aws --region us-east-1 --tier basic

echo "== [2/7] Neon: Basic cluster + database desafio-final + cdc_user (menor privilegio)"
# Neon console: create project/db; GRANT SELECT,INSERT,UPDATE,DELETE ON desafio.* TO cdc_user;

echo "== [3/7] Kafka Connect: PostgresCdcSourceV2 -> topic prefix desafio.payments"
confluent connector create \
  --name postgres-cdc-desafio \
  --connector postgres-cdc-source-v2 \
  --config connectors/cdc.json

echo "== [4/7] Topics Kafka (wildcard per-table prefix)"
for t in customers accounts cards merchants transactions; do
  confluent kafka topic create --topic "desafio.payments.$t" --partitions 3 --if-not-exists || true
done

echo "== [5/7] Flink SQL (statements em sql/)"
confluent flink create --name desafio-fraud --parallelism 2 || true

echo "== [6/7] Seed determinístico (seed=42)"
node seed/seed.js > /tmp/desafio-seed.sql
psql "$NEON_DATABASE_URL" -f /tmp/desafio-seed.sql

echo "== [7/7] Teste de compatibilidade de schema"
python tests/test_compatibility.py

echo "== SETUP_OK"
