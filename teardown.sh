#!/usr/bin/env bash
# teardown.sh — Desafio Final: cleanup total
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

echo "== [1/6] Flink statements"
confluent flink statement stop --all || true

echo "== [2/6] Kafka Connect connector"
confluent connector delete --name postgres-cdc-desafio || true

echo "== [3/6] Kafka topics"
for t in desafio.payments.customers desafio.payments.accounts desafio.payments.cards desafio.payments.merchants desafio.payments.transactions desafio.fraud.detected; do
  confluent kafka topic delete --topic "$t" || true
done

echo "== [4/6] Flink jobs"
confluent flink delete-job --job-name desafio-fraud || true

echo "== [5/6] Confluent cluster"
confluent cluster destroy --exact-name desafio-basic || true

echo "== [6/6] Verificacao de listas vazias"
confluent cluster list
confluent connect cluster list
confluent flink statement list

if confluent cluster list | grep -qi desafio; then echo "TEARDOWN_FALHOU: cluster restante"; exit 1; fi
if confluent flink statement list | grep -qi desafio; then echo "TEARDOWN_FALHOU: statement restante"; exit 1; fi
if confluent connect cluster list | grep -qi desafio; then echo "TEARDOWN_FALHOU: connector cluster restante"; exit 1; fi
echo "TEARDOWN_OK: connect cluster list / flink statement list -> None found."
