#!/usr/bin/env bash
# Integration Test — Full Pipeline (Confluent Cloud)
# Source: workflow-automation + github-actions-creator
# Run: ./scripts/integration_test_full_pipeline.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

# Check required secrets
check_secrets() {
    local required=(
        "CONFLUENT_API_KEY"
        "CONFLUENT_API_SECRET"
        "CONFLUENT_ENVIRONMENT"
        "NEON_API_KEY"
    )
    for var in "${required[@]}"; do
        if [[ -z "${!var:-}" ]]; then
            log_error "Missing required secret: $var"
            exit 1
        fi
    done
}

# Deploy pipeline to Confluent Cloud
deploy_pipeline() {
    log_info "Deploying pipeline to Confluent Cloud..."

    # 1. Create Kafka topics
    confluent kafka topic create desafio.payments.transactions --partitions 6 --config retention.ms=604800000
    confluent kafka topic create desafio.payments.accounts --partitions 3 --config retention.ms=604800000
    confluent kafka topic create desafio.payments.cards --partitions 3 --config retention.ms=604800000
    confluent kafka topic create desafio.payments.merchants --partitions 3 --config retention.ms=604800000
    confluent kafka topic create desafio.payments.transactions.enriched --partitions 6 --config retention.ms=604800000
    confluent kafka topic create desafio.payments.fraud_alerts --partitions 3 --config retention.ms=2592000000

    # 2. Register schemas
    for schema in "$PROJECT_ROOT/schemas"/*.avsc; do
        subject=$(basename "$schema" .avsc)
        confluent schema-registry schema register "$subject" --schema "$schema"
    done

    # 3. Create Debezium connector
    confluent connect connector create --config "$PROJECT_ROOT/connectors/cdc.json"

    # 4. Deploy Flink SQL
    # Note: Confluent Cloud Flink uses different deployment mechanism
    # This would use `confluent flink` commands or REST API
    log_warn "Flink SQL deployment via Confluent Cloud CLI not fully automated yet"
}

# Run seed data
run_seed() {
    log_info "Running deterministic seed (seed=42)..."
    cd "$PROJECT_ROOT"
    node seed/seed.js
}

# Verify pipeline end-to-end
verify_pipeline() {
    log_info "Verifying pipeline..."

    # Wait for CDC to catch up
    sleep 30

    # Check topic data
    log_info "Checking transactions topic..."
    confluent kafka topic consume desafio.payments.transactions --from-beginning --max-messages 5

    # Check enriched topic
    log_info "Checking enriched topic..."
    confluent kafka topic consume desafio.payments.transactions.enriched --from-beginning --max-messages 5

    # Check fraud alerts
    log_info "Checking fraud alerts..."
    confluent kafka topic consume desafio.payments.fraud_alerts --from-beginning --max-messages 10
}

# Cleanup
cleanup() {
    log_info "Cleaning up test resources..."
    # Delete test topics, connectors, etc.
}

main() {
    log_info "=== Full Pipeline Integration Test ==="

    check_secrets
    deploy_pipeline
    run_seed
    verify_pipeline

    log_info "=== Integration Test Complete ==="
}

# Trap cleanup on exit
trap cleanup EXIT

main "$@"