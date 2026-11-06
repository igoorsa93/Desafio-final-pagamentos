#!/usr/bin/env python3
# Schema Registry Compatibility Check
# Source: github-actions-creator + software-architecture
# Run: python scripts/check_schema_registry_compatibility.py

import os
import sys
import json
import glob
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSchema

# Config from environment
SR_URL = os.getenv("SCHEMA_REGISTRY_URL")
SR_API_KEY = os.getenv("SCHEMA_REGISTRY_API_KEY")
SR_API_SECRET = os.getenv("SCHEMA_REGISTRY_API_SECRET")

if not all([SR_URL, SR_API_KEY, SR_API_SECRET]):
    print("ERROR: Missing required environment variables:")
    print("  SCHEMA_REGISTRY_URL")
    print("  SCHEMA_REGISTRY_API_KEY")
    print("  SCHEMA_REGISTRY_API_SECRET")
    sys.exit(1)

# Create client
client = SchemaRegistryClient({
    "url": SR_URL,
    "basic.auth.user.info": f"{SR_API_KEY}:{SR_API_SECRET}"
})

# Namespace mapping
SCHEMA_SUBJECTS = {
    "schema-payment-events-v1.avsc": "desafio.payments.transactions-value",
    "schema-payment-events-v2.avsc": "desafio.payments.transactions-value",
    "schema-fraud-alert.avsc": "desafio.payments.fraud_alerts-value",
}


def check_compatibility(schema_file: str, subject: str) -> dict:
    """Check if schema is compatible with registered version."""
    with open(schema_file) as f:
        schema_str = f.read()

    schema = AvroSchema(schema_str)

    try:
        # Test compatibility (BACKWARD is default for consumer safety)
        is_compatible = client.test_compatibility(subject, schema, "BACKWARD")
        return {
            "subject": subject,
            "schema_file": os.path.basename(schema_file),
            "compatible": is_compatible,
            "compatibility_level": "BACKWARD",
            "error": None
        }
    except Exception as e:
        return {
            "subject": subject,
            "schema_file": os.path.basename(schema_file),
            "compatible": False,
            "compatibility_level": "BACKWARD",
            "error": str(e)
        }


def register_schema(schema_file: str, subject: str) -> dict:
    """Register new schema version."""
    with open(schema_file) as f:
        schema_str = f.read()

    schema = AvroSchema(schema_str)

    try:
        schema_id = client.register_schema(subject, schema)
        return {
            "subject": subject,
            "schema_id": schema_id,
            "status": "registered"
        }
    except Exception as e:
        return {
            "subject": subject,
            "status": "failed",
            "error": str(e)
        }


def main():
    print(f"Schema Registry: {SR_URL}")
    print("=" * 60)

    results = []

    # Check all schemas
    for schema_file, subject in SCHEMA_SUBJECTS.items():
        full_path = f"schemas/{schema_file}"
        if not os.path.exists(full_path):
            print(f"SKIP: {schema_file} not found")
            continue

        print(f"Checking {schema_file} -> {subject}...")
        result = check_compatibility(full_path, subject)
        results.append(result)

        if result["compatible"]:
            print(f"  ✓ COMPATIBLE (BACKWARD)")
        else:
            print(f"  ✗ INCOMPATIBLE: {result['error']}")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY:")
    all_compatible = all(r["compatible"] for r in results)
    for r in results:
        status = "✓" if r["compatible"] else "✗"
        print(f"  {status} {r['schema_file']} -> {r['subject']}")

    if all_compatible:
        print("\nAll schemas compatible! Safe to deploy.")
        sys.exit(0)
    else:
        print("\nINCOMPATIBLE SCHEMAS DETECTED — Deploy blocked!")
        sys.exit(1)


if __name__ == "__main__":
    main()