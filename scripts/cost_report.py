#!/usr/bin/env python3
# Cost Report Generator — Pipeline de Pagamentos
# Source: research-finance + workflow-automation
# Run: python scripts/cost_report.py --env staging --days 7

import argparse
import json
from datetime import datetime, timedelta
from typing import Dict, Any

# Confluent Cloud pricing (approximate, USD/month)
PRICING = {
    "basic": {
        "cku_per_hour": 0.11,           # CKU hour
        "storage_gb_month": 0.25,       # per GB/month
        "network_gb": 0.10,             # per GB in/out
        "connect_task_hour": 0.05,      # per task hour
        "flink_cfu_hour": 0.15,         # CFU hour
        "schema_registry": 0.00,        # included
    },
    "standard": {
        "cku_per_hour": 0.11,
        "storage_gb_month": 0.25,
        "network_gb": 0.10,
        "connect_task_hour": 0.05,
        "flink_cfu_hour": 0.15,
        "schema_registry": 0.00,
    },
    "dedicated": {
        "cku_per_hour": 0.18,
        "storage_gb_month": 0.30,
        "network_gb": 0.08,
        "connect_task_hour": 0.08,
        "flink_cfu_hour": 0.20,
        "schema_registry": 0.00,
    }
}

# Pipeline resource estimates (per month)
RESOURCE_ESTIMATES = {
    "kafka": {
        "cku": 1,           # Basic cluster ~1 CKU
        "storage_gb": 50,   # 7-day retention, ~2k tx/s
        "network_gb_in": 100,
        "network_gb_out": 200,
    },
    "connect": {
        "tasks": 1,         # Single Debezium task
        "hours": 730,
    },
    "flink": {
        "cfu": 2,           # 2 parallelism
        "hours": 730,
    },
    "schema_registry": {
        "schemas": 10,
    },
    "neon": {
        "compute_hours": 730,  # Basic, auto-suspend after 5min
        "storage_gb": 10,
    }
}


def calculate_monthly_cost(tier: str = "basic") -> Dict[str, Any]:
    """Calculate estimated monthly cost for the pipeline."""
    pricing = PRICING[tier]
    resources = RESOURCE_ESTIMATES

    kafka_cost = (
        resources["kafka"]["cku"] * pricing["cku_per_hour"] * 730 +
        resources["kafka"]["storage_gb"] * pricing["storage_gb_month"] +
        resources["kafka"]["network_gb_in"] * pricing["network_gb"] +
        resources["kafka"]["network_gb_out"] * pricing["network_gb"]
    )

    connect_cost = (
        resources["connect"]["tasks"] * pricing["connect_task_hour"] * resources["connect"]["hours"]
    )

    flink_cost = (
        resources["flink"]["cfu"] * pricing["flink_cfu_hour"] * resources["flink"]["hours"]
    )

    neon_cost = 19.00  # Neon Basic fixed ~$19/month

    total = kafka_cost + connect_cost + flink_cost + neon_cost

    return {
        "tier": tier,
        "breakdown": {
            "kafka": round(kafka_cost, 2),
            "connect": round(connect_cost, 2),
            "flink": round(flink_cost, 2),
            "neon": round(neon_cost, 2),
        },
        "total_monthly_usd": round(total, 2),
        "total_annual_usd": round(total * 12, 2),
        "assumptions": {
            "transactions_per_second": 2000,
            "retention_days": 7,
            "partitions": 6,
            "flink_parallelism": 2,
            "connect_tasks": 1,
        }
    }


def optimize_cost(current: Dict[str, Any]) -> Dict[str, Any]:
    """Suggest cost optimizations."""
    suggestions = []

    if current["breakdown"]["kafka"] > 50:
        suggestions.append({
            "area": "Kafka",
            "suggestion": "Reduce retention from 7d to 3d for non-critical topics",
            "estimated_savings_usd_month": round(current["breakdown"]["kafka"] * 0.3, 2)
        })
        suggestions.append({
            "area": "Kafka",
            "suggestion": "Enable tiered storage (Confluent Cloud)",
            "estimated_savings_usd_month": round(current["breakdown"]["kafka"] * 0.4, 2)
        })

    if current["breakdown"]["flink"] > 30:
        suggestions.append({
            "area": "Flink",
            "suggestion": "Scale parallelism to 1 outside business hours (cron)",
            "estimated_savings_usd_month": round(current["breakdown"]["flink"] * 0.5, 2)
        })
        suggestions.append({
            "area": "Flink",
            "suggestion": "Use Flink autoscaling (reactive mode)",
            "estimated_savings_usd_month": round(current["breakdown"]["flink"] * 0.3, 2)
        })

    suggestions.append({
        "area": "Neon",
        "suggestion": "Auto-suspend after 5min idle (enabled by default on Basic)",
        "estimated_savings_usd_month": 0  # Already included
    })

    return {
        "current_monthly": current["total_monthly_usd"],
        "potential_savings_monthly": round(sum(s["estimated_savings_usd_month"] for s in suggestions), 2),
        "suggestions": suggestions
    }


def main():
    parser = argparse.ArgumentParser(description="Generate cost report for pipeline")
    parser.add_argument("--env", choices=["basic", "standard", "dedicated"], default="basic", help="Confluent tier")
    parser.add_argument("--days", type=int, default=30, help="Days to project")
    parser.add_argument("--output", choices=["json", "table"], default="table", help="Output format")
    args = parser.parse_args()

    monthly = calculate_monthly_cost(args.env)
    optimization = optimize_cost(monthly)

    report = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "environment": args.env,
        "projection_days": args.days,
        "monthly": monthly,
        "projected_total": round(monthly["total_monthly_usd"] * (args.days / 30), 2),
        "optimization": optimization
    }

    if args.output == "json":
        print(json.dumps(report, indent=2))
    else:
        print("=" * 60)
        print(f"COST REPORT — Pipeline de Pagamentos ({args.env.upper()})")
        print("=" * 60)
        print(f"Generated: {report['generated_at']}")
        print(f"Projection: {args.days} days")
        print()
        print("MONTHLY BREAKDOWN:")
        for component, cost in monthly["breakdown"].items():
            print(f"  {component:15s}: ${cost:>8.2f}")
        print(f"  {'TOTAL':15s}: ${monthly['total_monthly_usd']:>8.2f}/month")
        print(f"  {'ANNUAL':15s}: ${monthly['total_annual_usd']:>8.2f}/year")
        print()
        print("ASSUMPTIONS:")
        for k, v in monthly["assumptions"].items():
            print(f"  {k}: {v}")
        print()
        print("OPTIMIZATION SUGGESTIONS:")
        print(f"  Current monthly: ${optimization['current_monthly']:.2f}")
        print(f"  Potential savings: ${optimization['potential_savings_monthly']:.2f}/month")
        for s in optimization["suggestions"]:
            print(f"  - [{s['area']}] {s['suggestion']} (save ~${s['estimated_savings_usd_month']:.2f}/mo)")


if __name__ == "__main__":
    main()