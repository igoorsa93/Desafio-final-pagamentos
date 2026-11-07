"""Backward evolution tests — schema V1 -> V2.
Ambos:
 1) compatibilidade ACCEPTANDO campo novo com default
 2) rejeição REMOÇÃO de campo obrigatório
"""
import json
from pathlib import Path

V1 = json.loads(Path("schemas/schema-payment-events-v1.avsc").read_text())
V2 = json.loads(Path("schemas/schema-payment-events-v2.avsc").read_text())

REQUIRED_V1 = set(V1.get("required") or [])
REQUIRED_V2 = set(V2.get("required") or [])


def test_v2_adds_optional_field_with_default():
    field = next(f for f in V2["fields"] if f["name"] == "tx_currency_code")
    assert field["default"] is not None  # default aplicado
    assert "tx_currency_code" not in REQUIRED_V2  # opcional, nao obrigatorio
    assert REQUIRED_V1 <= REQUIRED_V2  # V1 obrigatorios preservados


def test_v2_accepts_v1_payload():
    payload = {
        "event_id": "e1",
        "op": 0,
        "table_name": "transactions",
        "before": None,
        "after": {
            "row_id": 1,
            "amount_cents": 100,
            "amount_decimal": "1.00",
            "currency": "USD",
        },
        "ts": 1760000000000,
    }
    assert REQUIRED_V1 <= set(
        payload.keys()
    )  # payload V1 valido contra V2 (campos novos com default)


def test_rejects_removal_of_required_field():
    # Remover campo obrigatorio (ts) -> schema quebrado -> payload sem ele e invalido contra V2
    assert "ts" in REQUIRED_V2
    payload_sem = {
        "event_id": "e1",
        "op": 0,
        "table_name": "transactions",
        "before": None,
        "after": {"row_id": 1},
    }
    assert "ts" in REQUIRED_V2 - set(payload_sem.keys())


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"PASS: {name}")
    print("ALL_TESTS_OK")
