import json
import random

import pytest

from app import queries
from app.common import ValidationError, enrich_event, validate_event
from app.processor import TOPIC_DLQ, TOPIC_ENRICHED, process_message
from app.producer import build_order_events

GOOD = {
    "event_id": "e1", "order_id": "O-abcdef123456", "user_id": "U00001", "product_id": "P100",
    "quantity": 2, "status": "PAID", "payment_method": "UPI", "city": "Pune", "event_time": 1_700_000_000_000,
}


def test_enrich_computes_amount_and_full_snapshot():
    out = enrich_event(GOOD, now_ms=5)
    assert out["amount"] == 1598.0
    assert out["category"] == "electronics"
    assert out["processed_time"] == 5
    assert out["order_id"] == GOOD["order_id"] and out["status"] == "PAID"


@pytest.mark.parametrize("patch", [
    {"quantity": 0}, {"quantity": "2"}, {"quantity": True}, {"status": "LOST"},
    {"product_id": "NOPE"}, {"payment_method": "CASH"}, {"event_time": -1}, {"order_id": ""},
])
def test_invalid_events_rejected(patch):
    with pytest.raises(ValidationError):
        validate_event({**GOOD, **patch})


def test_process_message_routes_good_and_bad():
    topic, payload = process_message(json.dumps(GOOD).encode())
    assert topic == TOPIC_ENRICHED and json.loads(payload)["amount"] == 1598.0
    for bad in (b"not json", b"", json.dumps({**GOOD, "quantity": -1}).encode(), b"[1,2]"):
        topic, payload = process_message(bad)
        assert topic == TOPIC_DLQ and "error" in json.loads(payload)


def test_order_lifecycle_is_ordered_and_valid():
    rng = random.Random(7)
    for _ in range(200):
        events = build_order_events(1_700_000_000_000, rng, bad_ratio=0)
        assert events[0]["status"] == "CREATED"
        times = [e["event_time"] for e in events]
        assert times == sorted(times)
        assert len({e["order_id"] for e in events}) == 1
        for e in events:
            validate_event(e)


def test_queries_validate_inputs():
    now = 1_700_000_000_000
    assert "event_time > 1699999100000" in queries.kpis_sql(now, 15)
    with pytest.raises(ValueError):
        queries.kpis_sql(now, 0)
    with pytest.raises(ValueError):
        queries.top_products_sql(now, 5, 5, "Pune'; DROP TABLE x;--")
    with pytest.raises(ValueError):
        queries.order_sql("O-1' OR '1'='1")
    assert "O-abcdef123456" in queries.order_sql("O-abcdef123456")
