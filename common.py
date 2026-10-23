"""Shared config, domain model and pure business logic (easy to unit-test)."""
from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC_RAW = os.getenv("TOPIC_RAW", "orders.raw")
TOPIC_ENRICHED = os.getenv("TOPIC_ENRICHED", "orders.enriched")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "orders.dlq")
PINOT_BROKER = os.getenv("PINOT_BROKER", "http://localhost:8099")

STATUSES = ("CREATED", "PAID", "SHIPPED", "DELIVERED", "CANCELLED")
PAYMENT_METHODS = ("CARD", "UPI", "WALLET", "COD")
CITIES = ("Mumbai", "Delhi", "Bengaluru", "Pune", "Chennai", "Hyderabad")


@dataclass(frozen=True)
class Product:
    name: str
    category: str
    unit_price: float


# Stand-in for a reference-data store (in real life: a compacted topic, Redis, or a DB lookup).
CATALOG: dict[str, Product] = {
    "P100": Product("Wireless Mouse", "electronics", 799.0),
    "P101": Product("Mechanical Keyboard", "electronics", 3499.0),
    "P102": Product("USB-C Hub", "electronics", 1899.0),
    "P200": Product("Running Shoes", "fashion", 2999.0),
    "P201": Product("Denim Jacket", "fashion", 2499.0),
    "P300": Product("Coffee Beans 1kg", "grocery", 899.0),
    "P301": Product("Olive Oil 1L", "grocery", 749.0),
    "P400": Product("Yoga Mat", "sports", 999.0),
}

REQUIRED_FIELDS = (
    "event_id", "order_id", "user_id", "product_id",
    "quantity", "status", "payment_method", "city", "event_time",
)


class ValidationError(ValueError):
    """Raised for events that must be routed to the dead-letter topic."""


def validate_event(event: dict) -> None:
    if not isinstance(event, dict):
        raise ValidationError("event is not a JSON object")
    missing = [f for f in REQUIRED_FIELDS if event.get(f) in (None, "")]
    if missing:
        raise ValidationError(f"missing fields: {missing}")
    if event["status"] not in STATUSES:
        raise ValidationError(f"unknown status: {event['status']!r}")
    if event["payment_method"] not in PAYMENT_METHODS:
        raise ValidationError(f"unknown payment_method: {event['payment_method']!r}")
    if event["product_id"] not in CATALOG:
        raise ValidationError(f"unknown product_id: {event['product_id']!r}")
    if not isinstance(event["quantity"], int) or isinstance(event["quantity"], bool) or event["quantity"] <= 0:
        raise ValidationError(f"invalid quantity: {event['quantity']!r}")
    if not isinstance(event["event_time"], int) or event["event_time"] <= 0:
        raise ValidationError(f"invalid event_time: {event['event_time']!r}")


def enrich_event(event: dict, now_ms: int | None = None) -> dict:
    """Return a FULL snapshot of the order state (required by Pinot FULL upsert)."""
    validate_event(event)
    product = CATALOG[event["product_id"]]
    return {
        "event_id": event["event_id"],
        "order_id": event["order_id"],
        "user_id": event["user_id"],
        "product_id": event["product_id"],
        "product_name": product.name,
        "category": product.category,
        "quantity": event["quantity"],
        "amount": round(product.unit_price * event["quantity"], 2),
        "status": event["status"],
        "payment_method": event["payment_method"],
        "city": event["city"],
        "event_time": event["event_time"],
        "processed_time": now_ms if now_ms is not None else int(time.time() * 1000),
    }


def new_event_id() -> str:
    return str(uuid.uuid4())
