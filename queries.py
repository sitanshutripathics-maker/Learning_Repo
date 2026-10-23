"""Query builders. Pinot's HTTP API has no bind parameters, so every dynamic value is
strictly validated (whitelist / regex / int casts) BEFORE it is interpolated -> no SQL injection.
"""
from __future__ import annotations

import re

from app.common import CATALOG, CITIES, STATUSES

ORDER_ID_RE = re.compile(r"^O-[0-9a-f]{12}$")
CATEGORIES = {p.category for p in CATALOG.values()}


def _window(now_ms: int, minutes: int) -> int:
    if not isinstance(minutes, int) or not 1 <= minutes <= 24 * 60:
        raise ValueError("minutes must be an integer between 1 and 1440")
    return now_ms - minutes * 60_000


def kpis_sql(now_ms: int, minutes: int) -> str:
    since = _window(now_ms, minutes)
    return (
        "SELECT COUNT(*) AS orders, SUM(amount) AS gross_revenue, "
        "SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled "
        f"FROM orders_state WHERE event_time > {since}"
    )


def revenue_by_category_sql(now_ms: int, minutes: int) -> str:
    since = _window(now_ms, minutes)
    return (
        "SELECT category, COUNT(*) AS orders, SUM(amount) AS revenue "
        f"FROM orders_state WHERE event_time > {since} AND status <> 'CANCELLED' "
        "GROUP BY category ORDER BY revenue DESC LIMIT 20"
    )


def funnel_sql(now_ms: int, minutes: int) -> str:
    since = _window(now_ms, minutes)
    return (
        "SELECT status, COUNT(*) AS events FROM order_events "
        f"WHERE event_time > {since} GROUP BY status ORDER BY events DESC"
    )


def revenue_timeseries_sql(now_ms: int, minutes: int) -> str:
    since = _window(now_ms, minutes)
    return (
        "SELECT DATETRUNC('minute', event_time, 'MILLISECONDS') AS minute_ts, "
        "COUNT(*) AS orders, SUM(amount) AS revenue "
        f"FROM order_events WHERE event_time > {since} AND status = 'PAID' "
        "GROUP BY minute_ts ORDER BY minute_ts LIMIT 1440"
    )


def top_products_sql(now_ms: int, minutes: int, limit: int, city: str | None) -> str:
    since = _window(now_ms, minutes)
    if not 1 <= limit <= 100:
        raise ValueError("limit must be between 1 and 100")
    city_filter = ""
    if city is not None:
        if city not in CITIES:
            raise ValueError(f"unknown city: {city}")
        city_filter = f"AND city = '{city}' "
    return (
        "SELECT product_name, SUM(quantity) AS units, SUM(amount) AS revenue "
        f"FROM orders_state WHERE event_time > {since} AND status <> 'CANCELLED' {city_filter}"
        f"GROUP BY product_name ORDER BY revenue DESC LIMIT {limit}"
    )


def order_sql(order_id: str) -> str:
    if not ORDER_ID_RE.match(order_id):
        raise ValueError("invalid order_id")
    return f"SELECT * FROM orders_state WHERE order_id = '{order_id}' LIMIT 1"


def order_history_sql(order_id: str) -> str:
    if not ORDER_ID_RE.match(order_id):
        raise ValueError("invalid order_id")
    return f"SELECT status, event_time FROM order_events WHERE order_id = '{order_id}' ORDER BY event_time LIMIT 50"


__all__ = [n for n in dir() if n.endswith("_sql")] + ["STATUSES"]
