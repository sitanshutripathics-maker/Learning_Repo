"""Real-time analytics API on top of Pinot (FastAPI).

Run:  uvicorn app.api:app --port 8000   ->  http://localhost:8000/docs
"""
from __future__ import annotations

import time

import requests
from fastapi import FastAPI, HTTPException, Query

from app import queries
from app.common import PINOT_BROKER

app = FastAPI(title="Orders Real-Time Analytics", version="1.0.0")
session = requests.Session()


def run_sql(sql: str) -> list[dict]:
    try:
        resp = session.post(
            f"{PINOT_BROKER}/query/sql",
            json={"sql": sql, "queryOptions": "timeoutMs=10000"},
            timeout=15,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Pinot unavailable: {exc}") from exc
    body = resp.json()
    if body.get("exceptions"):
        raise HTTPException(status_code=500, detail=body["exceptions"][0].get("message", "query failed"))
    table = body["resultTable"]
    cols = table["dataSchema"]["columnNames"]
    return [dict(zip(cols, row)) for row in table["rows"]]


def _now_ms() -> int:
    return int(time.time() * 1000)


def _guard(builder, *args):
    try:
        return builder(*args)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


Minutes = Query(15, ge=1, le=1440, description="look-back window in minutes")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/kpis")
def kpis(minutes: int = Minutes):
    rows = run_sql(_guard(queries.kpis_sql, _now_ms(), minutes))
    return rows[0] if rows else {}


@app.get("/revenue-by-category")
def revenue_by_category(minutes: int = Minutes):
    return run_sql(_guard(queries.revenue_by_category_sql, _now_ms(), minutes))


@app.get("/funnel")
def funnel(minutes: int = Minutes):
    return run_sql(_guard(queries.funnel_sql, _now_ms(), minutes))


@app.get("/revenue-timeseries")
def revenue_timeseries(minutes: int = Minutes):
    return run_sql(_guard(queries.revenue_timeseries_sql, _now_ms(), minutes))


@app.get("/top-products")
def top_products(minutes: int = Minutes, limit: int = Query(5, ge=1, le=100), city: str | None = None):
    return run_sql(_guard(queries.top_products_sql, _now_ms(), minutes, limit, city))


@app.get("/orders/{order_id}")
def get_order(order_id: str):
    rows = run_sql(_guard(queries.order_sql, order_id))
    if not rows:
        raise HTTPException(status_code=404, detail="order not found")
    history = run_sql(_guard(queries.order_history_sql, order_id))
    return {"current": rows[0], "history": history}
