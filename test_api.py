"""API tests with Pinot mocked out."""
from fastapi.testclient import TestClient

from app import api

client = TestClient(api.app)


def test_validation_errors_are_422(monkeypatch):
    monkeypatch.setattr(api, "run_sql", lambda sql: [])
    assert client.get("/kpis?minutes=0").status_code == 422
    assert client.get("/top-products?city=Atlantis").status_code == 422
    assert client.get("/orders/O-1' OR 1=1").status_code == 422


def test_kpis_and_not_found(monkeypatch):
    seen = []
    monkeypatch.setattr(api, "run_sql", lambda sql: seen.append(sql) or [{"orders": 3}])
    assert client.get("/kpis").json() == {"orders": 3}
    monkeypatch.setattr(api, "run_sql", lambda sql: [])
    assert client.get("/orders/O-abcdef123456").status_code == 404
