from __future__ import annotations

import json
import time

import httpx
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from api import main
from api.database import db_connect, init_source_db, summary
from api.ingestion_client import DeliveryApiIngestionClient


@pytest.fixture()
def source_db(tmp_path, monkeypatch):
    path = tmp_path / "source.sqlite"
    init_source_db(path, reset=True)
    monkeypatch.setattr(main, "DEFAULT_DB_PATH", path)
    return path


@pytest.fixture()
def api_client(source_db):
    return TestClient(main.app)


def token_headers(client: TestClient) -> dict[str, str]:
    response = client.post(
        "/api/v1/auth/token",
        json={
            "client_id": "eatngo-bi-client",
            "client_secret": "local-development-secret",
        },
    )
    assert response.status_code == 200
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


def test_token_authentication(api_client):
    response = api_client.post(
        "/api/v1/auth/token",
        json={
            "client_id": "eatngo-bi-client",
            "client_secret": "local-development-secret",
        },
    )

    assert response.status_code == 200
    assert response.json()["token_type"] == "bearer"
    assert response.json()["expires_in"] > 0


def test_delivery_endpoint_does_not_require_token(api_client):
    response = api_client.get(
        "/api/v1/deliveries",
        params={
            "updated_after": "2026-05-01T00:00:00Z",
            "updated_before": "2026-05-02T00:00:00Z",
            "limit": 1,
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]


def test_token_expiry_helper_still_rejects_expired_token(api_client, monkeypatch):
    monkeypatch.setattr(main, "TOKEN_TTL_SECONDS", 0)
    response = api_client.post(
        "/api/v1/auth/token",
        json={
            "client_id": "eatngo-bi-client",
            "client_secret": "local-development-secret",
        },
    )
    token = response.json()["access_token"]
    time.sleep(0.01)

    with pytest.raises(HTTPException) as exc_info:
        main.require_token(f"Bearer {token}")

    assert exc_info.value.status_code == 401
    monkeypatch.setattr(main, "TOKEN_TTL_SECONDS", 300)


def test_cursor_pagination(api_client):
    params = {
        "limit": 5,
        "updated_after": "2026-05-01T00:00:00Z",
        "updated_before": "2026-05-04T00:00:00Z",
    }
    first = api_client.get("/api/v1/deliveries", params=params).json()
    second = api_client.get(
        "/api/v1/deliveries",
        params={**params, "cursor": first["pagination"]["next_cursor"]},
    ).json()

    assert len(first["data"]) == 5
    assert first["pagination"]["has_more"] is True
    assert first["data"] != second["data"]


def test_deterministic_generation(source_db):
    with db_connect(source_db) as con:
        first = summary(con, "2026-05-01T00:00:00Z", "2026-05-20T00:00:00Z")
    init_source_db(source_db, reset=True)
    with db_connect(source_db) as con:
        second = summary(con, "2026-05-01T00:00:00Z", "2026-05-20T00:00:00Z")

    assert first == second


def test_updated_after_and_brand_filtering(api_client):
    response = api_client.get(
        "/api/v1/deliveries",
        params={
            "updated_after": "2026-05-10T00:00:00Z",
            "updated_before": "2026-06-10T00:00:00Z",
            "brand": "Domino's Pizza Nigeria",
            "limit": 100,
        },
    )
    body = response.json()

    assert response.status_code == 200
    assert body["data"]
    assert {record["brand"] for record in body["data"]} == {"Domino's Pizza Nigeria"}
    assert all(record["updated_at"] > "2026-05-10T00:00:00Z" for record in body["data"])


def test_delivery_by_id_returns_latest_corrected_record(api_client):
    response = api_client.get("/api/v1/deliveries/D0000011")

    assert response.status_code == 200
    assert response.json()["delivery_status"] == "delivered"
    assert response.json()["delivery_fee"] > 0


def mock_transport_for_app(api_client: TestClient) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        params = dict(request.url.params)
        headers = dict(request.headers)
        if path == "/api/v1/auth/token":
            response = api_client.post(
                path, json=json.loads(request.content.decode("utf-8"))
            )
        else:
            response = api_client.get(path, headers=headers, params=params)
        return httpx.Response(
            response.status_code,
            headers=dict(response.headers),
            content=response.content,
            request=request,
        )

    return httpx.MockTransport(handler)


def test_ingestion_dedupes_corrected_records_and_advances_watermark(
    api_client, tmp_path
):
    client = DeliveryApiIngestionClient(
        base_url="http://testserver",
        target_db_path=tmp_path / "target.sqlite",
        page_limit=50,
        http_client=httpx.Client(
            transport=mock_transport_for_app(api_client), base_url="http://testserver"
        ),
    )
    result = client.run(
        updated_after="2026-05-01T00:00:00Z", updated_before="2026-05-20T00:00:00Z"
    )

    with client.connect() as con:
        watermark = con.execute(
            "SELECT last_successful_updated_at FROM api_watermarks"
        ).fetchone()[0]
        raw_count = con.execute("SELECT COUNT(*) FROM api_raw_responses").fetchone()[0]
        latest = con.execute(
            "SELECT delivery_status FROM latest_deliveries WHERE delivery_id = 'D0000011'"
        ).fetchone()

    assert result["raw_records"] > 0
    assert watermark is not None
    assert raw_count > 0
    assert latest["delivery_status"] == "delivered"


def test_watermark_not_advanced_after_failure(api_client, tmp_path):
    client = DeliveryApiIngestionClient(
        base_url="http://testserver",
        target_db_path=tmp_path / "target.sqlite",
        page_limit=50,
        http_client=httpx.Client(
            transport=mock_transport_for_app(api_client), base_url="http://testserver"
        ),
    )

    with pytest.raises(RuntimeError):
        client.run(
            updated_after="2026-05-01T00:00:00Z",
            updated_before="2026-05-02T00:00:00Z",
            fail_after_raw=True,
        )

    with client.connect() as con:
        assert con.execute("SELECT COUNT(*) FROM api_raw_responses").fetchone()[0] == 1
        assert con.execute("SELECT COUNT(*) FROM api_watermarks").fetchone()[0] == 0


def test_retry_on_429_and_503(tmp_path):
    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        if calls["count"] == 1:
            return httpx.Response(429, headers={"Retry-After": "0"}, request=request)
        if calls["count"] == 2:
            return httpx.Response(503, request=request)
        return httpx.Response(200, json={"ok": True}, request=request)

    client = DeliveryApiIngestionClient(
        base_url="http://provider",
        target_db_path=tmp_path / "target.sqlite",
        http_client=httpx.Client(
            transport=httpx.MockTransport(handler), base_url="http://provider"
        ),
        backoff_seconds=0,
    )

    response = client.request_with_retry("GET", "/anything")

    assert response.status_code == 200
    assert calls["count"] == 3


def test_raw_response_preservation_and_quality_checks(api_client, tmp_path):
    client = DeliveryApiIngestionClient(
        base_url="http://testserver",
        target_db_path=tmp_path / "target.sqlite",
        page_limit=25,
        http_client=httpx.Client(
            transport=mock_transport_for_app(api_client), base_url="http://testserver"
        ),
    )
    result = client.run(
        updated_after="2026-05-01T00:00:00Z", updated_before="2026-05-03T00:00:00Z"
    )

    with client.connect() as con:
        raw = con.execute(
            "SELECT raw_payload, checksum FROM api_raw_responses LIMIT 1"
        ).fetchone()
        quality = con.execute(
            "SELECT COUNT(*) FROM api_quality_results WHERE batch_id = ? AND status = 'FAIL'",
            [result["batch_id"]],
        ).fetchone()[0]

    assert json.loads(raw["raw_payload"])["data"]
    assert raw["checksum"]
    assert quality == 0


def test_summary_reconciliation_passes(api_client, tmp_path):
    client = DeliveryApiIngestionClient(
        base_url="http://testserver",
        target_db_path=tmp_path / "target.sqlite",
        page_limit=40,
        http_client=httpx.Client(
            transport=mock_transport_for_app(api_client), base_url="http://testserver"
        ),
    )
    result = client.run(
        updated_after="2026-05-01T00:00:00Z", updated_before="2026-05-05T00:00:00Z"
    )

    assert {row["status"] for row in result["reconciliation"]} == {"PASS"}
