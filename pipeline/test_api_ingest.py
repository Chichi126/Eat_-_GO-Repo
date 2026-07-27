from __future__ import annotations

from datetime import datetime, timezone

from pipeline.api_ingest import (
    ApiSourceConfig,
    fetch_records_for_interval,
    nested_value,
    normalize_records_for_target,
)


def test_nested_value_reads_dot_paths():
    payload = {"pagination": {"has_more": True, "next_cursor": "abc"}}

    assert nested_value(payload, "pagination.has_more") is True
    assert nested_value(payload, "pagination.next_cursor") == "abc"
    assert nested_value(payload, "pagination.missing", "fallback") == "fallback"


def test_fetch_records_for_cursor_paginated_api(monkeypatch):
    calls = []
    responses = [
        {
            "data": [{"delivery_id": "D1"}],
            "pagination": {"has_more": True, "next_cursor": "cursor-2"},
        },
        {
            "data": [{"delivery_id": "D2"}],
            "pagination": {"has_more": False, "next_cursor": None},
        },
    ]

    def fake_call_api(url, headers, params, max_retries=5):
        calls.append({"url": url, "headers": headers, "params": params})
        return responses[len(calls) - 1]

    monkeypatch.setattr("pipeline.api_ingest.call_api_with_retry", fake_call_api)
    config = ApiSourceConfig(
        provider_name="delivery_partner",
        source_name="deliveries_api",
        target_source="deliveries",
        base_url="http://provider",
        data_path="/api/v1/deliveries",
        auth_type="none",
        pagination_style="cursor",
        page_size_param_name="limit",
        has_more_key="pagination.has_more",
        cursor_response_key="pagination.next_cursor",
        interval_start_param_name="updated_after",
        interval_end_param_name="updated_before",
        extra_params=(("brand", "Domino's Pizza Nigeria"),),
    )

    records, raw_pages = fetch_records_for_interval(
        config,
        token=None,
        interval_start=datetime(2026, 5, 1, tzinfo=timezone.utc),
        interval_end=datetime(2026, 5, 2, tzinfo=timezone.utc),
    )

    assert records == [{"delivery_id": "D1"}, {"delivery_id": "D2"}]
    assert len(raw_pages) == 2
    assert calls[0]["params"]["updated_after"] == "2026-05-01T00:00:00Z"
    assert calls[0]["params"]["updated_before"] == "2026-05-02T00:00:00Z"
    assert calls[0]["params"]["limit"] == 500
    assert calls[0]["params"]["brand"] == "Domino's Pizza Nigeria"
    assert "cursor" not in calls[0]["params"]
    assert calls[1]["params"]["cursor"] == "cursor-2"


def test_delivery_api_records_are_normalized_to_json_source_contract():
    records = [
        {
            "delivery_id": "D1",
            "order_id": "O1",
            "store_id": "S1",
            "assigned_at": "2026-05-01T10:00:00Z",
            "promised_delivery_at": "2026-05-01T10:45:00Z",
            "delivered_at": "2026-05-01T10:40:00Z",
        }
    ]

    normalized = normalize_records_for_target("deliveries", records)

    assert normalized == [
        {
            "order_id": "O1",
            "store_id": "S1",
            "order_ts": "2026-05-01T10:00:00Z",
            "promised_delivery_minutes": 45,
            "actual_delivery_minutes": 40,
            "delivered_within_sla": 1,
        }
    ]
