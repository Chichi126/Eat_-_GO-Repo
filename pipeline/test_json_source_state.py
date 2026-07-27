from __future__ import annotations

import pytest

from pipeline.json_source_state import (
    READY,
    REQUIRED_JSON_FILES,
    UNCHANGED,
    inspect_json_source_drop,
    mark_json_source_processed,
)


def write_complete_drop(source_dir, content_suffix=""):
    source_dir.mkdir()
    for name in REQUIRED_JSON_FILES:
        source_dir.joinpath(name).write_text(
            f'{{"source": "{name}", "suffix": "{content_suffix}"}}\n',
            encoding="utf-8",
        )


def test_json_source_drop_is_unchanged_after_successful_mark(tmp_path):
    source_dir = tmp_path / "dataSource"
    state_path = tmp_path / "warehouse" / "source_state" / "json.json"
    write_complete_drop(source_dir)

    first_inspection = inspect_json_source_drop(source_dir, state_path)
    assert first_inspection.status == READY

    mark_json_source_processed(
        first_inspection,
        execution_date="2026-07-25",
        run_id="json-2026-07-25",
    )

    second_inspection = inspect_json_source_drop(source_dir, state_path)
    assert second_inspection.status == UNCHANGED
    assert second_inspection.fingerprint == first_inspection.fingerprint


def test_json_source_drop_becomes_ready_when_file_content_changes(tmp_path):
    source_dir = tmp_path / "dataSource"
    state_path = tmp_path / "warehouse" / "source_state" / "json.json"
    write_complete_drop(source_dir)
    first_inspection = inspect_json_source_drop(source_dir, state_path)
    mark_json_source_processed(
        first_inspection,
        execution_date="2026-07-25",
        run_id="json-2026-07-25",
    )

    source_dir.joinpath("orders.json").write_text(
        '{"source": "orders.json", "suffix": "changed"}\n',
        encoding="utf-8",
    )

    changed_inspection = inspect_json_source_drop(source_dir, state_path)
    assert changed_inspection.status == READY
    assert changed_inspection.fingerprint != first_inspection.fingerprint


def test_incomplete_json_source_drop_fails_fast(tmp_path):
    source_dir = tmp_path / "dataSource"
    source_dir.mkdir()
    source_dir.joinpath("orders.json").write_text("[]", encoding="utf-8")

    with pytest.raises(FileNotFoundError, match="Missing required file"):
        inspect_json_source_drop(source_dir, tmp_path / "state.json")
