"""JSON source-drop readiness and fingerprint state.

The JSON pipeline should only mark a file drop as processed after the full
warehouse and audit path succeeds. Airflow can use the same functions for
branching, but the source-change rule belongs here so local and scheduled runs
behave the same way.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REQUIRED_JSON_FILES = (
    "customers.json",
    "stores.json",
    "products.json",
    "orders.json",
    "order_items.json",
    "deliveries.json",
)

READY = "ready"
UNCHANGED = "unchanged"
EMPTY = "empty"
MISSING = "missing"


@dataclass(frozen=True)
class JsonSourceInspection:
    status: str
    source_path: str
    fingerprint: str | None
    files: list[dict[str, Any]]
    missing_files: list[str]
    state_path: str
    previous_fingerprint: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def default_json_source_state_path() -> Path:
    project_root = Path(__file__).resolve().parents[1]
    warehouse_db = project_root / "warehouse" / "eat_ngo_dw.duckdb"
    return warehouse_db.parent / "source_state" / "json_source_fingerprint.json"


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_previous_fingerprint(state_path: Path) -> str | None:
    if not state_path.is_file():
        return None
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    fingerprint = payload.get("fingerprint")
    return str(fingerprint) if fingerprint else None


def _source_fingerprint(files: list[dict[str, Any]]) -> str:
    payload = json.dumps(files, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def inspect_json_source_drop(
    source_path: str | Path,
    state_path: str | Path | None = None,
) -> JsonSourceInspection:
    """Inspect a six-file JSON source drop and compare it with last success."""
    source_dir = Path(source_path)
    resolved_state_path = (
        Path(state_path) if state_path else default_json_source_state_path()
    )
    previous_fingerprint = _load_previous_fingerprint(resolved_state_path)

    if not source_dir.is_dir():
        return JsonSourceInspection(
            status=MISSING,
            source_path=str(source_dir),
            fingerprint=None,
            files=[],
            missing_files=list(REQUIRED_JSON_FILES),
            state_path=str(resolved_state_path),
            previous_fingerprint=previous_fingerprint,
        )

    available_files = {
        path.name
        for path in source_dir.glob("*.json")
        if path.is_file() and not path.name.startswith(".")
    }
    if not available_files:
        return JsonSourceInspection(
            status=EMPTY,
            source_path=str(source_dir),
            fingerprint=None,
            files=[],
            missing_files=list(REQUIRED_JSON_FILES),
            state_path=str(resolved_state_path),
            previous_fingerprint=previous_fingerprint,
        )

    missing_files = [
        name for name in REQUIRED_JSON_FILES if name not in available_files
    ]
    if missing_files:
        raise FileNotFoundError(
            f"JSON source drop is incomplete in {source_dir}. "
            f"Missing required file(s): {missing_files}"
        )

    file_manifest = []
    for name in REQUIRED_JSON_FILES:
        path = source_dir / name
        stat = path.stat()
        file_manifest.append(
            {
                "file": name,
                "size_bytes": stat.st_size,
                "sha256": _file_sha256(path),
            }
        )

    fingerprint = _source_fingerprint(file_manifest)
    status = UNCHANGED if fingerprint == previous_fingerprint else READY
    return JsonSourceInspection(
        status=status,
        source_path=str(source_dir),
        fingerprint=fingerprint,
        files=file_manifest,
        missing_files=[],
        state_path=str(resolved_state_path),
        previous_fingerprint=previous_fingerprint,
    )


def mark_json_source_processed(
    inspection: JsonSourceInspection | dict[str, Any],
    execution_date: str,
    run_id: str,
) -> None:
    """Persist the source fingerprint after a successful pipeline run."""
    payload = inspection if isinstance(inspection, dict) else inspection.to_dict()
    if payload.get("status") != READY or not payload.get("fingerprint"):
        raise RuntimeError(
            "Only a ready JSON source inspection can be marked processed."
        )

    state_path = Path(str(payload["state_path"]))
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_payload = {
        "fingerprint": payload["fingerprint"],
        "source_path": payload["source_path"],
        "files": payload["files"],
        "execution_date": execution_date,
        "run_id": run_id,
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }
    temp_path = state_path.with_suffix(state_path.suffix + ".tmp")
    temp_path.write_text(json.dumps(state_payload, indent=2), encoding="utf-8")
    temp_path.replace(state_path)
