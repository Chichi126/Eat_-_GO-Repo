"""Shared configuration helpers for the Eat N' Go data pipeline."""

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_DIR = Path(__file__).resolve().parent
DEFAULT_SOURCE_DIR = PROJECT_ROOT / "dataSource"
DEFAULT_SCHEMA_MANIFEST_PATH = PIPELINE_DIR / "schema_manifest.json"
DEFAULT_WAREHOUSE_DB = PROJECT_ROOT / "warehouse" / "eat_ngo_dw.duckdb"


load_dotenv(PROJECT_ROOT / ".env")


def require_env(name: str) -> str:
    """Return a required environment variable or raise a clear error."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"Required environment variable '{name}' is not set; check your .env"
        )
    return value


def env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "y"}


def warehouse_db_path(default_to_local: bool = True) -> str | None:
    """Resolve the warehouse DB path consistently for Airflow and local runs."""
    configured = os.getenv("WAREHOUSE_DB")
    if configured:
        return configured
    return str(DEFAULT_WAREHOUSE_DB) if default_to_local else None


@dataclass(frozen=True)
class MinioSettings:
    endpoint: str
    access_key: str
    secret_key: str
    secure: bool


def minio_settings_from_env() -> MinioSettings:
    return MinioSettings(
        endpoint=require_env("MINIO_ENDPOINT"),
        access_key=require_env("MINIO_ACCESS_KEY"),
        secret_key=require_env("MINIO_SECRET_KEY"),
        secure=env_flag("MINIO_SECURE", default=False),
    )
