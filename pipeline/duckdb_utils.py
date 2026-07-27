"""DuckDB connection helpers shared by warehouse pipeline modules."""

from pathlib import Path

import duckdb

from pipeline.config import minio_settings_from_env, warehouse_db_path


def _sql_string(value: str) -> str:
    return value.replace("'", "''")


def configure_s3_access(con: duckdb.DuckDBPyConnection) -> None:
    settings = minio_settings_from_env()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{_sql_string(settings.endpoint)}';")
    con.execute(f"SET s3_access_key_id='{_sql_string(settings.access_key)}';")
    con.execute(f"SET s3_secret_access_key='{_sql_string(settings.secret_key)}';")
    con.execute(f"SET s3_use_ssl={'true' if settings.secure else 'false'};")
    con.execute("SET s3_url_style='path';")


def get_duckdb_connection(db_file: str | None = None) -> duckdb.DuckDBPyConnection:
    resolved_db_file = warehouse_db_path() if db_file is None else db_file
    if resolved_db_file:
        db_path = Path(resolved_db_file)
        if db_path.parent != Path("."):
            db_path.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(db_path))
    else:
        con = duckdb.connect()

    configure_s3_access(con)
    return con
