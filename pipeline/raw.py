import io
import json
import os
import uuid
from datetime import datetime, timezone

import duckdb

from pipeline.config import DEFAULT_SCHEMA_MANIFEST_PATH
from pipeline.duckdb_utils import get_duckdb_connection
from pipeline.minio_utils import ensure_bucket, get_minio_client

NUMERIC_TYPES = {
    "BIGINT",
    "INTEGER",
    "SMALLINT",
    "TINYINT",
    "DOUBLE",
    "FLOAT",
    "DECIMAL",
}
SCHEMA_MANIFEST_PATH = str(DEFAULT_SCHEMA_MANIFEST_PATH)

# Sources supported by the raw conversion layer. The schema manifest stores
# expected columns only; it does not determine which source files are processed.
SOURCES = ["customers", "stores", "products", "orders", "order_items", "deliveries"]

# Timestamp fields are cast explicitly during raw conversion. TRY_CAST keeps
# malformed values visible as NULLs for downstream data-quality checks.
TIMESTAMP_COLUMNS = {
    "orders": ["order_ts"],
    "deliveries": ["order_ts"],
    "order_items": [],
    "customers": [],
    "stores": [],
    "products": [],
}


def deterministic_batch_id(stage, execution_date):
    """Returns a deterministic batch id for a stage and logical date."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"eat-ngo/{stage}/{execution_date}"))


def load_schema_manifest(path=SCHEMA_MANIFEST_PATH):
    """Loads the expected schema manifest.

    Args:
        path: Manifest file path.

    Returns:
        Mapping of source name to expected DuckDB column types. Missing
        manifests are initialized as empty mappings.
    """
    if not os.path.exists(path):
        print(f"No schema_manifest.json found at {path}; initializing a new manifest.")
        with open(path, "w") as f:
            json.dump({}, f, indent=2)
        return {}

    with open(path) as f:
        return json.load(f)


def update_schema_manifest(source, actual_schema, path=SCHEMA_MANIFEST_PATH):
    """Persists the observed schema after a non-breaking conversion.

    Args:
        source: Source identifier, such as ``orders`` or ``deliveries``.
        actual_schema: Observed DuckDB column type mapping.
        path: Manifest file path.
    """
    if os.path.exists(path):
        with open(path) as f:
            manifest = json.load(f)
    else:
        manifest = {}

    if manifest.get(source) != actual_schema:
        manifest[source] = actual_schema
        with open(path, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"[{source}] schema_manifest.json updated with observed schema")


def get_actual_schema(con, json_uri):
    """Returns DuckDB-inferred column types for a JSON object.

    Args:
        con: Open DuckDB connection.
        json_uri: S3 URI for the raw JSON object.

    Returns:
        Mapping of column name to DuckDB type.
    """
    rows = con.execute(
        f"DESCRIBE SELECT * FROM read_json_auto('{json_uri}', hive_partitioning=false)"
    ).fetchall()
    return {row[0]: row[1] for row in rows}


def classify_drift(expected, actual):
    """Compares an observed schema with the expected schema.

    Args:
        expected: Expected DuckDB column type mapping.
        actual: Observed DuckDB column type mapping.

    Returns:
        Tuple of ``(is_breaking, details)``. Removed columns and non-numeric
        type changes are breaking; numeric widening is allowed.
    """
    missing_columns = [c for c in expected if c not in actual]
    new_columns = [c for c in actual if c not in expected]
    type_changes = []

    for col, expected_type in expected.items():
        if col not in actual:
            continue
        actual_type = actual[col]
        if actual_type == expected_type:
            continue
        breaking = not (expected_type in NUMERIC_TYPES and actual_type in NUMERIC_TYPES)
        type_changes.append(
            {
                "column": col,
                "expected": expected_type,
                "actual": actual_type,
                "breaking": breaking,
            }
        )

    is_breaking = bool(missing_columns) or any(t["breaking"] for t in type_changes)
    return is_breaking, {
        "missing_columns": missing_columns,
        "new_columns": new_columns,
        "type_changes": type_changes,
    }


def build_select_clause(actual_schema, timestamp_columns):
    """Builds an explicit projection for raw JSON conversion.

    Args:
        actual_schema: Observed source schema.
        timestamp_columns: Columns to cast with ``TRY_CAST(... AS TIMESTAMP)``.

    Returns:
        SQL projection clause for the DuckDB ``COPY`` statement.
    """
    parts = []
    for col in actual_schema:
        if col in timestamp_columns:
            parts.append(f'TRY_CAST("{col}" AS TIMESTAMP) AS "{col}"')
        else:
            parts.append(f'"{col}"')
    return ", ".join(parts)


def check_timestamp_cast_failures(con, staging_uri, timestamp_columns):
    """Counts failed timestamp casts after Parquet conversion.

    Args:
        con: Open DuckDB connection.
        staging_uri: S3 URI for the converted Parquet object.
        timestamp_columns: Timestamp columns cast during conversion.

    Returns:
        Mapping of timestamp column to NULL count.
    """
    if not timestamp_columns:
        return {}

    null_counts = {}
    for col in timestamp_columns:
        count = con.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{staging_uri}')
            WHERE "{col}" IS NULL
        """).fetchone()[0]
        if count > 0:
            null_counts[col] = count
            print(
                f"  WARNING: {count} row(s) have a NULL '{col}' after TRY_CAST; "
                f"check source formatting for that column"
            )
    return null_counts


def convert_source_to_staging(
    con,
    source,
    expected_schema,
    execution_date,
    batch_id,
    raw_bucket="rawjson",
    staging_bucket="staging",
    manifest_path=SCHEMA_MANIFEST_PATH,
    timestamp_columns=None,
):
    timestamp_columns = timestamp_columns or []
    raw_uri = f"s3://{raw_bucket}/ingestion_date={execution_date}/{source}.json"
    staging_uri = (
        f"s3://{staging_bucket}/{source}/load_date={execution_date}/{source}.parquet"
    )
    result = {"source": source, "raw_uri": raw_uri}

    try:
        actual_schema = get_actual_schema(con, raw_uri)
    except (duckdb.Error, OSError, ValueError) as e:
        result.update(status="error", error=f"could not read raw file: {e}")
        return result

    is_breaking, drift = classify_drift(expected_schema, actual_schema)
    result["drift"] = drift

    if is_breaking:
        result["status"] = "halted_schema_drift"
        print(f"[{source}] BREAKING schema drift detected; load halted. {drift}")
        return result

    if drift["new_columns"]:
        print(
            f"[{source}] non-breaking drift: new column(s) {drift['new_columns']} "
            "accepted; schema_manifest.json will be updated"
        )

    select_clause = build_select_clause(actual_schema, timestamp_columns)

    try:
        con.execute(f"""
            COPY (
                SELECT {select_clause},
                       '{source}.json'  AS _source_file,
                       current_timestamp AS _loaded_at,
                       '{batch_id}'      AS _batch_id
                FROM read_json_auto('{raw_uri}', hive_partitioning=false)
            ) TO '{staging_uri}' (FORMAT PARQUET);
        """)
        result["status"] = "success"
        result["staging_uri"] = staging_uri
        print(f"[{source}] converted -> {staging_uri}")

        cast_failures = check_timestamp_cast_failures(
            con, staging_uri, timestamp_columns
        )
        if cast_failures:
            result["timestamp_cast_failures"] = cast_failures

        # Record the accepted schema so subsequent runs compare against the
        # latest approved contract.
        if drift["new_columns"] or drift["type_changes"]:
            update_schema_manifest(source, actual_schema, path=manifest_path)

    except (duckdb.Error, OSError, ValueError) as e:
        result["status"] = "error"
        result["error"] = str(e)
        print(f"[{source}] FAILED conversion: {e}")

    return result


def write_manifest(client, manifest, execution_date, bucket="staging"):
    object_name = f"_manifest/load_date={execution_date}/conversion_manifest.json"
    body = json.dumps(manifest, indent=2, default=str).encode("utf-8")
    ensure_bucket(client, bucket)
    client.put_object(
        bucket,
        object_name,
        io.BytesIO(body),
        length=len(body),
        content_type="application/json",
    )
    return f"s3://{bucket}/{object_name}"


def read_staging_all_partitions(con, source, staging_bucket="staging"):
    """Reads all Parquet load-date partitions for a source.

    Args:
        con: Open DuckDB connection.
        source: Source name.
        staging_bucket: MinIO bucket containing staged Parquet objects.

    Returns:
        Pandas DataFrame with partitions unioned by column name.
    """
    uri = f"s3://{staging_bucket}/{source}/load_date=*/{source}.parquet"
    return con.execute(
        f"SELECT * FROM read_parquet('{uri}', union_by_name=true)"
    ).fetchdf()


def normalize_sources(sources=None):
    if sources is None:
        return SOURCES
    selected = [source.strip() for source in sources if source and source.strip()]
    unknown = [source for source in selected if source not in SOURCES]
    if unknown:
        raise ValueError(f"Unsupported raw source(s): {unknown}")
    if not selected:
        raise ValueError("At least one raw source is required.")
    return selected


def run_raw_pipeline(
    execution_date=None, raw_bucket="rawjson", staging_bucket="staging", sources=None
):
    execution_date = execution_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    batch_id = deterministic_batch_id("raw", execution_date)

    con = get_duckdb_connection(db_file="")
    client = get_minio_client()
    try:
        schema_manifest = load_schema_manifest()

        selected_sources = normalize_sources(sources)
        print(f"Raw conversion sources: {', '.join(selected_sources)}")

        results = [
            convert_source_to_staging(
                con,
                source,
                schema_manifest.get(source, {}),
                execution_date,
                batch_id,
                raw_bucket=raw_bucket,
                staging_bucket=staging_bucket,
                timestamp_columns=TIMESTAMP_COLUMNS.get(source, []),
            )
            for source in selected_sources
        ]

        manifest = {
            "execution_date": execution_date,
            "batch_id": batch_id,
            "run_completed_at": datetime.now(timezone.utc).isoformat(),
            "results": results,
            "status": "success"
            if all(r["status"] == "success" for r in results)
            else "partial_failure",
        }
        manifest_path = write_manifest(
            client, manifest, execution_date, bucket=staging_bucket
        )
        print(f"\nConversion manifest written: {manifest_path} ({manifest['status']})")

        failures = [r for r in results if r["status"] != "success"]
        if failures:
            raise RuntimeError(
                f"{len(failures)} source(s) did not convert cleanly: "
                f"{[(r['source'], r['status']) for r in failures]}"
            )
    finally:
        con.close()
