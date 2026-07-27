"""Run and validate the Eat N' Go BI pipeline with structured logs.

Default mode is local and self-contained:
  python scripts/run_pipeline.py --mode local --fresh

JSON object-store mode uses the existing MinIO-backed file pipeline:
  python scripts/run_pipeline.py --mode json --execution-date 2026-07-25

API mode extracts configured API sources before running the shared warehouse path:
  python scripts/run_pipeline.py --mode api --api-provider delivery_partner --execution-date 2026-07-25
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "warehouse" / "eat_ngo_dw.duckdb"
DEFAULT_LOG_DIR = PROJECT_ROOT / "logs" / "pipeline_runs"
WAREHOUSE_SQL_FILES = [
    PROJECT_ROOT / "warehouse" / "store_scd.sql",
    PROJECT_ROOT / "warehouse" / "marts.sql",
    PROJECT_ROOT / "warehouse" / "core.sql",
    PROJECT_ROOT / "warehouse" / "data_quality.sql",
    PROJECT_ROOT / "warehouse" / "reconciliation.sql",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the Eat N' Go BI pipeline and validation checks."
    )
    parser.add_argument(
        "--mode",
        choices=["local", "json", "api", "object-store"],
        default="local",
        help="local builds from files directly; json uses the MinIO JSON flow; api uses configured API extraction.",
    )
    parser.add_argument(
        "--db",
        default=str(DEFAULT_DB_PATH),
        help="DuckDB database file to build or validate.",
    )
    parser.add_argument(
        "--execution-date",
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Logical pipeline date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Stable run identifier for audit history. Defaults to '<mode>-<execution-date>'.",
    )
    parser.add_argument(
        "--source-dir",
        default=str(PROJECT_ROOT / "dataSource"),
        help="Directory containing the six source JSON files for local mode.",
    )
    parser.add_argument(
        "--log-dir",
        default=str(DEFAULT_LOG_DIR),
        help="Directory where run logs are written.",
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Delete the target DuckDB file before running.",
    )
    parser.add_argument(
        "--api-provider",
        default=None,
        help="Provider name for API mode, e.g. delivery_partner.",
    )
    parser.add_argument(
        "--api-source",
        default=None,
        help="Logical API source name, e.g. deliveries_api.",
    )
    parser.add_argument(
        "--api-target-source",
        default=None,
        help="Downstream source contract to land, e.g. deliveries.",
    )
    parser.add_argument("--api-base-url", default=None, help="Provider API base URL.")
    parser.add_argument(
        "--api-token-path", default=None, help="OAuth token endpoint path."
    )
    parser.add_argument("--api-data-path", default=None, help="Data endpoint path.")
    parser.add_argument(
        "--api-count-path",
        default=None,
        help="Optional provider count endpoint for reconciliation.",
    )
    parser.add_argument(
        "--api-client-id", default=None, help="OAuth client id for API mode."
    )
    parser.add_argument(
        "--api-client-secret", default=None, help="OAuth client secret for API mode."
    )
    parser.add_argument(
        "--api-bearer-token",
        default=None,
        help="Bearer token when --api-auth-type=bearer.",
    )
    parser.add_argument(
        "--api-auth-type",
        choices=["oauth_client_credentials", "bearer", "none"],
        default=None,
        help="Authentication strategy for API mode.",
    )
    parser.add_argument(
        "--api-store-ids",
        default=None,
        help="Optional comma-separated store ids. Omit to pull all stores from the provider.",
    )
    parser.add_argument(
        "--api-data-key",
        default=None,
        help="Response key containing records. Default: data.",
    )
    parser.add_argument(
        "--api-has-more-key",
        default=None,
        help="Response key indicating another page. Default: has_more.",
    )
    parser.add_argument(
        "--api-total-key",
        default=None,
        help="Count response key for vendor total. Default: total.",
    )
    parser.add_argument(
        "--api-pagination-style",
        choices=["page", "cursor"],
        default=None,
        help="Provider pagination style. Default: page.",
    )
    parser.add_argument(
        "--api-page-param-name",
        default=None,
        help="Provider query parameter for page number. Default: page.",
    )
    parser.add_argument(
        "--api-page-size-param-name",
        default=None,
        help="Provider query parameter for page size. Default: page_size.",
    )
    parser.add_argument(
        "--api-cursor-param-name",
        default=None,
        help="Provider query parameter for cursor pagination. Default: cursor.",
    )
    parser.add_argument(
        "--api-cursor-response-key",
        default=None,
        help="Response key containing the next cursor. Dot paths are allowed.",
    )
    parser.add_argument(
        "--api-store-param-name",
        default=None,
        help="Provider query parameter for store filter. Default: store_id.",
    )
    parser.add_argument(
        "--api-interval-start-param",
        default=None,
        help="Provider query parameter for interval start. Default: updated_since.",
    )
    parser.add_argument(
        "--api-interval-end-param",
        default=None,
        help="Provider query parameter for interval end. Default: updated_until.",
    )
    parser.add_argument(
        "--api-record-id-field",
        default=None,
        help="Optional record id field used to dedupe overlap records.",
    )
    parser.add_argument(
        "--api-filter",
        action="append",
        default=None,
        help="Extra provider query filter in key=value form. Can be repeated.",
    )
    parser.add_argument(
        "--api-interval-start",
        default=None,
        help="Inclusive API interval start, e.g. 2026-07-25T00:00:00Z.",
    )
    parser.add_argument(
        "--api-interval-end",
        default=None,
        help="Exclusive API interval end, e.g. 2026-07-25T01:00:00Z.",
    )
    parser.add_argument(
        "--api-lookback-minutes",
        type=int,
        default=10,
        help="Watermark overlap for late provider updates. Default: 10 minutes.",
    )
    parser.add_argument(
        "--api-chunk-minutes",
        type=int,
        default=60,
        help="Maximum API interval chunk size. Default: 60 minutes.",
    )
    return parser.parse_args()


def configure_logging(log_dir: Path) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_path = log_dir / f"pipeline_run_{run_id}.log"

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    return log_path


def require_source_files(source_dir: Path) -> None:
    from pipeline.json_source_state import EMPTY, MISSING, inspect_json_source_drop

    inspection = inspect_json_source_drop(source_dir)
    if inspection.status in {MISSING, EMPTY}:
        raise FileNotFoundError(f"No complete JSON source drop found in {source_dir}")


def reset_database_if_requested(db_path: Path, fresh: bool) -> None:
    if not fresh:
        return
    for path in [db_path, db_path.with_suffix(db_path.suffix + ".wal")]:
        if path.exists():
            LOGGER.info("Deleting existing database artifact: %s", path)
            path.unlink()


def execute_sql_file(con: duckdb.DuckDBPyConnection, sql_path: Path) -> None:
    LOGGER.info("Running SQL file: %s", sql_path.relative_to(PROJECT_ROOT))
    con.execute(sql_path.read_text(encoding="utf-8"))


def build_local_staging(
    con: duckdb.DuckDBPyConnection,
    source_dir: Path,
    execution_date: str,
    run_id: str,
) -> None:
    LOGGER.info("Building local staging tables from %s", source_dir)

    def source(name: str) -> str:
        return str((source_dir / name).resolve()).replace("'", "''")

    load_timestamp = f"{execution_date} 00:00:00"
    sql_run_id = run_id.replace("'", "''")

    con.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE staging.int_customers AS
        SELECT *, 'customers.json' AS _source_file, TIMESTAMP '{load_timestamp}' AS _loaded_at, '{sql_run_id}' AS _batch_id
        FROM read_json_auto('{source("customers.json")}', hive_partitioning=false);
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE staging.int_stores AS
        SELECT *, TRIM(city) AS clean_city, 'stores.json' AS _source_file, TIMESTAMP '{load_timestamp}' AS _loaded_at, '{sql_run_id}' AS _batch_id
        FROM read_json_auto('{source("stores.json")}', hive_partitioning=false);
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE staging.int_products AS
        SELECT *, COALESCE(category, 'Uncategorized') AS unified_category,
               'products.json' AS _source_file, TIMESTAMP '{load_timestamp}' AS _loaded_at, '{sql_run_id}' AS _batch_id
        FROM read_json_auto('{source("products.json")}', hive_partitioning=false);
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE staging.int_orders AS
        WITH src AS (
            SELECT *, TIMESTAMP '{load_timestamp}' AS _loaded_at
            FROM read_json_auto('{source("orders.json")}', hive_partitioning=false)
        ),
        ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY CAST(order_ts AS TIMESTAMP) ASC, order_id ASC) AS order_rank
            FROM src
        )
        SELECT
            order_id, customer_id, store_id, channel, payment_method, promo_code,
            CAST(order_ts AS TIMESTAMP) AS order_timestamp,
            items_count, qty_total, subtotal_ngn, discount_ngn, tax_ngn,
            delivery_fee_ngn, total_amount_ngn,
            YEAR(CAST(order_ts AS TIMESTAMP)) AS order_year,
            MONTH(CAST(order_ts AS TIMESTAMP)) AS order_month,
            WEEK(CAST(order_ts AS TIMESTAMP)) AS order_week,
            DAYOFWEEK(CAST(order_ts AS TIMESTAMP)) AS order_day_of_week,
            HOUR(CAST(order_ts AS TIMESTAMP)) AS order_hour,
            CASE WHEN promo_code IS NOT NULL AND promo_code != '' THEN 1 ELSE 0 END AS promo_flag,
            CASE WHEN order_rank = 1 THEN 'New' ELSE 'Returning' END AS new_vs_returning_customer_flag,
            CASE WHEN order_rank = 1 THEN 'First-Time Buyer' ELSE 'Loyal Customer' END AS customer_type,
            'orders.json' AS _source_file,
            _loaded_at,
            '{sql_run_id}' AS _batch_id
        FROM ranked;
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE staging.int_deliveries AS
        SELECT
            order_id,
            store_id,
            CAST(order_ts AS TIMESTAMP) AS order_timestamp,
            promised_delivery_minutes,
            actual_delivery_minutes,
            (actual_delivery_minutes - promised_delivery_minutes) AS delay_minutes,
            delivered_within_sla AS delivery_sla_met_flag,
            'deliveries.json' AS _source_file,
            TIMESTAMP '{load_timestamp}' AS _loaded_at,
            '{sql_run_id}' AS _batch_id
        FROM read_json_auto('{source("deliveries.json")}', hive_partitioning=false);
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE staging.int_order_items AS
        SELECT
            i.order_id, i.product_id, i.product_name, i.category,
            i.qty AS quantity,
            i.unit_price_ngn AS unit_price,
            i.line_total_ngn AS product_revenue,
            o.store_id, o.channel, o.order_timestamp, o.promo_flag,
            'order_items.json' AS _source_file,
            TIMESTAMP '{load_timestamp}' AS _loaded_at,
            '{sql_run_id}' AS _batch_id
        FROM read_json_auto('{source("order_items.json")}', hive_partitioning=false) i
        LEFT JOIN staging.int_orders o ON i.order_id = o.order_id;
        """
    )
    LOGGER.info("Local staging build complete.")


def run_json_source_pipeline(execution_date: str, source_dir: Path) -> None:
    LOGGER.info("Running MinIO-backed JSON source phases for %s", execution_date)
    from pipeline.landing import load_json_data_to_minio
    from pipeline.raw import run_raw_pipeline
    from pipeline.staging import run_staging_pipeline

    load_json_data_to_minio(
        bucket_name="rawjson",
        source_path=str(source_dir),
        execution_date=execution_date,
    )
    run_raw_pipeline(execution_date=execution_date)
    run_staging_pipeline(execution_date=execution_date)


def run_api_source_pipeline(
    execution_date: str, args: argparse.Namespace
) -> dict[str, object]:
    LOGGER.info("Running API source phases for %s", execution_date)
    from pipeline.api_ingest import build_api_source_config, run_api_extraction
    from pipeline.raw import run_raw_pipeline
    from pipeline.staging import run_staging_pipeline

    config = build_api_source_config(
        provider_name=args.api_provider,
        source_name=args.api_source,
        target_source=args.api_target_source,
        base_url=args.api_base_url,
        token_path=args.api_token_path,
        data_path=args.api_data_path,
        count_path=args.api_count_path,
        client_id=args.api_client_id,
        client_secret=args.api_client_secret,
        bearer_token=args.api_bearer_token,
        auth_type=args.api_auth_type,
        store_ids=args.api_store_ids,
        data_key=args.api_data_key,
        has_more_key=args.api_has_more_key,
        total_key=args.api_total_key,
        pagination_style=args.api_pagination_style,
        page_param_name=args.api_page_param_name,
        page_size_param_name=args.api_page_size_param_name,
        cursor_param_name=args.api_cursor_param_name,
        cursor_response_key=args.api_cursor_response_key,
        store_param_name=args.api_store_param_name,
        interval_start_param_name=args.api_interval_start_param,
        interval_end_param_name=args.api_interval_end_param,
        extra_params=args.api_filter,
        record_id_field=args.api_record_id_field,
    )
    manifest = run_api_extraction(
        config,
        execution_date=execution_date,
        interval_start=args.api_interval_start,
        interval_end=args.api_interval_end,
        lookback_minutes=args.api_lookback_minutes,
        chunk_minutes=args.api_chunk_minutes,
    )
    if int(manifest.get("records_after_dedupe", 0)) == 0:
        LOGGER.info("API extraction returned no records; skipping warehouse rebuild.")
        return manifest
    run_raw_pipeline(execution_date=execution_date, sources=[config.target_source])
    run_staging_pipeline(execution_date=execution_date)
    return manifest


def run_warehouse_sql(
    con: duckdb.DuckDBPyConnection, execution_date: str, run_id: str
) -> None:
    con.execute(f"SET VARIABLE exec_date = DATE '{execution_date}';")
    sql_run_id = run_id.replace("'", "''")
    con.execute(f"SET VARIABLE run_id = '{sql_run_id}';")
    for sql_file in WAREHOUSE_SQL_FILES:
        execute_sql_file(con, sql_file)


def fetch_single_value(con: duckdb.DuckDBPyConnection, query: str) -> int:
    return int(con.execute(query).fetchone()[0])


def log_validation_summary(con: duckdb.DuckDBPyConnection) -> None:
    LOGGER.info("Row counts:")
    for schema, table in [
        ("staging", "int_orders"),
        ("staging", "int_order_items"),
        ("staging", "int_deliveries"),
        ("marts", "fact_orders"),
        ("marts", "fact_order_items"),
        ("marts", "fact_deliveries"),
    ]:
        count = fetch_single_value(con, f"SELECT COUNT(*) FROM {schema}.{table}")
        LOGGER.info("  %s.%s = %s", schema, table, count)

    LOGGER.info("Data quality result summary:")
    for status, count in con.execute(
        "SELECT status, COUNT(*) FROM audit.data_quality_results GROUP BY status ORDER BY status"
    ).fetchall():
        LOGGER.info("  %s = %s", status, count)

    LOGGER.info("Exception summary:")
    exception_rows = con.execute(
        """
        SELECT check_name, severity, exception_count
        FROM audit.vw_data_quality_exception_summary
        ORDER BY check_name
        """
    ).fetchall()
    if not exception_rows:
        LOGGER.info("  no row-level exceptions")
    for check_name, severity, count in exception_rows:
        LOGGER.info("  %s [%s] = %s", check_name, severity, count)

    LOGGER.info("Reconciliation summary:")
    for status, count in con.execute(
        "SELECT status, COUNT(*) FROM audit.reconciliation_summary GROUP BY status ORDER BY status"
    ).fetchall():
        LOGGER.info("  %s = %s", status, count)


def assert_pipeline_passed(con: duckdb.DuckDBPyConnection) -> None:
    failed_quality = con.execute(
        "SELECT check_name FROM audit.data_quality_results WHERE status = 'FAIL' ORDER BY check_name"
    ).fetchall()
    failed_reconciliation = con.execute(
        "SELECT reconciliation_level, metric, dimension_key FROM audit.reconciliation_summary WHERE status = 'FAIL' ORDER BY 1, 2, 3"
    ).fetchall()
    error_exceptions = con.execute(
        "SELECT check_name, COUNT(*) FROM audit.data_quality_exceptions WHERE severity = 'ERROR' GROUP BY check_name ORDER BY check_name"
    ).fetchall()

    if failed_quality or failed_reconciliation or error_exceptions:
        LOGGER.error("Failed quality checks: %s", failed_quality)
        LOGGER.error("Failed reconciliation rows: %s", failed_reconciliation[:25])
        LOGGER.error("Error exceptions: %s", error_exceptions)
        raise RuntimeError("Pipeline validation failed. Check the run log for details.")


def main() -> int:
    args = parse_args()
    db_path = Path(args.db).resolve()
    source_dir = Path(args.source_dir).resolve()
    log_path = configure_logging(Path(args.log_dir).resolve())
    normalized_mode = "json" if args.mode == "object-store" else args.mode
    run_id = args.run_id or f"{normalized_mode}-{args.execution_date}"

    LOGGER.info("Pipeline run started.")
    LOGGER.info("Mode: %s", args.mode)
    LOGGER.info("Database: %s", db_path)
    LOGGER.info("Execution date: %s", args.execution_date)
    LOGGER.info("Run id: %s", run_id)
    LOGGER.info("Log file: %s", log_path)
    json_source_inspection = None
    api_manifest = None

    try:
        if normalized_mode == "local":
            require_source_files(source_dir)
        elif normalized_mode == "json":
            from pipeline.json_source_state import (
                EMPTY,
                MISSING,
                UNCHANGED,
                inspect_json_source_drop,
                mark_json_source_processed,
            )

            json_state_path = (
                db_path.parent / "source_state" / "json_source_fingerprint.json"
            )
            json_source_inspection = inspect_json_source_drop(
                source_dir,
                state_path=json_state_path,
            )
            LOGGER.info("JSON source status: %s", json_source_inspection.status)
            if json_source_inspection.status in {MISSING, EMPTY} or (
                json_source_inspection.status == UNCHANGED and not args.fresh
            ):
                LOGGER.info(
                    "Skipping JSON pipeline. Source status=%s, state=%s",
                    json_source_inspection.status,
                    json_source_inspection.state_path,
                )
                return 0
            if json_source_inspection.status == UNCHANGED and args.fresh:
                LOGGER.info("Rebuilding unchanged JSON source because --fresh was set.")

        reset_database_if_requested(db_path, args.fresh)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        if normalized_mode in {"json", "api"}:
            os.environ["WAREHOUSE_DB"] = str(db_path)
            if normalized_mode == "json":
                run_json_source_pipeline(args.execution_date, source_dir)
            else:
                api_manifest = run_api_source_pipeline(args.execution_date, args)
                if int(api_manifest.get("records_after_dedupe", 0)) == 0:
                    LOGGER.info("Pipeline run completed successfully with no API data.")
                    return 0

        con = duckdb.connect(str(db_path))
        try:
            if normalized_mode == "local":
                build_local_staging(con, source_dir, args.execution_date, run_id)

            run_warehouse_sql(con, args.execution_date, run_id)
            log_validation_summary(con)
            assert_pipeline_passed(con)
        finally:
            con.close()

        if (
            normalized_mode == "json"
            and json_source_inspection is not None
            and json_source_inspection.status != UNCHANGED
        ):
            mark_json_source_processed(
                json_source_inspection,
                execution_date=args.execution_date,
                run_id=run_id,
            )
            LOGGER.info("JSON source fingerprint marked processed.")

        LOGGER.info("Pipeline run completed successfully.")
        return 0
    except Exception:
        LOGGER.exception("Pipeline run failed.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
