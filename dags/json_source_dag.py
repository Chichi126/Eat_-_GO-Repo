import os
from datetime import datetime, timezone

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import get_current_context, task
from common_pipeline import (
    default_args,
    schedule_from_env,
    send_success_summary_email,
    warehouse_tasks,
)

from pipeline.audit import assert_audit_passed
from pipeline.json_source_state import (
    EMPTY,
    MISSING,
    READY,
    UNCHANGED,
    inspect_json_source_drop,
    mark_json_source_processed,
)
from pipeline.landing import load_json_data_to_minio
from pipeline.raw import run_raw_pipeline
from pipeline.staging import run_staging_pipeline

JSON_SOURCE_PATH = "/opt/airflow/dataSource"


with DAG(
    dag_id="eat_ngo_json_source_pipeline",
    start_date=datetime(2026, 7, 25, tzinfo=timezone.utc),
    schedule=schedule_from_env("EAT_NGO_JSON_SCHEDULE", "0 8 * * *"),
    catchup=False,
    tags=["eat_ngo", "warehouse", "source:json"],
    default_args=default_args(),
) as dag:

    @task(task_id="inspect_json_source")
    def inspect_json_source():
        """Inspect the JSON drop and compare it with the last successful one."""
        inspection = inspect_json_source_drop(
            source_path=os.getenv("EAT_NGO_JSON_SOURCE_PATH", JSON_SOURCE_PATH),
            state_path=os.getenv("EAT_NGO_JSON_STATE_PATH"),
        )
        print(
            "JSON source inspection: "
            f"status={inspection.status}, fingerprint={inspection.fingerprint}, "
            f"state_path={inspection.state_path}"
        )
        return inspection.to_dict()

    @task.branch(task_id="choose_json_source")
    def choose_json_source(source_inspection):
        """Continue only when the JSON source has new data to process."""
        status = source_inspection["status"]
        if status in {MISSING, EMPTY, UNCHANGED}:
            print(f"Skipping JSON pipeline because source status is {status}.")
            return "skip_json_source"
        if status != READY:
            raise RuntimeError(f"Unsupported JSON source status: {status}")
        return "land_json_files"

    @task(task_id="land_json_files")
    def land_json_files():
        """Upload source JSON files into the raw MinIO bucket."""
        context = get_current_context()
        return load_json_data_to_minio(
            bucket_name="rawjson",
            source_path=os.getenv("EAT_NGO_JSON_SOURCE_PATH", JSON_SOURCE_PATH),
            execution_date=context["ds"],
        )

    @task(task_id="convert_json_to_parquet")
    def convert_json_to_parquet():
        """Schema-check raw JSON and convert it to staging Parquet."""
        context = get_current_context()
        return run_raw_pipeline(execution_date=context["ds"])

    @task(task_id="build_staging_tables")
    def build_staging_tables():
        """Build cleaned, deduplicated staging tables from Parquet."""
        context = get_current_context()
        return run_staging_pipeline(execution_date=context["ds"])

    @task(task_id="validate_audit_results")
    def validate_audit_results():
        """Fail the DAG on hard data-quality or reconciliation failures."""
        assert_audit_passed()

    @task(task_id="mark_json_source_processed")
    def mark_json_source_processed_task(source_inspection):
        """Save the source fingerprint after the full pipeline succeeds."""
        context = get_current_context()
        mark_json_source_processed(
            source_inspection,
            execution_date=context["ds"],
            run_id=context["run_id"],
        )

    skip_json_source = EmptyOperator(task_id="skip_json_source")
    load_store_scd, load_marts, load_core, run_data_quality, run_reconciliation = (
        warehouse_tasks()
    )
    source_inspection = inspect_json_source()
    branch = choose_json_source(source_inspection)
    landed = land_json_files()
    converted = convert_json_to_parquet()
    staged = build_staging_tables()
    validated = validate_audit_results()
    marked_processed = mark_json_source_processed_task(source_inspection)
    success_email = send_success_summary_email(
        source_name="json",
        source_inspection=source_inspection,
    )

    branch >> [landed, skip_json_source]
    [skip_json_source, marked_processed] >> success_email

    (
        landed
        >> converted
        >> staged
        >> load_store_scd
        >> load_marts
        >> load_core
        >> run_data_quality
        >> run_reconciliation
        >> validated
        >> marked_processed
    )
