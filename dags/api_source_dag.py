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

from pipeline.api_ingest import DELIVERIES_API_CONFIG, run_api_extraction
from pipeline.audit import assert_audit_passed
from pipeline.raw import run_raw_pipeline
from pipeline.staging import run_staging_pipeline

with DAG(
    dag_id="eat_ngo_api_source_pipeline",
    start_date=datetime(2026, 7, 25, tzinfo=timezone.utc),
    schedule=schedule_from_env("EAT_NGO_API_SCHEDULE"),
    catchup=False,
    tags=["eat_ngo", "warehouse", "source:api"],
    default_args=default_args(),
) as dag:

    @task(task_id="inspect_api_source")
    def inspect_api_source():
        """Inspect whether the API provider source is configured."""
        if DELIVERIES_API_CONFIG is None:
            return {
                "status": "missing_config",
                "message": "Set DELIVERY_API_BASE_URL or API_BASE_URL to enable it.",
            }
        return {
            "status": "ready",
            "provider_name": DELIVERIES_API_CONFIG.provider_name,
            "source_name": DELIVERIES_API_CONFIG.source_name,
            "target_source": DELIVERIES_API_CONFIG.target_source,
            "base_url": DELIVERIES_API_CONFIG.base_url,
        }

    @task.branch(task_id="choose_api_source")
    def choose_api_source(source_inspection):
        """Continue only when an API provider has been configured."""
        if source_inspection["status"] == "missing_config":
            print(
                "No API source configured; skipping this run. "
                f"{source_inspection['message']}"
            )
            return "skip_api_source"

        print(
            "API source configured for "
            f"{DELIVERIES_API_CONFIG.provider_name}/{DELIVERIES_API_CONFIG.source_name}; "
            "continuing pipeline."
        )
        return "extract_api_source"

    @task(task_id="extract_api_source")
    def extract_api_source():
        """Pull the configured API source and land it into the raw JSON bucket."""
        if DELIVERIES_API_CONFIG is None:
            raise RuntimeError(
                "API source config is required before extraction can run."
            )

        context = get_current_context()
        return run_api_extraction(
            DELIVERIES_API_CONFIG,
            execution_date=context["ds"],
        )

    @task.branch(task_id="choose_api_extract_result")
    def choose_api_extract_result(api_manifest):
        """Continue warehouse work only when the API returned records."""
        if int(api_manifest.get("records_after_dedupe", 0)) == 0:
            print("API extraction completed with no records; skipping conversion.")
            return "skip_empty_api_extract"
        return "convert_api_json_to_parquet"

    @task(task_id="convert_api_json_to_parquet")
    def convert_api_json_to_parquet():
        """Schema-check the API-landed JSON and convert it to staging Parquet."""
        context = get_current_context()
        if DELIVERIES_API_CONFIG is None:
            raise RuntimeError("API source config is required before conversion.")
        return run_raw_pipeline(
            execution_date=context["ds"],
            sources=[DELIVERIES_API_CONFIG.target_source],
        )

    @task(task_id="build_staging_tables")
    def build_staging_tables():
        """Build cleaned, deduplicated staging tables from Parquet."""
        context = get_current_context()
        return run_staging_pipeline(execution_date=context["ds"])

    @task(task_id="validate_audit_results")
    def validate_audit_results():
        """Fail the DAG on hard data-quality or reconciliation failures."""
        assert_audit_passed()

    skip_api_source = EmptyOperator(task_id="skip_api_source")
    skip_empty_api_extract = EmptyOperator(task_id="skip_empty_api_extract")
    load_store_scd, load_marts, load_core, run_data_quality, run_reconciliation = (
        warehouse_tasks()
    )
    source_inspection = inspect_api_source()
    branch = choose_api_source(source_inspection)
    extracted = extract_api_source()
    extract_result_branch = choose_api_extract_result(extracted)
    converted = convert_api_json_to_parquet()
    staged = build_staging_tables()
    validated = validate_audit_results()
    success_email = send_success_summary_email(
        source_name="api",
        source_inspection=source_inspection,
    )

    branch >> [extracted, skip_api_source]
    extracted >> extract_result_branch
    extract_result_branch >> [converted, skip_empty_api_extract]
    [skip_api_source, skip_empty_api_extract, validated] >> success_email

    (
        converted
        >> staged
        >> load_store_scd
        >> load_marts
        >> load_core
        >> run_data_quality
        >> run_reconciliation
        >> validated
    )
