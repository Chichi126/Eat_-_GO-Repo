"""Operational gate for the quality and reconciliation audit tables."""

import os

import duckdb


def assert_audit_passed(db_file=None):
    """Raise after audit scripts run so Airflow records and alerts on failures."""
    db_file = db_file or os.getenv("WAREHOUSE_DB")
    if not db_file:
        raise RuntimeError(
            "WAREHOUSE_DB must be configured before validating audit results"
        )

    con = duckdb.connect(db_file, read_only=True)
    try:
        failed_quality = con.execute(
            "SELECT check_name FROM audit.data_quality_results WHERE status = 'FAIL' ORDER BY check_name"
        ).fetchall()
        failed_reconciliation = con.execute(
            "SELECT metric FROM audit.reconciliation_summary WHERE status = 'FAIL' ORDER BY metric"
        ).fetchall()
        failed_exception_counts = con.execute(
            """
            SELECT check_name, COUNT(*) AS exception_count
            FROM audit.data_quality_exceptions
            WHERE severity = 'ERROR'
            GROUP BY check_name
            ORDER BY check_name
            """
        ).fetchall()
    finally:
        con.close()

    if failed_quality or failed_reconciliation or failed_exception_counts:
        details = {
            "quality_checks": [row[0] for row in failed_quality],
            "reconciliation_metrics": [row[0] for row in failed_reconciliation],
            "error_exception_counts": {
                row[0]: row[1] for row in failed_exception_counts
            },
        }
        raise RuntimeError(f"Audit validation failed: {details}")
