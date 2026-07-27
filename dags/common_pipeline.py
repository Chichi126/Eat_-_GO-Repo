import os
from datetime import timedelta
from html import escape
from pathlib import Path

from airflow.providers.smtp.notifications.smtp import send_smtp_notification
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import get_current_context, task


def alert_emails():
    return [
        email.strip()
        for email in os.getenv("AIRFLOW_ALERT_EMAIL", "").split(",")
        if email.strip()
    ]


def default_args():
    emails = alert_emails()
    args = {
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    }
    if emails:
        args["on_failure_callback"] = send_smtp_notification(
            to=emails,
            subject="Eat N' Go Airflow task failed: {{ ti.dag_id }}.{{ ti.task_id }}",
            html_content=(
                "<p>DAG: {{ ti.dag_id }}</p>"
                "<p>Task: {{ ti.task_id }}</p>"
                "<p>Run: {{ run_id }}</p>"
                "<p>Log: <a href='{{ ti.log_url }}'>{{ ti.log_url }}</a></p>"
            ),
        )
    return args


def schedule_from_env(name, default=None):
    value = os.getenv(name)
    if value is None:
        value = default
    if value is None or value.strip().lower() in {"", "none", "manual"}:
        return None
    return value


def duckdb_read_command(sql_file):
    return (
        'duckdb "$WAREHOUSE_DB" '
        "-c \"SET VARIABLE exec_date = '{{ ds }}'::DATE;\" "
        "-c \"SET VARIABLE run_id = '{{ run_id }}';\" "
        f'-c ".read /opt/airflow/warehouse/{sql_file}"'
    )


def _query_pairs(con, sql):
    return {str(key): int(count) for key, count in con.execute(sql).fetchall()}


def _query_rows(con, sql):
    return con.execute(sql).fetchall()


def _warehouse_summary(warehouse_db):
    if not warehouse_db or not Path(warehouse_db).is_file():
        return {
            "available": False,
            "message": f"Warehouse DB not found at {warehouse_db or '<unset>'}.",
        }

    import duckdb

    try:
        with duckdb.connect(str(warehouse_db), read_only=True) as con:
            row_counts = _query_rows(
                con,
                """
                SELECT 'staging.int_orders' AS table_name, COUNT(*) AS row_count FROM staging.int_orders
                UNION ALL SELECT 'staging.int_order_items', COUNT(*) FROM staging.int_order_items
                UNION ALL SELECT 'staging.int_deliveries', COUNT(*) FROM staging.int_deliveries
                UNION ALL SELECT 'marts.fact_orders', COUNT(*) FROM marts.fact_orders
                UNION ALL SELECT 'marts.fact_order_items', COUNT(*) FROM marts.fact_order_items
                UNION ALL SELECT 'marts.fact_deliveries', COUNT(*) FROM marts.fact_deliveries
                ORDER BY table_name;
                """,
            )
            quality = _query_pairs(
                con,
                """
                SELECT status, COUNT(*)
                FROM audit.data_quality_results
                GROUP BY status
                ORDER BY status;
                """,
            )
            reconciliation = _query_pairs(
                con,
                """
                SELECT status, COUNT(*)
                FROM audit.reconciliation_summary
                GROUP BY status
                ORDER BY status;
                """,
            )
            exceptions = _query_rows(
                con,
                """
                SELECT check_name, severity, exception_count
                FROM audit.vw_data_quality_exception_summary
                ORDER BY severity, check_name
                LIMIT 10;
                """,
            )
    except duckdb.Error as exc:
        return {"available": False, "message": f"Warehouse summary unavailable: {exc}"}

    return {
        "available": True,
        "row_counts": row_counts,
        "quality": quality,
        "reconciliation": reconciliation,
        "exceptions": exceptions,
    }


def _html_table(headers, rows):
    header_html = "".join(f"<th>{escape(str(header))}</th>" for header in headers)
    row_html = "".join(
        "<tr>" + "".join(f"<td>{escape(str(value))}</td>" for value in row) + "</tr>"
        for row in rows
    )
    return (
        "<table border='1' cellpadding='6' cellspacing='0'>"
        f"<thead><tr>{header_html}</tr></thead>"
        f"<tbody>{row_html}</tbody>"
        "</table>"
    )


def _render_success_email_html(context, source_name, run_outcome, warehouse_summary):
    task_instance = context["ti"]
    rows = [
        ("DAG", task_instance.dag_id),
        ("Task", task_instance.task_id),
        ("Run ID", context["run_id"]),
        ("Execution date", context["ds"]),
        ("Source", source_name),
        ("Outcome", run_outcome),
        ("Log", task_instance.log_url),
    ]
    html = [
        "<h3>Eat N' Go pipeline run succeeded</h3>",
        _html_table(["Field", "Value"], rows),
    ]

    if not warehouse_summary["available"]:
        html.append(f"<p>{escape(warehouse_summary['message'])}</p>")
        return "".join(html)

    html.append("<h4>Warehouse Row Counts</h4>")
    html.append(_html_table(["Table", "Rows"], warehouse_summary["row_counts"]))

    html.append("<h4>Data Quality</h4>")
    html.append(
        _html_table(["Status", "Checks"], sorted(warehouse_summary["quality"].items()))
    )

    html.append("<h4>Reconciliation</h4>")
    html.append(
        _html_table(
            ["Status", "Rows"], sorted(warehouse_summary["reconciliation"].items())
        )
    )

    exceptions = warehouse_summary["exceptions"]
    html.append("<h4>Top Exceptions</h4>")
    if exceptions:
        html.append(_html_table(["Check", "Severity", "Rows"], exceptions))
    else:
        html.append("<p>No row-level exceptions were recorded.</p>")

    return "".join(html)


def _json_run_outcome(source_inspection):
    if not source_inspection:
        return "processed"
    status = source_inspection.get("status", "unknown")
    if status == "ready":
        return "processed new JSON source drop"
    if status == "unchanged":
        return "skipped because JSON source files were unchanged"
    if status == "empty":
        return "skipped because no JSON files were present"
    if status == "missing":
        return "skipped because the JSON source folder was missing"
    return f"completed with source status {status}"


def _api_run_outcome(source_inspection):
    if source_inspection and source_inspection.get("status") == "missing_config":
        return "skipped because no API provider source was configured"
    return "processed configured API source"


@task(task_id="send_success_summary_email", trigger_rule="none_failed_min_one_success")
def send_success_summary_email(
    source_name,
    source_inspection=None,
):
    emails = alert_emails()
    if not emails:
        print("AIRFLOW_ALERT_EMAIL is not set; success summary email was not sent.")
        return {"status": "skipped", "reason": "no_recipients"}

    context = get_current_context()
    run_outcome = (
        _json_run_outcome(source_inspection)
        if source_name == "json"
        else _api_run_outcome(source_inspection)
    )
    warehouse_summary = _warehouse_summary(os.getenv("WAREHOUSE_DB"))
    html_content = _render_success_email_html(
        context=context,
        source_name=source_name,
        run_outcome=run_outcome,
        warehouse_summary=warehouse_summary,
    )
    subject = f"Eat N' Go pipeline succeeded: {context['ti'].dag_id} ({context['ds']})"
    send_smtp_notification(
        to=emails,
        subject=subject,
        html_content=html_content,
    ).notify(context)
    print(f"Success summary email sent to {', '.join(emails)}.")
    return {"status": "sent", "recipients": emails}


def warehouse_tasks():
    load_store_scd = BashOperator(
        task_id="load_store_scd",
        bash_command=duckdb_read_command("store_scd.sql"),
    )
    load_marts = BashOperator(
        task_id="load_marts",
        bash_command=duckdb_read_command("marts.sql"),
    )
    load_core = BashOperator(
        task_id="load_core",
        bash_command=duckdb_read_command("core.sql"),
    )
    run_data_quality = BashOperator(
        task_id="run_data_quality_checks",
        bash_command=duckdb_read_command("data_quality.sql"),
    )
    run_reconciliation = BashOperator(
        task_id="run_reconciliation",
        bash_command=duckdb_read_command("reconciliation.sql"),
    )
    return load_store_scd, load_marts, load_core, run_data_quality, run_reconciliation
