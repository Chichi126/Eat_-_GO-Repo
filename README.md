# Eat'N'Go BI Engineering Pipeline

Local batch pipeline for multi-brand restaurant sales, customer, store, product, and delivery reporting.

The project turns six operational JSON files, with an optional provider API feed, into an auditable DuckDB reporting warehouse with staging tables, dimensional marts, BI-facing views, data-quality checks, reconciliation, orchestration, and run logs.

## Business Context

Eat'N'Go Limited operates Domino's Pizza, Cold Stone Creamery, and Pinkberry in Nigeria. The warehouse uses the following brand values:

- Domino's Pizza Nigeria
- Cold Stone Creamery Nigeria
- Pinkberry Nigeria

## Design Intent

This repository is designed for a single-node BI engineering workflow where the main priorities are:

- deterministic reruns
- explicit source validation
- separate source ingestion paths for JSON and API feeds
- clear raw, staging, mart, and audit boundaries
- inspectable data-quality and reconciliation results
- low setup overhead for local runs and Airflow testing

The scope is batch-oriented. The API path is provider-configurable, but this repository does not include a real vendor API contract.

## Outputs

The reporting outputs are exposed through the `core` schema:

| View | Purpose |
| --- | --- |
| `core.vw_sales_performance` | Order, revenue, channel, payment, promotion, and customer-type reporting |
| `core.vw_delivery_performance` | Delivery SLA, delay, store, and date reporting |
| `core.vw_product_performance` | Product/category quantity and revenue reporting |
| `core.vw_customer_summary` | Customer order history, spend, and recency reporting |

The underlying dimensional model is kept in `marts` so the reporting views can be traced back to grain-level facts.

Expected result from the current sample data:

| Area | Result |
| --- | --- |
| `marts.fact_orders` | 1,000 rows |
| `marts.fact_order_items` | 1,733 rows |
| `marts.fact_deliveries` | 454 rows |
| Data quality | 39 pass, 2 warn, 0 fail |
| Reconciliation | 80 pass, 0 fail |

## Business Rules

- `order_ts` is the authoritative business timestamp for sales, product, and delivery reporting.
- `fact_orders` is order grain and supports order count, revenue, channel, payment, promotion, and customer-type analysis.
- `fact_order_items` is order/product grain and carries its own date, store, and channel keys to avoid product metrics depending on order-level joins.
- `fact_deliveries` is delivery grain and uses promised minutes, actual minutes, and SLA status for delivery reporting.
- Promo codes with zero discount and unusual daily order-volume changes are warning exceptions. They remain visible for business investigation but do not block the load.
- Hard failures such as missing critical keys, broken relationships, invalid totals, and invalid delivery values block the pipeline.

## Design Trade-Offs

- DuckDB is used because the dataset fits local analytical processing and benefits from a file-backed warehouse.
- SQL owns the core transformations so model logic remains inspectable without stepping through Python.
- MinIO is used in the containerized path to show a raw/staging object-store boundary without introducing cloud infrastructure.
- JSON and API sources are kept separate at ingestion. Both paths converge only after source data has landed as raw JSON.
- API extraction stores provider response pages under `rawapi` and writes normalized source-contract JSON under `rawjson` for the shared converter.
- The store dimension is implemented as SCD Type 2. The current tracked attribute is `city`; additional attributes should be added only when the business meaning is confirmed.
- Run status is represented through Airflow task state, optional email, and audit tables.

## Architecture

Mermaid source and rendered diagrams are in:

- `docs/eat_ngo_pipeline_architecture.md`
- `docs/eat_ngo_architecture_diagram.svg`
- `docs/eat_ngo_data_model_diagram.svg`

Pipeline stages:

1. Validate the source boundary.
2. Land JSON files or API payloads into MinIO.
3. Convert raw JSON to typed Parquet.
4. Build canonical staging tables.
5. Build SCD dimensions, facts, and BI views.
6. Run data-quality checks and row-level exception capture.
7. Run reconciliation checks.
8. Persist run history and send optional Airflow email notifications.

## System Guarantees

- JSON runs require the full six-file source drop before loading.
- JSON files are fingerprinted by content, size, and expected file set.
- A JSON fingerprint is marked processed only after the warehouse build, data-quality checks, and reconciliation complete successfully.
- API runs use bounded half-open intervals, pagination, retries, optional store filters, and a configurable lookback window.
- API watermarks advance only after raw provider pages and the API manifest are written.
- Zero-record API intervals are recorded and exit successfully without writing an empty schema-breaking source file.
- Raw conversion validates schema drift before writing Parquet.
- Data-quality and reconciliation history are keyed by `run_id`.
- Airflow branches cleanly for no JSON change, missing API config, and zero-record API intervals.

## Repository Layout

```text
.
├── api/                  # FastAPI provider simulator and API test harness
├── dags/                 # Airflow DAGs for JSON and API source paths
├── dataSource/           # Six source JSON files used by the local pipeline
├── docs/                 # Requirement document, architecture, and model diagrams
├── pipeline/             # Ingestion, raw conversion, staging, audit, and state helpers
├── scripts/              # Local no-Airflow pipeline entrypoints
├── warehouse/            # DuckDB SQL for profiling, marts, quality, and reconciliation
├── docker-compose.yaml   # Airflow, Postgres, Redis, and MinIO stack
└── README.md
```

Key files:

| File | Purpose |
| --- | --- |
| `scripts/run_pipeline.py` | Main local runner with `local`, `json`, and `api` modes |
| `scripts/run_json_pipeline.py` | JSON object-store flow wrapper |
| `scripts/run_api_pipeline.py` | Provider API flow wrapper |
| `pipeline/json_source_state.py` | JSON source readiness and fingerprint state |
| `pipeline/api_ingest.py` | Generic interval API extraction and raw landing |
| `pipeline/raw.py` | Raw JSON schema validation and Parquet conversion |
| `pipeline/staging.py` | Canonical staging table build |
| `warehouse/marts.sql` | Dimensional model and facts |
| `warehouse/core.sql` | BI-facing views |
| `warehouse/data_quality.sql` | Data-quality checks and exceptions |
| `warehouse/reconciliation.sql` | Reconciliation summary and history |
| `dags/json_source_dag.py` | Airflow JSON source DAG |
| `dags/api_source_dag.py` | Airflow API source DAG |

## Data Model

| Layer | Relation(s) | Purpose |
| --- | --- | --- |
| Staging | `staging.int_customers`, `staging.int_stores`, `staging.int_products`, `staging.int_orders`, `staging.int_order_items`, `staging.int_deliveries` | Cleaned and deduplicated source-aligned tables |
| Dimensions | `marts.dim_customer`, `marts.dim_store`, `marts.dim_product`, `marts.dim_date`, `marts.dim_channel`, `marts.dim_payment_method`, `marts.dim_promo` | Shared reporting dimensions |
| Facts | `marts.fact_orders`, `marts.fact_order_items`, `marts.fact_deliveries` | Order, item, and delivery grain facts |
| Views | `core.vw_sales_performance`, `core.vw_delivery_performance`, `core.vw_product_performance`, `core.vw_customer_summary` | BI-facing query layer |
| Audit | `audit.data_quality_*`, `audit.reconciliation_*` | Quality checks, row-level exceptions, and run history |

## Running The Pipeline

Prerequisites:

- Python 3.12 or a compatible local Python runtime
- Docker Desktop for MinIO and Airflow runs
- Project dependencies installed from `requirements.txt` when running outside Docker

Install local dependencies:

```bash
python3 -m pip install -r requirements.txt
```

### Option 1: Local DuckDB Run

Use this for a direct end-to-end warehouse build without Airflow or MinIO.

```bash
python3 scripts/run_pipeline.py --mode local --fresh
```

The runner builds the DuckDB warehouse from `dataSource/`, runs marts, BI views, quality checks, reconciliation, and writes a timestamped log under:

```text
logs/pipeline_runs/
```

Use a temporary warehouse path when testing changes:

```bash
python3 scripts/run_pipeline.py \
  --mode local \
  --db /tmp/eatngo_pipeline_test.duckdb \
  --fresh \
  --execution-date 2026-07-25 \
  --run-id local-2026-07-25
```

### Option 2: JSON Source Flow

Use this when validating the object-store path for the six normal source files.

```bash
python3 scripts/run_json_pipeline.py --execution-date 2026-07-25
```

The JSON flow:

1. Validates the expected six-file drop in `dataSource/`.
2. Compares the current source fingerprint with the last successful run.
3. Lands JSON files into MinIO.
4. Converts raw JSON to Parquet.
5. Builds staging, marts, quality checks, and reconciliation.
6. Marks the fingerprint as processed after success.

Fingerprint state is stored at:

```text
warehouse/source_state/json_source_fingerprint.json
```

Use `--fresh` only for a deliberate rebuild from unchanged JSON files.

### Option 3: API Source Flow

Use this when pulling a provider feed through the same raw/staging/warehouse path.

```bash
python3 scripts/run_api_pipeline.py \
  --execution-date 2026-07-25 \
  --api-provider delivery_partner \
  --api-source deliveries_api \
  --api-target-source deliveries \
  --api-base-url https://api.provider.example.com \
  --api-data-path /v1/deliveries \
  --api-token-path /oauth/token \
  --api-client-id "$DELIVERY_API_CLIENT_ID" \
  --api-client-secret "$DELIVERY_API_CLIENT_SECRET"
```

For controlled backfills:

```bash
python3 scripts/run_api_pipeline.py \
  --execution-date 2026-07-25 \
  --api-interval-start 2026-07-25T00:00:00Z \
  --api-interval-end 2026-07-25T01:00:00Z
```

API extraction writes two raw artifacts:

- `rawapi/.../page-00001.json`: provider response pages with request metadata
- `rawjson/ingestion_date=<date>/deliveries.json`: normalized source-contract records for the shared converter

The local FastAPI provider simulator is documented separately in `api/README.md`.

### Option 4: Airflow Stack

Use this when testing orchestration, branching, object-store integration, and email notification.

Start the initialization task first:

```bash
docker compose up airflow-init
```

Then start the stack:

```bash
docker compose up -d
```

Airflow UI:

```text
http://127.0.0.1:8080
```

DAGs:

| DAG | Purpose | Schedule |
| --- | --- | --- |
| `eat_ngo_json_source_pipeline` | Six-file JSON source flow | Daily at 08:00 UTC by default |
| `eat_ngo_api_source_pipeline` | Provider/API source flow | Manual unless `EAT_NGO_API_SCHEDULE` is set |

The DAGs use branching for no-work cases and can send success/failure email when SMTP settings and `AIRFLOW_ALERT_EMAIL` are configured.

## Configuration

Core settings:

| Variable | Default | Notes |
| --- | --- | --- |
| `WAREHOUSE_DB` | `warehouse/eat_ngo_dw.duckdb` locally, `/opt/airflow/warehouse/eat_ngo_dw.duckdb` in Airflow | DuckDB warehouse path |
| `MINIO_ENDPOINT` | from `.env` | MinIO endpoint for object-store flows |
| `MINIO_ACCESS_KEY` | from `.env` | MinIO access key |
| `MINIO_SECRET_KEY` | from `.env` | MinIO secret key |
| `MINIO_SECURE` | `false` | Set to `true` for TLS |

JSON settings:

| Variable | Default | Notes |
| --- | --- | --- |
| `EAT_NGO_JSON_SOURCE_PATH` | `/opt/airflow/dataSource` in Airflow | Mounted JSON source folder |
| `EAT_NGO_JSON_STATE_PATH` | `warehouse/source_state/json_source_fingerprint.json` | JSON fingerprint state |
| `EAT_NGO_JSON_SCHEDULE` | `0 8 * * *` | Airflow JSON DAG schedule |

API settings:

| Variable | Default | Notes |
| --- | --- | --- |
| `DELIVERY_API_BASE_URL` / `API_BASE_URL` | unset | Provider API base URL |
| `DELIVERY_API_DATA_PATH` / `API_DATA_PATH` | `/v1/deliveries` | Provider data endpoint |
| `DELIVERY_API_TOKEN_PATH` / `API_TOKEN_PATH` | `/oauth/token` | OAuth token path |
| `DELIVERY_API_CLIENT_ID` / `API_CLIENT_ID` | unset | OAuth client id |
| `DELIVERY_API_CLIENT_SECRET` / `API_CLIENT_SECRET` | unset | OAuth client secret |
| `DELIVERY_API_AUTH_TYPE` / `API_AUTH_TYPE` | `oauth_client_credentials` | `oauth_client_credentials`, `bearer`, or `none` |
| `DELIVERY_API_STORE_IDS` / `API_STORE_IDS` | unset | Optional comma-separated store ids |
| `DELIVERY_API_PAGINATION_STYLE` / `API_PAGINATION_STYLE` | `page` | `page` or `cursor` |
| `DELIVERY_API_PAGE_SIZE_PARAM_NAME` / `API_PAGE_SIZE_PARAM_NAME` | `page_size` | Provider page-size parameter |
| `DELIVERY_API_HAS_MORE_KEY` / `API_HAS_MORE_KEY` | `has_more` | Supports dot paths such as `pagination.has_more` |
| `DELIVERY_API_CURSOR_RESPONSE_KEY` / `API_CURSOR_RESPONSE_KEY` | `next_cursor` | Supports dot paths such as `pagination.next_cursor` |
| `EAT_NGO_API_SCHEDULE` | manual | Airflow API DAG schedule |

Notification settings:

| Variable | Notes |
| --- | --- |
| `AIRFLOW_ALERT_EMAIL` | Comma-separated success/failure email recipients |
| `SMTP_MAIL_FROM` | Sender address |
| `SMTP_USER` | SMTP username |
| `SMTP_PASSWORD` | SMTP password or app password |

## Quality And Reconciliation

Quality checks are defined in:

```text
warehouse/data_quality.sql
```

Results and exceptions are written to:

- `audit.data_quality_results`
- `audit.data_quality_exceptions`
- `audit.data_quality_results_history`
- `audit.data_quality_exceptions_history`
- `audit.vw_data_quality_exception_summary`

Reconciliation is defined in:

```text
warehouse/reconciliation.sql
```

It compares staging outputs to reporting outputs overall and by store, channel, product, promotion, and delivery-store. Reconciliation history is retained in `audit.reconciliation_summary_history`.

`ERROR` findings block the pipeline. `WARN` findings load but remain visible for investigation.

## Testing

Run the full test suite:

```bash
pytest -q
```

Run formatting and linting:

```bash
ruff format --check pipeline scripts api dags
ruff check pipeline scripts api dags
```

Validate Airflow DAG parsing:

```bash
docker compose exec -T airflow-scheduler airflow dags list
docker compose exec -T airflow-scheduler airflow dags show eat_ngo_json_source_pipeline
docker compose exec -T airflow-scheduler airflow dags show eat_ngo_api_source_pipeline
```

## Troubleshooting

| Symptom | Likely cause | Action |
| --- | --- | --- |
| JSON DAG skips immediately | Source folder is missing, empty, or unchanged | Check `EAT_NGO_JSON_SOURCE_PATH`; use `--fresh` only for intentional rebuilds |
| JSON DAG fails before loading | One or more required JSON files is missing | Restore the six-file drop in `dataSource/` |
| API DAG skips immediately | Provider base URL is not configured | Set `DELIVERY_API_BASE_URL` or `API_BASE_URL` |
| API interval returns no warehouse refresh | Provider returned zero records | Check the API manifest and interval; this is a successful no-work run |
| API extraction fails authentication | Auth settings are incomplete | Check auth type, token path, client id, client secret, or bearer token |
| MinIO tasks cannot connect | MinIO settings or Docker network are wrong | Confirm `.env`, `docker compose ps`, and `MINIO_ENDPOINT=minio:9000` |
| Airflow does not show new DAG behavior | Containers are using an older image or DAG processor has not refreshed | Rebuild and recreate Airflow services |

## If This Moved To Production

The first changes I would make are:

- Move JSON fingerprint state, API watermarks, run manifests, and source-processing history into a transactional metadata store such as PostgreSQL. Files are acceptable for a local project, but production recovery, reruns, and operations need queryable state.
- Store raw and staged data in managed object storage with lifecycle policies, encryption, bucket-level access controls, and clear retention rules for provider payloads.
- Replace local DuckDB as the serving warehouse with a managed analytical warehouse when concurrency, access control, or BI tool integration requires it.
- Add stronger orchestration controls: single-run locking per source, explicit backfill parameters, rerun approvals, task-level SLAs, and concurrency limits.
- Move secrets and API credentials into a secrets manager instead of environment files.
- Expand alerting beyond email to an operational channel with severity routing, ownership, and escalation for failed quality checks or missed provider intervals.
- Version source contracts and data-quality expectations so vendor schema changes are reviewed before they affect reporting tables.
- Add more SCD2 store attributes once the business source of truth is confirmed, such as region, opening status, ownership group, and store format.
- Add automated lineage and freshness monitoring for core reporting views, especially order volume, delivery completeness, and API watermark lag.
- Separate CI validation from runtime orchestration by running unit tests, SQL checks, DAG import checks, and container builds before deployment.
