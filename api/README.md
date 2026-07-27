# FastAPI Delivery Provider Simulator

Local provider-style API used to test the pipeline's API ingestion path.

The simulator is separate from the warehouse implementation. It behaves like an external delivery partner source: it owns its own SQLite source database, exposes paginated delivery endpoints, supports interval filters, and returns deterministic synthetic records for repeatable tests.

## Purpose

Use this API to validate:

- interval-based extraction
- cursor pagination
- store, city, brand, and delivery-status filters
- provider summary reconciliation
- retry handling through simulated HTTP failures
- late-arriving and corrected delivery records
- the pipeline handoff from provider JSON to normalized `deliveries.json`

The simulator is not the BI warehouse. The BI pipeline entrypoints remain:

- `scripts/run_api_pipeline.py`
- `python3 -m api.run_ingestion`

Both entrypoints land API data as JSON before raw conversion and warehouse loading.

## Source Data

The API uses SQLite as its local source system. On startup, it creates deterministic Faker-based delivery records for:

- Domino's Pizza Nigeria
- Cold Stone Creamery Nigeria
- Pinkberry Nigeria

The generated data includes normal records plus controlled test cases for duplicates, corrections, failed deliveries, cancelled deliveries, SLA breaches, and late updates.

## Running Locally

Start the API directly:

```bash
uvicorn api.main:app --reload --port 8000
```

Local URL:

```text
http://127.0.0.1:8000
```

Swagger UI:

```text
http://127.0.0.1:8000/docs
```

## Running With Docker

Build the image:

```bash
docker build -f api/Dockerfile -t eatngo-fake-provider-api:latest .
```

Run the container:

```bash
docker run -d \
  --name eatngo-fake-provider-api \
  -p 8010:8000 \
  eatngo-fake-provider-api:latest
```

Docker URL:

```text
http://127.0.0.1:8010
```

If the pipeline container needs to call the API by Docker DNS, attach it to the same network as the Airflow/MinIO stack:

```bash
docker network connect eat_n_go_asscopy_airflow_network eatngo-fake-provider-api
```

Then use:

```text
http://eatngo-fake-provider-api:8000
```

## Endpoints

| Endpoint | Method | Purpose |
| --- | --- | --- |
| `/health` | `GET` | Health check |
| `/` | `GET` | API metadata |
| `/api/v1/auth/token` | `POST` | Optional local token endpoint |
| `/api/v1/deliveries` | `GET` | Cursor-paginated delivery records |
| `/api/v1/deliveries/summary` | `GET` | Source summary for reconciliation |
| `/api/v1/deliveries/{delivery_id}` | `GET` | Latest record for one delivery |

Read endpoints do not require a bearer token in the local simulator. The token endpoint remains available so OAuth-style client-credential behavior can still be tested when needed.

Optional token request:

```bash
curl -X POST "http://127.0.0.1:8010/api/v1/auth/token" \
  -H "Content-Type: application/json" \
  -d '{"client_id":"eatngo-bi-client","client_secret":"local-development-secret"}'
```

## Query Examples

Fetch one page of delivery records:

```bash
curl "http://127.0.0.1:8010/api/v1/deliveries?updated_after=2026-05-01T00:00:00Z&updated_before=2026-05-05T00:00:00Z&limit=10"
```

Filter by brand:

```bash
curl "http://127.0.0.1:8010/api/v1/deliveries?updated_after=2026-05-01T00:00:00Z&updated_before=2026-05-05T00:00:00Z&brand=Domino%27s%20Pizza%20Nigeria&limit=10"
```

Get summary counts for reconciliation:

```bash
curl "http://127.0.0.1:8010/api/v1/deliveries/summary?updated_after=2026-05-01T00:00:00Z&updated_before=2026-05-05T00:00:00Z"
```

Simulate a retryable provider response:

```bash
curl "http://127.0.0.1:8010/api/v1/deliveries?updated_after=2026-05-01T00:00:00Z&updated_before=2026-05-02T00:00:00Z&simulate_status=429"
```

## Running The API Through The BI Pipeline

With the Docker API running on the host:

```bash
python3 -m api.run_ingestion \
  --base-url http://127.0.0.1:8010 \
  --execution-date 2026-05-01 \
  --updated-after 2026-05-01T00:00:00Z \
  --updated-before 2026-05-05T00:00:00Z \
  --page-limit 100
```

From a container on the shared Docker network:

```bash
python3 -m api.run_ingestion \
  --base-url http://eatngo-fake-provider-api:8000 \
  --execution-date 2026-05-01 \
  --updated-after 2026-05-01T00:00:00Z \
  --updated-before 2026-05-05T00:00:00Z \
  --page-limit 100
```

Pipeline landing behavior:

| Location | Content |
| --- | --- |
| `rawapi/provider=<provider>/source=<source>/ingestion_date=<date>/page-00001.json` | Provider response envelope, request parameters, pagination metadata |
| `rawjson/ingestion_date=<date>/deliveries.json` | Normalized delivery source-contract records |
| `rawjson/ingestion_date=<date>/_api_manifest_deliveries.json` | Extraction manifest, watermarks, counts, and reconciliation result |

If an interval returns no records, the pipeline writes the raw API response page and manifest, advances the watermark, and does not write an empty `deliveries.json`.

## Tests

Run the API tests:

```bash
pytest api/test_delivery_api.py
```

The tests cover endpoint behavior, deterministic data generation, cursor pagination, filtering, retry handling, raw response preservation, watermark behavior, and reconciliation checks.
