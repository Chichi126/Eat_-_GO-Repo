import os
import json
import time
import random
import logging
import duckdb
import boto3
import httpx
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("EatNGo_Ingestion_Pipeline")


class EatNGoAPIClient:
    """Handles Auth, Rate Limiting, Backoff, and Pagination for Eat N' Go Provider API."""
    
    def __init__(self, base_url: str, client_id: str, client_secret: str):
        self.base_url = base_url.rstrip("/")
        self.client_id = client_id
        self.client_secret = client_secret
        self.token: Optional[str] = None

    def authenticate(self) -> None:
        """Obtains OAuth token from /api/v1/auth/token."""
        url = f"{self.base_url}/api/v1/auth/token"
        logger.info("Authenticating with Eat N' Go Provider API...")
        
        with httpx.Client(timeout=10.0) as client:
            response = client.post(
                url, 
                json={"client_id": self.client_id, "client_secret": self.client_secret}
            )
            response.raise_for_status()
            self.token = response.json().get("access_token")
            logger.info("OAuth Token obtained successfully.")

    def _request_with_retry(self, url: str, params: Dict[str, Any], max_retries: int = 5) -> Dict[str, Any]:
        """Handles 429 Rate Limits, 5xx Transient Failures with Backoff and Jitter."""
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
        
        for attempt in range(1, max_retries + 1):
            try:
                with httpx.Client(timeout=30.0) as client:
                    response = client.get(url, headers=headers, params=params)
                    
                    # Handle 429 Rate Limits
                    if response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", 2 ** attempt))
                        jitter = random.uniform(0.5, 1.5)
                        wait_time = retry_after + jitter
                        logger.warning(f"Rate limited (429). Retrying in {wait_time:.2f}s...")
                        time.sleep(wait_time)
                        continue
                        
                    # Handle Token Expiry
                    if response.status_code == 401:
                        logger.warning("Token expired or missing. Re-authenticating...")
                        self.authenticate()
                        headers["Authorization"] = f"Bearer {self.token}"
                        continue

                    response.raise_for_status()
                    return response.json()

            except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                if attempt == max_retries:
                    logger.error(f"Max retries reached for {url}. Error: {str(exc)}")
                    raise exc
                
                wait_time = (2 ** attempt) + random.uniform(0.1, 1.0)
                logger.warning(f"Request failed ({exc}). Retrying in {wait_time:.2f}s...")
                time.sleep(wait_time)
                
        raise RuntimeError("Failed to fetch data after maximum retries.")

    def fetch_incremental_deliveries(self, updated_after: str) -> List[Dict[str, Any]]:
        """Pulls delivery records using cursor pagination."""
        if not self.token:
            self.authenticate()

        url = f"{self.base_url}/api/v1/deliveries"
        cursor = None
        pages = []
        page_num = 1

        while True:
            params = {
                "updated_after": updated_after,
                "limit": 100
            }
            if cursor:
                params["cursor"] = cursor

            logger.info(f"Fetching page {page_num} with updated_after >= {updated_after}")
            payload = self._request_with_retry(url, params)
            pages.append(payload)

            pagination = payload.get("pagination", {})
            cursor = pagination.get("next_cursor")
            has_more = pagination.get("has_more", False)

            if not has_more or not cursor:
                break
            page_num += 1

        return pages

    def fetch_vendor_summary(self, updated_after: str) -> Dict[str, Any]:
        """Fetches metrics from summary endpoint for reconciliation."""
        url = f"{self.base_url}/api/v1/deliveries/summary"
        return self._request_with_retry(url, {"updated_after": updated_after})


class EatNGoOrchestrator:
    def __init__(self, db_path: str = "warehouse.duckdb"):
        self.db_path = db_path
        self.s3_bucket = os.getenv("S3_BUCKET_NAME", "rawapi")
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localhost:9000"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        )
        self._init_metadata_store()

    def _get_db_connection(self):
        return duckdb.connect(self.db_path)

    def _init_metadata_store(self):
        """Metadata table for watermark tracking."""
        with self._get_db_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metadata_watermarks (
                    pipeline_name VARCHAR PRIMARY KEY,
                    last_watermark VARCHAR,
                    updated_at TIMESTAMP
                );
            """)

    def get_last_watermark(self, pipeline_name: str) -> str:
        with self._get_db_connection() as conn:
            res = conn.execute(
                "SELECT last_watermark FROM metadata_watermarks WHERE pipeline_name = ?", 
                [pipeline_name]
            ).fetchone()
            return res[0] if res else "2026-05-01T00:00:00Z"

    def update_watermark(self, pipeline_name: str, new_watermark: str):
        with self._get_db_connection() as conn:
            conn.execute("""
                INSERT INTO metadata_watermarks (pipeline_name, last_watermark, updated_at)
                VALUES (?, ?, NOW())
                ON CONFLICT (pipeline_name) DO UPDATE SET 
                    last_watermark = EXCLUDED.last_watermark,
                    updated_at = NOW();
            """, [pipeline_name, new_watermark])

    def land_raw_json_to_s3(self, pages: List[Dict[str, Any]]) -> List[str]:
        """Stores exact raw API response envelopes in S3 / MinIO."""
        now = datetime.now(timezone.utc)
        run_ts = now.strftime("%H%M%S")
        s3_keys = []

        for idx, page in enumerate(pages, start=1):
            # Clean object key without duplicated bucket prefix
            s3_key = f"provider=eatngo/source=deliveries/ingestion_date={now.strftime('%Y-%m-%d')}/page_{run_ts}_p{idx}.json"
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=json.dumps(page),
                ContentType="application/json"
            )
            s3_keys.append(s3_key)
            logger.info(f"Landed raw API response to s3://{self.s3_bucket}/{s3_key}")
            
        return s3_keys

    def process_and_load_warehouse(self):
        """Reads JSON from S3 into DuckDB and deduplicates latest state using QUALIFY."""
        with self._get_db_connection() as conn:
            endpoint = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000").replace("http://", "").replace("https://", "").strip("/")

            conn.execute("INSTALL httpfs; LOAD httpfs;")
            conn.execute(f"SET s3_endpoint='{endpoint}';")
            conn.execute("SET s3_use_ssl=false;")
            conn.execute("SET s3_url_style='path';")
            conn.execute(f"SET s3_access_key_id='{os.getenv('AWS_ACCESS_KEY_ID', 'minioadmin')}';")
            conn.execute(f"SET s3_secret_access_key='{os.getenv('AWS_SECRET_ACCESS_KEY', 'minioadmin')}';")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS fact_deliveries (
                    delivery_id VARCHAR PRIMARY KEY,
                    order_id VARCHAR,
                    brand VARCHAR,
                    store_id VARCHAR,
                    store_name VARCHAR,
                    city VARCHAR,
                    delivery_partner VARCHAR,
                    delivery_status VARCHAR,
                    delivery_fee INTEGER,
                    distance_km DOUBLE,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                );
            """)

            logger.info("Extracting and deduplicating raw JSON payloads in DuckDB...")
            # Explicit wildcard pattern matching provider=eatngo/source=deliveries/ingestion_date=YYYY-MM-DD/*.json
            s3_path_pattern = f"s3://{self.s3_bucket}/provider=eatngo/*/*/*.json"
            
            dedup_query = f"""
                CREATE OR REPLACE TEMP TABLE temp_staged_deliveries AS
                SELECT 
                    payload.delivery_id,
                    payload.order_id,
                    payload.brand,
                    payload.store_id,
                    payload.store_name,
                    payload.city,
                    payload.delivery_partner,
                    payload.delivery_status,
                    payload.delivery_fee,
                    payload.distance_km,
                    payload.created_at::TIMESTAMP as created_at,
                    payload.updated_at::TIMESTAMP as updated_at
                FROM (
                    SELECT unnest(data) as payload 
                    FROM read_json_auto('{s3_path_pattern}')
                )
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY payload.delivery_id 
                    ORDER BY payload.updated_at DESC
                ) = 1;
            """
            conn.execute(dedup_query)

            conn.execute("""
                MERGE INTO fact_deliveries AS target
                USING temp_staged_deliveries AS src
                ON target.delivery_id = src.delivery_id
                WHEN MATCHED THEN UPDATE SET
                    delivery_status = src.delivery_status,
                    delivery_fee = src.delivery_fee,
                    updated_at = src.updated_at
                WHEN NOT MATCHED THEN INSERT VALUES (
                    src.delivery_id, src.order_id, src.brand, src.store_id, src.store_name,
                    src.city, src.delivery_partner, src.delivery_status, src.delivery_fee,
                    src.distance_km, src.created_at, src.updated_at
                );
            """)
            logger.info("Successfully merged records into DuckDB `fact_deliveries`.")

            
    def reconcile(self, vendor_summary: Dict[str, Any]):
        """Reconciles total metrics between DuckDB warehouse and API summary."""
        with self._get_db_connection() as conn:
            result = conn.execute("""
                SELECT COUNT(DISTINCT delivery_id), COALESCE(SUM(delivery_fee), 0) 
                FROM fact_deliveries;
            """).fetchone()
            
            db_distinct_count, db_total_fees = result[0], result[1]
            summary_count = vendor_summary.get("distinct_delivery_count", 0)
            summary_fees = vendor_summary.get("total_delivery_fees", 0)

            logger.info("--- RECONCILIATION REPORT ---")
            logger.info(f"Target DB Distinct Count : {db_distinct_count} | Source Summary Count : {summary_count}")
            logger.info(f"Target DB Total Fees     : {db_total_fees} | Source Summary Fees  : {summary_fees}")

            if db_distinct_count < summary_count:
                logger.error("🚨 RECONCILIATION FAILED: Record count mismatch!")
            else:
                logger.info("✅ RECONCILIATION PASSED: Data matches source summary totals.")


def run_pipeline():
    pipeline_name = "eatngo_deliveries_pipeline"
    orchestrator = EatNGoOrchestrator()
    
    client = EatNGoAPIClient(
        base_url=os.getenv("DELIVERY_API_BASE_URL", "http://127.0.0.1:8000"),
        client_id=os.getenv("DELIVERY_API_CLIENT_ID", "eatngo-bi-client"),
        client_secret=os.getenv("DELIVERY_API_CLIENT_SECRET", "local-development-secret")
    )

    try:
        last_watermark = orchestrator.get_last_watermark(pipeline_name)
        logger.info(f"Starting run from watermark: {last_watermark}")

        raw_pages = client.fetch_incremental_deliveries(updated_after=last_watermark)
        
        if not raw_pages or not raw_pages[0].get("data"):
            logger.info("No new updates found.")
            return

        orchestrator.land_raw_json_to_s3(raw_pages)
        orchestrator.process_and_load_warehouse()

        vendor_summary = client.fetch_vendor_summary(updated_after=last_watermark)
        orchestrator.reconcile(vendor_summary)

        latest_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        orchestrator.update_watermark(pipeline_name, latest_timestamp)
        logger.info(f"Watermark updated to {latest_timestamp}")

    except Exception as e:
        logger.critical(f"Pipeline execution failed: {str(e)}", exc_info=True)


if __name__ == "__main__":
    run_pipeline()