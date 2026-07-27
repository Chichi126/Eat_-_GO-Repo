import uuid
from datetime import datetime, timezone

from pipeline.duckdb_utils import get_duckdb_connection


def deterministic_batch_id(stage, execution_date):
    """Returns a deterministic batch id for a stage and logical date."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"eat-ngo/{stage}/{execution_date}"))


def run_staging_pipeline(execution_date=None, staging_bucket="staging", db_file=None):
    """Builds canonical staging tables from raw Parquet partitions.

    Args:
        execution_date: Logical pipeline date in ``YYYY-MM-DD`` format.
        staging_bucket: MinIO bucket containing converted Parquet objects.
        db_file: Optional DuckDB database path.

    Raises:
        Exception: Propagates DuckDB or object-store failures to the caller.
    """
    execution_date = execution_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    batch_id = deterministic_batch_id("staging", execution_date)

    con = get_duckdb_connection(db_file)

    print(f"\n--- Initializing `staging` Schema in DuckDB for {execution_date} ---")
    con.execute("CREATE SCHEMA IF NOT EXISTS staging;")

    sources = ["customers", "stores", "products", "orders", "order_items", "deliveries"]
    # Staging reads all load-date partitions and keeps the newest record per
    # business key.
    uris = {s: f"s3://{staging_bucket}/{s}/load_date=*/{s}.parquet" for s in sources}
    results = []

    try:
        # Customers: one current row per customer_id.
        con.execute(f"""
            CREATE OR REPLACE TABLE staging.int_customers AS
	            SELECT * EXCLUDE (rn)
	            FROM (
	            SELECT * EXCLUDE (_batch_id),
	               ROW_NUMBER() OVER (
	                   PARTITION BY customer_id
	                   ORDER BY _loaded_at DESC, _batch_id DESC, _source_file DESC
	               ) as rn,
	               '{batch_id}' AS _batch_id
	            FROM read_parquet('{uris["customers"]}')
	                )
            WHERE rn = 1;
            """)
        results.append({"model": "staging.int_customers", "status": "success"})
        print("[staging.int_customers] Table created in DuckDB.")

        # Stores: one current row per store_id with normalized city text.
        con.execute(f"""
            CREATE OR REPLACE TABLE staging.int_stores AS
	            SELECT * EXCLUDE (rn)
	            FROM (
	                SELECT * EXCLUDE (_batch_id),
	               TRIM(city) AS clean_city,
	               ROW_NUMBER() OVER (
	                   PARTITION BY store_id
	                   ORDER BY _loaded_at DESC, _batch_id DESC, _source_file DESC
	               ) as rn,
	               '{batch_id}' AS _batch_id
	                FROM read_parquet('{uris["stores"]}')
	                )
            WHERE rn = 1;
            """)
        results.append({"model": "staging.int_stores", "status": "success"})
        print("[staging.int_stores] Table created in DuckDB.")

        # Products: one current row per product_id with a reporting category.
        con.execute(f"""
            CREATE OR REPLACE TABLE staging.int_products AS
	            SELECT * EXCLUDE (rn)
	            FROM (
	                SELECT * EXCLUDE (_batch_id),
	                COALESCE(category, 'Uncategorized') AS unified_category,
	                ROW_NUMBER() OVER (
	                    PARTITION BY product_id
	                    ORDER BY _loaded_at DESC, _batch_id DESC, _source_file DESC
	                ) as rn,
	                '{batch_id}' AS _batch_id
	                FROM read_parquet('{uris["products"]}')
	                )
            WHERE rn = 1;
            """)
        results.append({"model": "staging.int_products", "status": "success"})
        print("[staging.int_products] Table created in DuckDB.")

        # Orders: one current row per order_id with date and cohort attributes.
        con.execute(f"""
            CREATE OR REPLACE TABLE staging.int_orders AS
	            WITH unique_orders AS (
	                SELECT *,
	                       ROW_NUMBER() OVER (
	                           PARTITION BY order_id
	                           ORDER BY _loaded_at DESC, _batch_id DESC, _source_file DESC
	                       ) as rn
	                FROM read_parquet('{uris["orders"]}')
	                QUALIFY rn = 1
	            ),
            cohort_evaluation AS (
	                SELECT *,
	                       CAST(order_ts AS TIMESTAMP) as order_dt,
	                       ROW_NUMBER() OVER (
	                           PARTITION BY customer_id
	                           ORDER BY CAST(order_ts AS TIMESTAMP) ASC, order_id ASC
	                       ) as order_rank
	                FROM unique_orders
	            )
            SELECT 
                order_id, customer_id, store_id, channel, payment_method, promo_code,
                order_dt AS order_timestamp,
                items_count, qty_total, subtotal_ngn, discount_ngn, tax_ngn,
                delivery_fee_ngn, total_amount_ngn,
                
                -- Date attributes
                YEAR(order_dt) AS order_year,
                MONTH(order_dt) AS order_month,
                WEEK(order_dt) AS order_week,
                DAYOFWEEK(order_dt) AS order_day_of_week,
                HOUR(order_dt) AS order_hour,
                
                -- Reporting flags
                CASE WHEN promo_code IS NOT NULL AND promo_code != '' THEN 1 ELSE 0 END AS promo_flag,
                CASE WHEN order_rank = 1 THEN 'New' ELSE 'Returning' END AS new_vs_returning_customer_flag,
                CASE WHEN order_rank = 1 THEN 'First-Time Buyer' ELSE 'Loyal Customer' END AS customer_type,
                
                _source_file, _loaded_at, '{batch_id}' AS _batch_id
            FROM cohort_evaluation;
        """)
        results.append({"model": "staging.int_orders", "status": "success"})
        print("[staging.int_orders] Table created in DuckDB.")

        # Deliveries: one current row per order_id with SLA metrics.
        con.execute(f"""
            CREATE OR REPLACE TABLE staging.int_deliveries AS
	            WITH unique_deliveries AS (
	            SELECT *,
	               ROW_NUMBER() OVER (
	                   PARTITION BY order_id
	                   ORDER BY _loaded_at DESC, _batch_id DESC, _source_file DESC
	               ) as rn
	        FROM read_parquet('{uris["deliveries"]}')
	        QUALIFY rn = 1
	        )
        SELECT 
            d.order_id,
            d.store_id,
            d.order_ts AS order_timestamp,
            d.promised_delivery_minutes,
            d.actual_delivery_minutes,
            (d.actual_delivery_minutes - d.promised_delivery_minutes) AS delay_minutes,
            d.delivered_within_sla AS delivery_sla_met_flag,
            d._source_file, d._loaded_at, '{batch_id}' AS _batch_id
        FROM unique_deliveries d;
        """)
        results.append({"model": "staging.int_deliveries", "status": "success"})
        print("[staging.int_deliveries] Table created in DuckDB.")
        # Order items: one current row per order/product line.
        con.execute(f"""
            CREATE OR REPLACE TABLE staging.int_order_items AS
	            WITH unique_items AS (
	                SELECT *,
	                    ROW_NUMBER() OVER (
	                        PARTITION BY order_id, product_id
	                        ORDER BY _loaded_at DESC, _batch_id DESC, _source_file DESC
	                    ) as rn
	                FROM read_parquet('{uris["order_items"]}')
	                QUALIFY rn = 1
	            )
            SELECT 
                i.order_id, i.product_id, i.product_name, i.category,
                i.qty AS quantity,
                i.unit_price_ngn AS unit_price,
                i.line_total_ngn AS product_revenue,

                o.store_id, o.channel, o.order_timestamp, o.promo_flag,

                i._source_file, i._loaded_at, '{batch_id}' AS _batch_id
            FROM unique_items i
            LEFT JOIN staging.int_orders o ON i.order_id = o.order_id;
        """)
        results.append({"model": "staging.int_order_items", "status": "success"})
        print("[staging.int_order_items] Table created in DuckDB.")

        print(f"\nStaging layer built successfully in DuckDB database: {db_file}")

    except Exception as e:
        print(f"\nCRITICAL STAGING PIPELINE EXCEPTION: {e}")
        raise
    finally:
        con.close()


if __name__ == "__main__":
    run_staging_pipeline()
