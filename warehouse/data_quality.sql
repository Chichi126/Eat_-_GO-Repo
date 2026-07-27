-- Automated data-quality checks (Question 6).
-- This script is intentionally run after every successful warehouse load.
-- A failing check is retained in audit.data_quality_results and the related
-- records are exposed in audit.data_quality_exceptions for investigation.

CREATE SCHEMA IF NOT EXISTS audit;

SET VARIABLE audit_execution_date = COALESCE(getvariable('exec_date'), CURRENT_DATE);
SET VARIABLE audit_run_id = COALESCE(
    getvariable('run_id'),
    'manual-' || CAST(getvariable('audit_execution_date') AS VARCHAR)
);

CREATE OR REPLACE TABLE audit.data_quality_rulebook AS
SELECT 'ERROR' AS severity, 'reject_or_fix_before_reporting' AS handling_action,
       'Breaks completeness, key integrity, financial correctness, or BI joins.' AS handling_notes
UNION ALL
SELECT 'WARN', 'load_but_review',
       'Does not block reporting, but should be reviewed by data owner or operations.';

CREATE OR REPLACE TABLE audit.data_quality_results AS
WITH checks AS (
    SELECT 'customers_record_count' AS check_name, COUNT(*) AS observed_value, 1 AS expected_minimum, 'ERROR' AS severity FROM staging.int_customers
    UNION ALL SELECT 'stores_record_count', COUNT(*), 1, 'ERROR' FROM staging.int_stores
    UNION ALL SELECT 'products_record_count', COUNT(*), 1, 'ERROR' FROM staging.int_products
    UNION ALL SELECT 'orders_record_count', COUNT(*), 1, 'ERROR' FROM staging.int_orders
    UNION ALL SELECT 'order_items_record_count', COUNT(*), 1, 'ERROR' FROM staging.int_order_items
    UNION ALL SELECT 'deliveries_record_count', COUNT(*), 1, 'ERROR' FROM staging.int_deliveries
    UNION ALL SELECT 'duplicate_customer_id', COUNT(*) - COUNT(DISTINCT customer_id), 0, 'ERROR' FROM staging.int_customers
    UNION ALL SELECT 'duplicate_store_id', COUNT(*) - COUNT(DISTINCT store_id), 0, 'ERROR' FROM staging.int_stores
    UNION ALL SELECT 'duplicate_product_id', COUNT(*) - COUNT(DISTINCT product_id), 0, 'ERROR' FROM staging.int_products
    UNION ALL SELECT 'duplicate_order_id', COUNT(*) - COUNT(DISTINCT order_id), 0, 'ERROR' FROM staging.int_orders
    UNION ALL SELECT 'duplicate_order_item_key', COUNT(*) - COUNT(DISTINCT CONCAT(order_id, '|', product_id)), 0, 'ERROR' FROM staging.int_order_items
    UNION ALL SELECT 'orphan_order_customer', COUNT(*), 0, 'ERROR' FROM staging.int_orders o LEFT JOIN staging.int_customers c USING (customer_id) WHERE c.customer_id IS NULL
    UNION ALL SELECT 'orphan_order_store', COUNT(*), 0, 'ERROR' FROM staging.int_orders o LEFT JOIN staging.int_stores s USING (store_id) WHERE s.store_id IS NULL
    UNION ALL SELECT 'orphan_item_order', COUNT(*), 0, 'ERROR' FROM staging.int_order_items i LEFT JOIN staging.int_orders o USING (order_id) WHERE o.order_id IS NULL
    UNION ALL SELECT 'orphan_item_product', COUNT(*), 0, 'ERROR' FROM staging.int_order_items i LEFT JOIN staging.int_products p USING (product_id) WHERE p.product_id IS NULL
    UNION ALL SELECT 'orphan_delivery_order', COUNT(*), 0, 'ERROR' FROM staging.int_deliveries d LEFT JOIN staging.int_orders o USING (order_id) WHERE o.order_id IS NULL
    UNION ALL SELECT 'null_critical_order_fields', COUNT(*), 0, 'ERROR' FROM staging.int_orders WHERE order_id IS NULL OR customer_id IS NULL OR store_id IS NULL OR order_timestamp IS NULL
    UNION ALL SELECT 'negative_order_amount', COUNT(*), 0, 'ERROR' FROM staging.int_orders WHERE subtotal_ngn < 0 OR total_amount_ngn < 0
    UNION ALL SELECT 'negative_item_revenue', COUNT(*), 0, 'ERROR' FROM staging.int_order_items WHERE product_revenue < 0 OR quantity <= 0
    UNION ALL SELECT 'invalid_channel', COUNT(*), 0, 'ERROR' FROM staging.int_orders WHERE channel NOT IN ('Dine-in', 'Takeaway', 'Delivery', 'Online App') OR channel IS NULL
    UNION ALL SELECT 'missing_delivery_for_delivery_order', COUNT(*), 0, 'ERROR' FROM staging.int_orders o LEFT JOIN staging.int_deliveries d USING (order_id) WHERE o.channel IN ('Delivery', 'Online App') AND d.order_id IS NULL
    UNION ALL SELECT 'invalid_delivery_sla_flag', COUNT(*), 0, 'ERROR' FROM staging.int_deliveries WHERE delivery_sla_met_flag NOT IN (0, 1) OR delivery_sla_met_flag IS NULL
    UNION ALL SELECT 'invalid_delivery_time', COUNT(*), 0, 'ERROR' FROM staging.int_deliveries WHERE promised_delivery_minutes <= 0 OR actual_delivery_minutes <= 0 OR actual_delivery_minutes IS NULL OR promised_delivery_minutes IS NULL
    UNION ALL SELECT 'invalid_order_timestamp', COUNT(*), 0, 'ERROR' FROM staging.int_orders WHERE order_timestamp IS NULL OR CAST(order_timestamp AS DATE) < DATE '2020-01-01' OR CAST(order_timestamp AS DATE) > CURRENT_DATE + INTERVAL 1 DAY
    UNION ALL SELECT 'discount_exceeds_subtotal', COUNT(*), 0, 'ERROR' FROM staging.int_orders WHERE discount_ngn > subtotal_ngn
    UNION ALL SELECT 'order_total_formula_mismatch', COUNT(*), 0, 'ERROR' FROM staging.int_orders WHERE ABS((subtotal_ngn - discount_ngn + tax_ngn + delivery_fee_ngn) - total_amount_ngn) > 0.05
    UNION ALL SELECT 'order_items_count_mismatch', COUNT(*), 0, 'ERROR' FROM (SELECT o.order_id FROM staging.int_orders o JOIN staging.int_order_items i USING (order_id) GROUP BY o.order_id, o.items_count HAVING COUNT(*) != o.items_count)
    UNION ALL SELECT 'order_quantity_total_mismatch', COUNT(*), 0, 'ERROR' FROM (SELECT o.order_id FROM staging.int_orders o JOIN staging.int_order_items i USING (order_id) GROUP BY o.order_id, o.qty_total HAVING SUM(i.quantity) != o.qty_total)
    UNION ALL SELECT 'missing_fact_store_mapping', COUNT(*), 0, 'ERROR' FROM marts.fact_orders WHERE store_sk IS NULL
    UNION ALL SELECT 'missing_fact_product_mapping', COUNT(*), 0, 'ERROR' FROM marts.fact_order_items WHERE product_sk IS NULL
    UNION ALL SELECT 'duplicate_dim_customer_sk', COUNT(*) - COUNT(DISTINCT customer_sk), 0, 'ERROR' FROM marts.dim_customer
    UNION ALL SELECT 'duplicate_dim_product_sk', COUNT(*) - COUNT(DISTINCT product_sk), 0, 'ERROR' FROM marts.dim_product
    UNION ALL SELECT 'duplicate_dim_store_current_row', COUNT(*) - COUNT(DISTINCT store_id), 0, 'ERROR' FROM marts.dim_store WHERE is_current
    UNION ALL SELECT 'duplicate_fact_order_item_grain', COUNT(*) - COUNT(DISTINCT order_id || '|' || product_id), 0, 'ERROR' FROM marts.fact_order_items
    UNION ALL SELECT 'fact_orders_null_foreign_key', COUNT(*), 0, 'ERROR' FROM marts.fact_orders WHERE customer_sk IS NULL OR store_sk IS NULL OR channel_sk IS NULL OR payment_method_sk IS NULL OR promo_sk IS NULL OR date_sk IS NULL
    UNION ALL SELECT 'fact_order_items_null_foreign_key', COUNT(*), 0, 'ERROR' FROM marts.fact_order_items WHERE product_sk IS NULL OR store_sk IS NULL OR channel_sk IS NULL OR date_sk IS NULL
    UNION ALL SELECT 'fact_deliveries_null_foreign_key', COUNT(*), 0, 'ERROR' FROM marts.fact_deliveries WHERE store_sk IS NULL OR date_sk IS NULL
    UNION ALL SELECT 'promo_with_zero_discount', COUNT(*), 0, 'WARN' FROM staging.int_orders WHERE promo_code IS NOT NULL AND promo_code <> '' AND discount_ngn = 0
    UNION ALL SELECT 'late_arriving_order_records', COUNT(*), 0, 'WARN' FROM staging.int_orders WHERE CAST(_loaded_at AS DATE) > CAST(order_timestamp AS DATE) + INTERVAL 1 DAY AND CAST(order_timestamp AS DATE) >= CURRENT_DATE - INTERVAL 30 DAY
    UNION ALL SELECT 'unusual_daily_order_volume_change', COUNT(*), 0, 'WARN' FROM (
        WITH daily_counts AS (
            SELECT
                CAST(order_timestamp AS DATE) AS order_date,
                COUNT(*) AS order_count,
                LAG(COUNT(*)) OVER (ORDER BY CAST(order_timestamp AS DATE)) AS previous_order_count
            FROM staging.int_orders
            GROUP BY CAST(order_timestamp AS DATE)
        )
        SELECT order_date
        FROM daily_counts
        WHERE previous_order_count >= 10
          AND ABS(order_count - previous_order_count)::DOUBLE / NULLIF(previous_order_count, 0) > 0.50
    )
    UNION ALL SELECT 'order_item_subtotal_mismatch', COUNT(*), 0, 'ERROR' FROM (SELECT o.order_id FROM staging.int_orders o JOIN staging.int_order_items i USING (order_id) GROUP BY o.order_id, o.subtotal_ngn HAVING ABS(SUM(i.product_revenue) - o.subtotal_ngn) > 0.01)
)
SELECT
    getvariable('audit_run_id') AS run_id,
    CAST(getvariable('audit_execution_date') AS DATE) AS execution_date,
    CURRENT_TIMESTAMP AS checked_at,
    check_name,
    severity,
    observed_value,
    expected_minimum,
    CASE
        WHEN check_name LIKE '%record_count' AND observed_value < expected_minimum THEN 'FAIL'
        WHEN check_name NOT LIKE '%record_count' AND observed_value > expected_minimum THEN CASE WHEN severity = 'WARN' THEN 'WARN' ELSE 'FAIL' END
        ELSE 'PASS'
    END AS status
FROM checks;

CREATE TABLE IF NOT EXISTS audit.data_quality_results_history AS
SELECT * FROM audit.data_quality_results WHERE FALSE;

DELETE FROM audit.data_quality_results_history
WHERE run_id = getvariable('audit_run_id');

INSERT INTO audit.data_quality_results_history
SELECT * FROM audit.data_quality_results;

CREATE OR REPLACE TABLE audit.data_quality_exceptions AS
WITH exception_rows AS (
SELECT
    CURRENT_TIMESTAMP AS detected_at,
    'orphan_order_customer' AS check_name,
    'ERROR' AS severity,
    'staging.int_orders' AS source_table,
    o.order_id AS record_key,
    'Order customer_id does not exist in customer master' AS issue,
    'Hold affected order from trusted reporting until customer master is corrected or mapping is supplied.' AS recommended_action,
    o._source_file AS source_file,
    o._loaded_at AS loaded_at
FROM staging.int_orders o LEFT JOIN staging.int_customers c USING (customer_id)
WHERE c.customer_id IS NULL
UNION ALL
SELECT CURRENT_TIMESTAMP, 'orphan_order_store', 'ERROR', 'staging.int_orders', o.order_id,
       'Order store_id does not exist in store master',
       'Hold affected order from trusted reporting until store master is corrected or mapping is supplied.',
       o._source_file, o._loaded_at
FROM staging.int_orders o LEFT JOIN staging.int_stores s USING (store_id)
WHERE s.store_id IS NULL
UNION ALL
SELECT CURRENT_TIMESTAMP, 'orphan_item_order', 'ERROR', 'staging.int_order_items', i.order_id || '|' || i.product_id,
       'Order item has no matching order header',
       'Quarantine affected line item and request source-system replay or order-header correction.',
       i._source_file, i._loaded_at
FROM staging.int_order_items i LEFT JOIN staging.int_orders o USING (order_id)
WHERE o.order_id IS NULL
UNION ALL
SELECT CURRENT_TIMESTAMP, 'orphan_item_product', 'ERROR', 'staging.int_order_items', i.order_id || '|' || i.product_id,
       'Order item product_id does not exist in product master',
       'Hold product-level reporting for affected line until product master mapping is supplied.',
       i._source_file, i._loaded_at
FROM staging.int_order_items i LEFT JOIN staging.int_products p USING (product_id)
WHERE p.product_id IS NULL
UNION ALL
SELECT CURRENT_TIMESTAMP, 'orphan_delivery_order', 'ERROR', 'staging.int_deliveries', d.order_id,
       'Delivery record has no matching order header',
       'Quarantine affected delivery record and request source-system replay or order-header correction.',
       d._source_file, d._loaded_at
FROM staging.int_deliveries d LEFT JOIN staging.int_orders o USING (order_id)
WHERE o.order_id IS NULL
UNION ALL
SELECT CURRENT_TIMESTAMP, 'null_critical_order_fields', 'ERROR', 'staging.int_orders', order_id,
       'Order has a null critical key or timestamp',
       'Reject affected order from reporting until required key fields and timestamp are supplied.',
       _source_file, _loaded_at
FROM staging.int_orders
WHERE order_id IS NULL OR customer_id IS NULL OR store_id IS NULL OR order_timestamp IS NULL
UNION ALL
SELECT CURRENT_TIMESTAMP, 'duplicate_order_id', 'ERROR', 'staging.int_orders', order_id,
       'Duplicate order_id remains after staging',
       'Investigate source replay/correction logic and keep only the latest valid business record.',
       MIN(_source_file), MAX(_loaded_at)
FROM staging.int_orders
GROUP BY order_id
HAVING COUNT(*) > 1
UNION ALL
SELECT CURRENT_TIMESTAMP, 'duplicate_order_item_key', 'ERROR', 'staging.int_order_items', order_id || '|' || product_sk_source,
       'Duplicate order/product line key remains after staging',
       'Investigate duplicated line items and reconcile with order subtotal before reporting product mix.',
       MIN(source_file), MAX(loaded_at)
FROM (
    SELECT order_id, product_id AS product_sk_source, _source_file AS source_file, _loaded_at AS loaded_at
    FROM staging.int_order_items
)
GROUP BY order_id, product_sk_source
HAVING COUNT(*) > 1
UNION ALL
SELECT CURRENT_TIMESTAMP, 'missing_delivery_for_delivery_order', 'ERROR', 'staging.int_orders', o.order_id,
       'Delivery or Online App order has no delivery record',
       'Hold delivery SLA reporting for affected order and request missing delivery feed record.',
       o._source_file, o._loaded_at
FROM staging.int_orders o LEFT JOIN staging.int_deliveries d USING (order_id)
WHERE o.channel IN ('Delivery', 'Online App') AND d.order_id IS NULL
UNION ALL
SELECT CURRENT_TIMESTAMP, 'negative_order_amount', 'ERROR', 'staging.int_orders', order_id,
       'Negative subtotal or total amount',
       'Reject affected order from financial reporting until source amount is corrected.',
       _source_file, _loaded_at
FROM staging.int_orders
WHERE subtotal_ngn < 0 OR total_amount_ngn < 0
UNION ALL
SELECT CURRENT_TIMESTAMP, 'negative_item_revenue', 'ERROR', 'staging.int_order_items', order_id || '|' || product_id,
       'Negative product revenue or non-positive quantity',
       'Reject affected line item from product reporting until source amount or quantity is corrected.',
       _source_file, _loaded_at
FROM staging.int_order_items
WHERE product_revenue < 0 OR quantity <= 0
UNION ALL
SELECT CURRENT_TIMESTAMP, 'invalid_channel', 'ERROR', 'staging.int_orders', order_id,
       'Order channel is null or outside the accepted channel set',
       'Map the channel to an approved reporting value or correct the source feed.',
       _source_file, _loaded_at
FROM staging.int_orders
WHERE channel NOT IN ('Dine-in', 'Takeaway', 'Delivery', 'Online App') OR channel IS NULL
UNION ALL
SELECT CURRENT_TIMESTAMP, 'invalid_delivery_time', 'ERROR', 'staging.int_deliveries', order_id,
       'Delivery timing fields are null or non-positive',
       'Exclude from SLA metrics until delivery timing is corrected.',
       _source_file, _loaded_at
FROM staging.int_deliveries
WHERE promised_delivery_minutes <= 0 OR actual_delivery_minutes <= 0 OR actual_delivery_minutes IS NULL OR promised_delivery_minutes IS NULL
UNION ALL
SELECT CURRENT_TIMESTAMP, 'invalid_delivery_sla_flag', 'ERROR', 'staging.int_deliveries', order_id,
       'Delivery SLA flag is null or outside 0/1',
       'Recompute or correct SLA flag before reporting SLA compliance.',
       _source_file, _loaded_at
FROM staging.int_deliveries
WHERE delivery_sla_met_flag NOT IN (0, 1) OR delivery_sla_met_flag IS NULL
UNION ALL
SELECT CURRENT_TIMESTAMP, 'order_total_formula_mismatch', 'ERROR', 'staging.int_orders', order_id,
       'Order total does not equal subtotal minus discount plus tax and delivery fee',
       'Reject from financial reporting if variance exceeds tolerance; otherwise document rounding rule.',
       _source_file, _loaded_at
FROM staging.int_orders
WHERE ABS((subtotal_ngn - discount_ngn + tax_ngn + delivery_fee_ngn) - total_amount_ngn) > 0.05
UNION ALL
SELECT CURRENT_TIMESTAMP, 'order_item_subtotal_mismatch', 'ERROR', 'staging.int_orders', o.order_id,
       'Order item revenue does not reconcile to order subtotal',
       'Reject affected order from product/revenue reporting until order lines or order header are corrected.',
       o._source_file, o._loaded_at
FROM staging.int_orders o
JOIN staging.int_order_items i USING (order_id)
GROUP BY o.order_id, o.subtotal_ngn, o._source_file, o._loaded_at
HAVING ABS(SUM(i.product_revenue) - o.subtotal_ngn) > 0.01
UNION ALL
SELECT CURRENT_TIMESTAMP, 'order_items_count_mismatch', 'ERROR', 'staging.int_orders', o.order_id,
       'Order items_count does not match the number of order item rows',
       'Reconcile order header and line-item feed before using item count metrics.',
       o._source_file, o._loaded_at
FROM staging.int_orders o
JOIN staging.int_order_items i USING (order_id)
GROUP BY o.order_id, o.items_count, o._source_file, o._loaded_at
HAVING COUNT(*) != o.items_count
UNION ALL
SELECT CURRENT_TIMESTAMP, 'order_quantity_total_mismatch', 'ERROR', 'staging.int_orders', o.order_id,
       'Order qty_total does not match summed item quantity',
       'Reconcile order header and line-item feed before using quantity metrics.',
       o._source_file, o._loaded_at
FROM staging.int_orders o
JOIN staging.int_order_items i USING (order_id)
GROUP BY o.order_id, o.qty_total, o._source_file, o._loaded_at
HAVING SUM(i.quantity) != o.qty_total
UNION ALL
SELECT CURRENT_TIMESTAMP, 'missing_fact_store_mapping', 'ERROR', 'marts.fact_orders', order_id,
       'Fact order did not resolve to a store surrogate key',
       'Fix store SCD effective dating or source store mapping before BI refresh.',
       NULL::VARCHAR, NULL::TIMESTAMP
FROM marts.fact_orders
WHERE store_sk IS NULL
UNION ALL
SELECT CURRENT_TIMESTAMP, 'missing_fact_product_mapping', 'ERROR', 'marts.fact_order_items', order_id || '|' || product_id,
       'Fact order item did not resolve to a product surrogate key',
       'Fix product dimension mapping before BI product reporting.',
       NULL::VARCHAR, NULL::TIMESTAMP
FROM marts.fact_order_items
WHERE product_sk IS NULL
UNION ALL
SELECT CURRENT_TIMESTAMP, 'fact_orders_null_foreign_key', 'ERROR', 'marts.fact_orders', order_id,
       'Fact order contains a null dimensional foreign key',
       'Fix the upstream dimension mapping before publishing the BI semantic layer.',
       _source_file, _loaded_at
FROM marts.fact_orders
WHERE customer_sk IS NULL OR store_sk IS NULL OR channel_sk IS NULL OR payment_method_sk IS NULL OR promo_sk IS NULL OR date_sk IS NULL
UNION ALL
SELECT CURRENT_TIMESTAMP, 'fact_order_items_null_foreign_key', 'ERROR', 'marts.fact_order_items', order_id || '|' || product_id,
       'Fact order item contains a null dimensional foreign key',
       'Fix product, store, channel, or date mapping before publishing product BI metrics.',
       _source_file, _loaded_at
FROM marts.fact_order_items
WHERE product_sk IS NULL OR store_sk IS NULL OR channel_sk IS NULL OR date_sk IS NULL
UNION ALL
SELECT CURRENT_TIMESTAMP, 'fact_deliveries_null_foreign_key', 'ERROR', 'marts.fact_deliveries', order_id,
       'Fact delivery contains a null dimensional foreign key',
       'Fix store or date mapping before publishing delivery BI metrics.',
       _source_file, _loaded_at
FROM marts.fact_deliveries
WHERE store_sk IS NULL OR date_sk IS NULL
UNION ALL
SELECT CURRENT_TIMESTAMP, 'duplicate_fact_order_item_grain', 'ERROR', 'marts.fact_order_items', order_id || '|' || product_id,
       'Fact order item has more than one row at its declared order/product grain',
       'Deduplicate line items or introduce a true line-item identifier before product reporting.',
       MIN(_source_file), MAX(_loaded_at)
FROM marts.fact_order_items
GROUP BY order_id, product_id
HAVING COUNT(*) > 1
UNION ALL
SELECT CURRENT_TIMESTAMP, 'promo_with_zero_discount', 'WARN', 'staging.int_orders', order_id,
       'Promo code supplied with zero discount',
       'Load order, but ask commercial team to confirm whether promo code was informational or discount was missed.',
       _source_file, _loaded_at
FROM staging.int_orders
WHERE promo_code IS NOT NULL AND promo_code <> '' AND discount_ngn = 0
UNION ALL
SELECT CURRENT_TIMESTAMP, 'late_arriving_order_records', 'WARN', 'staging.int_orders', order_id,
       'Order business date is more than one day before its load date',
       'Load record and preserve raw audit trail; review whether it is a correction/backfill and rerun impacted reporting date.',
       _source_file, _loaded_at
FROM staging.int_orders
WHERE CAST(_loaded_at AS DATE) > CAST(order_timestamp AS DATE) + INTERVAL 1 DAY
  AND CAST(order_timestamp AS DATE) >= CURRENT_DATE - INTERVAL 30 DAY
UNION ALL
SELECT CURRENT_TIMESTAMP, 'unusual_daily_order_volume_change', 'WARN', 'staging.int_orders', CAST(order_date AS VARCHAR),
       'Daily order volume changed by more than 50 percent versus the prior day',
       'Load data, but compare to source-system control totals and confirm no missing file/store/channel before dashboard sign-off.',
       NULL::VARCHAR, NULL::TIMESTAMP
FROM (
    WITH daily_counts AS (
        SELECT
            CAST(order_timestamp AS DATE) AS order_date,
            COUNT(*) AS order_count,
            LAG(COUNT(*)) OVER (ORDER BY CAST(order_timestamp AS DATE)) AS previous_order_count
        FROM staging.int_orders
        GROUP BY CAST(order_timestamp AS DATE)
    )
    SELECT order_date
    FROM daily_counts
    WHERE previous_order_count >= 10
      AND ABS(order_count - previous_order_count)::DOUBLE / NULLIF(previous_order_count, 0) > 0.50
)
)
SELECT
    getvariable('audit_run_id') AS run_id,
    CAST(getvariable('audit_execution_date') AS DATE) AS execution_date,
    *
FROM exception_rows;

CREATE TABLE IF NOT EXISTS audit.data_quality_exceptions_history AS
SELECT * FROM audit.data_quality_exceptions WHERE FALSE;

DELETE FROM audit.data_quality_exceptions_history
WHERE run_id = getvariable('audit_run_id');

INSERT INTO audit.data_quality_exceptions_history
SELECT * FROM audit.data_quality_exceptions;

CREATE OR REPLACE VIEW audit.vw_data_quality_exception_summary AS
SELECT
    check_name,
    severity,
    source_table,
    COUNT(*) AS exception_count,
    MIN(detected_at) AS first_detected_at,
    MAX(detected_at) AS last_detected_at
FROM audit.data_quality_exceptions
GROUP BY check_name, severity, source_table;
