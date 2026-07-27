-- Source-to-reporting reconciliation controls (Question 7).
CREATE SCHEMA IF NOT EXISTS audit;

SET VARIABLE audit_execution_date = COALESCE(getvariable('exec_date'), CURRENT_DATE);
SET VARIABLE audit_run_id = COALESCE(
    getvariable('run_id'),
    'manual-' || CAST(getvariable('audit_execution_date') AS VARCHAR)
);

CREATE OR REPLACE TABLE audit.reconciliation_summary AS
WITH reconciliation_rows AS (
    SELECT CURRENT_TIMESTAMP AS reconciled_at, 'overall' AS reconciliation_level, 'orders_count' AS metric, 'ALL' AS dimension_key,
           (SELECT COUNT(*) FROM staging.int_orders)::DOUBLE AS source_value,
           (SELECT COUNT(*) FROM marts.fact_orders)::DOUBLE AS reporting_value
    UNION ALL
    SELECT CURRENT_TIMESTAMP, 'overall', 'deliveries_count', 'ALL',
           (SELECT COUNT(*) FROM staging.int_deliveries)::DOUBLE,
           (SELECT COUNT(*) FROM marts.fact_deliveries)::DOUBLE
    UNION ALL
    SELECT CURRENT_TIMESTAMP, 'overall', 'order_items_count', 'ALL',
           (SELECT COUNT(*) FROM staging.int_order_items)::DOUBLE,
           (SELECT COUNT(*) FROM marts.fact_order_items)::DOUBLE
    UNION ALL
    SELECT CURRENT_TIMESTAMP, 'overall', 'order_subtotal_ngn', 'ALL',
           COALESCE((SELECT SUM(subtotal_ngn) FROM staging.int_orders), 0)::DOUBLE,
           COALESCE((SELECT SUM(order_revenue) FROM marts.rpt_order_summary), 0)::DOUBLE
    UNION ALL
    SELECT CURRENT_TIMESTAMP, 'overall', 'product_revenue_ngn', 'ALL',
           COALESCE((SELECT SUM(product_revenue) FROM staging.int_order_items), 0)::DOUBLE,
           COALESCE((SELECT SUM(product_revenue) FROM marts.fact_order_items), 0)::DOUBLE
    UNION ALL
    SELECT CURRENT_TIMESTAMP, 'overall', 'delivery_sla_met_count', 'ALL',
           COALESCE((SELECT SUM(delivery_sla_met_flag) FROM staging.int_deliveries), 0)::DOUBLE,
           COALESCE((SELECT SUM(delivery_sla_met_flag) FROM marts.fact_deliveries), 0)::DOUBLE
    UNION ALL
    SELECT CURRENT_TIMESTAMP, 'overall', 'promo_order_count', 'ALL',
           COALESCE((SELECT SUM(CASE WHEN promo_code IS NOT NULL AND promo_code <> '' THEN 1 ELSE 0 END) FROM staging.int_orders), 0)::DOUBLE,
           COALESCE((SELECT SUM(promo_flag) FROM marts.fact_orders), 0)::DOUBLE
),
store_source AS (
    SELECT store_id, COUNT(*)::DOUBLE AS order_count, COALESCE(SUM(subtotal_ngn), 0)::DOUBLE AS revenue
    FROM staging.int_orders
    GROUP BY store_id
),
store_reporting AS (
    SELECT ds.store_id, COUNT(*)::DOUBLE AS order_count, COALESCE(SUM(ros.order_revenue), 0)::DOUBLE AS revenue
    FROM marts.fact_orders fo
    LEFT JOIN marts.dim_store ds ON fo.store_sk = ds.store_sk
    LEFT JOIN marts.rpt_order_summary ros ON fo.order_id = ros.order_id
    GROUP BY ds.store_id
),
channel_source AS (
    SELECT channel, COUNT(*)::DOUBLE AS order_count, COALESCE(SUM(subtotal_ngn), 0)::DOUBLE AS revenue
    FROM staging.int_orders
    GROUP BY channel
),
channel_reporting AS (
    SELECT dc.channel, COUNT(*)::DOUBLE AS order_count, COALESCE(SUM(ros.order_revenue), 0)::DOUBLE AS revenue
    FROM marts.fact_orders fo
    LEFT JOIN marts.dim_channel dc ON fo.channel_sk = dc.channel_sk
    LEFT JOIN marts.rpt_order_summary ros ON fo.order_id = ros.order_id
    GROUP BY dc.channel
),
product_source AS (
    SELECT product_id, COALESCE(SUM(product_revenue), 0)::DOUBLE AS product_revenue
    FROM staging.int_order_items
    GROUP BY product_id
),
product_reporting AS (
    SELECT foi.product_id, COALESCE(SUM(foi.product_revenue), 0)::DOUBLE AS product_revenue
    FROM marts.fact_order_items foi
    GROUP BY foi.product_id
),
promo_source AS (
    SELECT COALESCE(NULLIF(promo_code, ''), 'NO_PROMO') AS promo_key, COUNT(*)::DOUBLE AS order_count
    FROM staging.int_orders
    GROUP BY COALESCE(NULLIF(promo_code, ''), 'NO_PROMO')
),
promo_reporting AS (
    SELECT COALESCE(pr.promo_code, 'NO_PROMO') AS promo_key, COUNT(*)::DOUBLE AS order_count
    FROM marts.fact_orders fo
    LEFT JOIN marts.dim_promo pr ON fo.promo_sk = pr.promo_sk
    GROUP BY COALESCE(pr.promo_code, 'NO_PROMO')
),
delivery_store_source AS (
    SELECT store_id, COUNT(*)::DOUBLE AS delivery_count
    FROM staging.int_deliveries
    GROUP BY store_id
),
delivery_store_reporting AS (
    SELECT ds.store_id, COUNT(*)::DOUBLE AS delivery_count
    FROM marts.fact_deliveries fd
    LEFT JOIN marts.dim_store ds ON fd.store_sk = ds.store_sk
    GROUP BY ds.store_id
),
dimension_rows AS (
    SELECT CURRENT_TIMESTAMP, 'store', 'orders_count', COALESCE(s.store_id, r.store_id, 'UNKNOWN'),
           COALESCE(s.order_count, 0), COALESCE(r.order_count, 0)
    FROM store_source s FULL OUTER JOIN store_reporting r USING (store_id)
    UNION ALL
    SELECT CURRENT_TIMESTAMP, 'store', 'order_subtotal_ngn', COALESCE(s.store_id, r.store_id, 'UNKNOWN'),
           COALESCE(s.revenue, 0), COALESCE(r.revenue, 0)
    FROM store_source s FULL OUTER JOIN store_reporting r USING (store_id)
    UNION ALL
    SELECT CURRENT_TIMESTAMP, 'channel', 'orders_count', COALESCE(s.channel, r.channel, 'UNKNOWN'),
           COALESCE(s.order_count, 0), COALESCE(r.order_count, 0)
    FROM channel_source s FULL OUTER JOIN channel_reporting r USING (channel)
    UNION ALL
    SELECT CURRENT_TIMESTAMP, 'channel', 'order_subtotal_ngn', COALESCE(s.channel, r.channel, 'UNKNOWN'),
           COALESCE(s.revenue, 0), COALESCE(r.revenue, 0)
    FROM channel_source s FULL OUTER JOIN channel_reporting r USING (channel)
    UNION ALL
    SELECT CURRENT_TIMESTAMP, 'product', 'product_revenue_ngn', COALESCE(s.product_id, r.product_id, 'UNKNOWN'),
           COALESCE(s.product_revenue, 0), COALESCE(r.product_revenue, 0)
    FROM product_source s FULL OUTER JOIN product_reporting r USING (product_id)
    UNION ALL
    SELECT CURRENT_TIMESTAMP, 'promo', 'orders_count', COALESCE(s.promo_key, r.promo_key, 'UNKNOWN'),
           COALESCE(s.order_count, 0), COALESCE(r.order_count, 0)
    FROM promo_source s FULL OUTER JOIN promo_reporting r USING (promo_key)
    UNION ALL
    SELECT CURRENT_TIMESTAMP, 'delivery_store', 'deliveries_count', COALESCE(s.store_id, r.store_id, 'UNKNOWN'),
           COALESCE(s.delivery_count, 0), COALESCE(r.delivery_count, 0)
    FROM delivery_store_source s FULL OUTER JOIN delivery_store_reporting r USING (store_id)
),
all_rows AS (
    SELECT * FROM reconciliation_rows
    UNION ALL
    SELECT * FROM dimension_rows
)
SELECT
    getvariable('audit_run_id') AS run_id,
    CAST(getvariable('audit_execution_date') AS DATE) AS execution_date,
    reconciled_at,
    reconciliation_level,
    metric,
    dimension_key,
    source_value,
    reporting_value,
    reporting_value - source_value AS variance,
    CASE WHEN ABS(reporting_value - source_value) < 0.01 THEN 'PASS' ELSE 'FAIL' END AS status
FROM all_rows;

CREATE TABLE IF NOT EXISTS audit.reconciliation_summary_history AS
SELECT * FROM audit.reconciliation_summary WHERE FALSE;

DELETE FROM audit.reconciliation_summary_history
WHERE run_id = getvariable('audit_run_id');

INSERT INTO audit.reconciliation_summary_history
SELECT * FROM audit.reconciliation_summary;
