-- Source profiling queries for Question 1.
-- Run from the project root with:
--   duckdb -c ".read warehouse/source_profiling.sql"

CREATE OR REPLACE TEMP TABLE source_profile_summary AS
SELECT
    'customers.json' AS source_file,
    COUNT(*) AS record_count,
    COUNT(DISTINCT customer_id) AS distinct_business_keys,
    COUNT(*) - COUNT(DISTINCT customer_id) AS duplicate_business_keys,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_critical_keys,
    MIN(CAST(signup_date AS DATE)) AS min_business_date,
    MAX(CAST(signup_date AS DATE)) AS max_business_date
FROM read_json_auto('dataSource/customers.json', hive_partitioning=false)
UNION ALL
SELECT
    'stores.json',
    COUNT(*),
    COUNT(DISTINCT store_id),
    COUNT(*) - COUNT(DISTINCT store_id),
    SUM(CASE WHEN store_id IS NULL THEN 1 ELSE 0 END),
    NULL::DATE,
    NULL::DATE
FROM read_json_auto('dataSource/stores.json', hive_partitioning=false)
UNION ALL
SELECT
    'products.json',
    COUNT(*),
    COUNT(DISTINCT product_id),
    COUNT(*) - COUNT(DISTINCT product_id),
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END),
    NULL::DATE,
    NULL::DATE
FROM read_json_auto('dataSource/products.json', hive_partitioning=false)
UNION ALL
SELECT
    'orders.json',
    COUNT(*),
    COUNT(DISTINCT order_id),
    COUNT(*) - COUNT(DISTINCT order_id),
    SUM(CASE WHEN order_id IS NULL OR customer_id IS NULL OR store_id IS NULL THEN 1 ELSE 0 END),
    MIN(CAST(order_ts AS DATE)),
    MAX(CAST(order_ts AS DATE))
FROM read_json_auto('dataSource/orders.json', hive_partitioning=false)
UNION ALL
SELECT
    'order_items.json',
    COUNT(*),
    COUNT(DISTINCT order_id || '|' || product_id),
    COUNT(*) - COUNT(DISTINCT order_id || '|' || product_id),
    SUM(CASE WHEN order_id IS NULL OR product_id IS NULL THEN 1 ELSE 0 END),
    NULL::DATE,
    NULL::DATE
FROM read_json_auto('dataSource/order_items.json', hive_partitioning=false)
UNION ALL
SELECT
    'deliveries.json',
    COUNT(*),
    COUNT(DISTINCT order_id),
    COUNT(*) - COUNT(DISTINCT order_id),
    SUM(CASE WHEN order_id IS NULL OR store_id IS NULL THEN 1 ELSE 0 END),
    MIN(CAST(order_ts AS DATE)),
    MAX(CAST(order_ts AS DATE))
FROM read_json_auto('dataSource/deliveries.json', hive_partitioning=false);

CREATE OR REPLACE TEMP TABLE source_relationship_profile AS
SELECT 'orders.customer_id -> customers.customer_id' AS relationship, COUNT(*) AS orphan_count
FROM read_json_auto('dataSource/orders.json', hive_partitioning=false) o
LEFT JOIN read_json_auto('dataSource/customers.json', hive_partitioning=false) c USING (customer_id)
WHERE c.customer_id IS NULL
UNION ALL
SELECT 'orders.store_id -> stores.store_id', COUNT(*)
FROM read_json_auto('dataSource/orders.json', hive_partitioning=false) o
LEFT JOIN read_json_auto('dataSource/stores.json', hive_partitioning=false) s USING (store_id)
WHERE s.store_id IS NULL
UNION ALL
SELECT 'order_items.order_id -> orders.order_id', COUNT(*)
FROM read_json_auto('dataSource/order_items.json', hive_partitioning=false) i
LEFT JOIN read_json_auto('dataSource/orders.json', hive_partitioning=false) o USING (order_id)
WHERE o.order_id IS NULL
UNION ALL
SELECT 'order_items.product_id -> products.product_id', COUNT(*)
FROM read_json_auto('dataSource/order_items.json', hive_partitioning=false) i
LEFT JOIN read_json_auto('dataSource/products.json', hive_partitioning=false) p USING (product_id)
WHERE p.product_id IS NULL
UNION ALL
SELECT 'deliveries.order_id -> orders.order_id', COUNT(*)
FROM read_json_auto('dataSource/deliveries.json', hive_partitioning=false) d
LEFT JOIN read_json_auto('dataSource/orders.json', hive_partitioning=false) o USING (order_id)
WHERE o.order_id IS NULL;

SELECT * FROM source_profile_summary ORDER BY source_file;
SELECT * FROM source_relationship_profile ORDER BY relationship;
