-- build_marts.sql
-- Reads exclusively from intermediate.* tables already materialized in the
-- persisted DuckDB warehouse file — no S3/httpfs setup needed here, that
-- already happened upstream in load_staging / load_intermediate.
-- Safe to re-run: every object uses CREATE OR REPLACE.

CREATE SCHEMA IF NOT EXISTS marts;

-- ============================================================================
-- DIM_DATE — day grain. order_hour is intentionally NOT included here — it
-- stays on the fact tables as a degenerate attribute since it's intraday,
-- not a property of the calendar day itself.
-- PK: date_sk (the date value itself)
-- ============================================================================
CREATE OR REPLACE TABLE marts.dim_date AS
WITH bounds AS (
    SELECT MIN(CAST(order_timestamp AS DATE)) AS min_d,
           MAX(CAST(order_timestamp AS DATE)) AS max_d
    FROM staging.int_orders
),
days AS (
    SELECT UNNEST(GENERATE_SERIES((SELECT min_d FROM bounds), (SELECT max_d FROM bounds), INTERVAL 1 DAY))::DATE AS full_date
)
SELECT
    full_date                        AS date_sk,
    full_date,
    YEAR(full_date)                  AS year,
    MONTH(full_date)                 AS month,
    WEEK(full_date)                  AS week,
    DAYOFWEEK(full_date)             AS day_of_week,
    DAYNAME(full_date)               AS day_name,
    CASE WHEN DAYOFWEEK(full_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM days;

-- ============================================================================
-- DIM_CUSTOMER — PK: customer_sk (surrogate). Business key: customer_id.
-- ============================================================================
CREATE OR REPLACE TABLE marts.dim_customer AS
SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_sk,
    customer_id,
    signup_date,
    city,
    phone,
    is_business
FROM staging.int_customers;

-- ============================================================================
-- DIM_STORE — NOT built here. dim_store is an SCD Type 2 table maintained
-- by dim_store_scd.sql, which must run BEFORE this file (see the Airflow
-- task ordering: load_dim_store_scd >> load_marts). Rebuilding it here with
-- CREATE OR REPLACE would destroy its version history on every run.
-- ============================================================================

-- ============================================================================
-- DIM_PRODUCT — PK: product_sk. Business key: product_id.
-- ============================================================================
CREATE OR REPLACE TABLE marts.dim_product AS
SELECT
    ROW_NUMBER() OVER (ORDER BY product_id) AS product_sk,
    product_id,
    product_name,
    unified_category,
    base_price_ngn
FROM staging.int_products;

-- ============================================================================
-- DIM_CHANNEL — derived (no dedicated source file). PK: channel_sk.
-- ============================================================================
CREATE OR REPLACE TABLE marts.dim_channel AS
SELECT
    ROW_NUMBER() OVER (ORDER BY channel) AS channel_sk,
    channel
FROM (SELECT DISTINCT channel FROM staging.int_orders WHERE channel IS NOT NULL);

-- ============================================================================
-- DIM_PAYMENT_METHOD — derived. PK: payment_method_sk.
-- ============================================================================
CREATE OR REPLACE TABLE marts.dim_payment_method AS
SELECT
    ROW_NUMBER() OVER (ORDER BY payment_method) AS payment_method_sk,
    payment_method
FROM (SELECT DISTINCT payment_method FROM staging.int_orders WHERE payment_method IS NOT NULL);

-- ============================================================================
-- DIM_PROMO — derived. promo_sk = 0 is a dedicated "No Promo" row so
-- fact_orders never needs a nullable FK for non-promo orders.
-- ============================================================================
CREATE OR REPLACE TABLE marts.dim_promo AS
SELECT 0 AS promo_sk, NULL AS promo_code, FALSE AS is_promo
UNION ALL
SELECT
    ROW_NUMBER() OVER (ORDER BY promo_code) AS promo_sk,
    promo_code,
    TRUE AS is_promo
FROM (
    SELECT DISTINCT promo_code
    FROM staging.int_orders
    WHERE promo_code IS NOT NULL AND promo_code != ''
);

-- ============================================================================
-- FACT_ORDERS — grain: one row per order. PK: order_id.
-- Use this table, not fact_order_items, for order-level KPIs (order count,
-- AOV, new-vs-returning mix) — joining to items fans out one order into N
-- rows and inflates any COUNT/SUM computed from that join.
-- dim_store join is date-ranged (not a plain store_id match), since
-- dim_store is SCD2 — a store_id can map to multiple historical rows.
-- ============================================================================
CREATE OR REPLACE TABLE marts.fact_orders AS
SELECT
    o.order_id,
    c.customer_sk,
    s.store_sk,
    ch.channel_sk,
    pm.payment_method_sk,
    COALESCE(pr.promo_sk, 0) AS promo_sk,
    CAST(o.order_timestamp AS DATE) AS date_sk,
    o.order_timestamp,
    o.order_hour,
    o.promo_flag,
    o.new_vs_returning_customer_flag,
    o.customer_type,
    o.items_count,
    o.qty_total,
    o.subtotal_ngn,
    o.discount_ngn,
    o.tax_ngn,
    o.delivery_fee_ngn,
    o.total_amount_ngn,
    1 AS order_count,
    o._source_file,
    o._loaded_at,
    o._batch_id
FROM staging.int_orders o
LEFT JOIN marts.dim_customer c        ON o.customer_id = c.customer_id
LEFT JOIN marts.dim_store s            ON o.store_id = s.store_id
                                       AND CAST(o.order_timestamp AS DATE) >= s.effective_from
                                       AND CAST(o.order_timestamp AS DATE) <= s.effective_to
LEFT JOIN marts.dim_channel ch          ON o.channel = ch.channel
LEFT JOIN marts.dim_payment_method pm   ON o.payment_method = pm.payment_method
LEFT JOIN marts.dim_promo pr            ON o.promo_code = pr.promo_code;

-- ============================================================================
-- FACT_ORDER_ITEMS — grain: one row per (order_id, product_id) line item.
-- PK: composite (order_id, product_id) — source has no dedicated line-item id.
-- Carries date/store/channel keys directly so product BI does not need to
-- join through fact_orders. This keeps facts additive at their own grain.
-- ============================================================================
CREATE OR REPLACE TABLE marts.fact_order_items AS
SELECT
    oi.order_id,
    oi.product_id,
    p.product_sk,
    CAST(oi.order_timestamp AS DATE) AS date_sk,
    s.store_sk,
    ch.channel_sk,
    oi.order_timestamp,
    oi.promo_flag,
    oi.quantity,
    oi.unit_price,
    oi.product_revenue,
    oi._source_file,
    oi._loaded_at,
    oi._batch_id
FROM staging.int_order_items oi
LEFT JOIN marts.dim_product p ON oi.product_id = p.product_id
LEFT JOIN marts.dim_store s   ON oi.store_id = s.store_id
                              AND CAST(oi.order_timestamp AS DATE) >= s.effective_from
                              AND CAST(oi.order_timestamp AS DATE) <= s.effective_to
LEFT JOIN marts.dim_channel ch ON oi.channel = ch.channel;

-- ============================================================================
-- FACT_DELIVERIES — grain: one row per order (source has no delivery_id —
-- confirmed 1:1 with orders in the intermediate layer). PK: order_id.
-- dim_store join is date-ranged for the same SCD2 reason as fact_orders.
-- ============================================================================
CREATE OR REPLACE TABLE marts.fact_deliveries AS
SELECT
    d.order_id,
    s.store_sk,
    CAST(d.order_timestamp AS DATE) AS date_sk,
    d.promised_delivery_minutes,
    d.actual_delivery_minutes,
    d.delay_minutes,
    d.delivery_sla_met_flag,
    d._source_file,
    d._loaded_at,
    d._batch_id
FROM staging.int_deliveries d
LEFT JOIN marts.dim_store s ON d.store_id = s.store_id
                            AND CAST(d.order_timestamp AS DATE) >= s.effective_from
                            AND CAST(d.order_timestamp AS DATE) <= s.effective_to;

-- ============================================================================
-- RPT_ORDER_SUMMARY — reporting view, not a materialized table: items-per-
-- order and order-level revenue, derived from the two fact tables rather
-- than stored redundantly. Covers the last two Q5 asks (items per order,
-- average order value — AOV is AVG(order_revenue) computed by the BI layer
-- or a downstream query over this view, not baked in here).
-- ============================================================================
CREATE OR REPLACE VIEW marts.rpt_order_summary AS
SELECT
    fo.order_id,
    fo.date_sk,
    fo.store_sk,
    fo.channel_sk,
    COUNT(foi.product_sk)     AS items_per_order,
    SUM(foi.product_revenue)  AS order_revenue
FROM marts.fact_orders fo
LEFT JOIN marts.fact_order_items foi ON fo.order_id = foi.order_id
GROUP BY fo.order_id, fo.date_sk, fo.store_sk, fo.channel_sk;
