-- core_semantic_layer.sql
-- The BI-facing layer (Q8). Reads only from marts.* — never from
-- intermediate.* or raw parquet directly, so the BI tool's model is fully
-- insulated from anything upstream changing. Run this AFTER build_marts.sql.
-- All views, not tables: this layer is pure re-shaping/aggregation of
-- marts data, so there's nothing here that needs its own persisted state.

CREATE SCHEMA IF NOT EXISTS core;

-- ============================================================================
-- CORE.VW_SALES_PERFORMANCE — grain: one row per (date, store, channel).
-- Primary view for revenue/order dashboards. KPI definitions baked in here
-- are the ones that must be computed consistently regardless of which BI
-- tool consumes them — AOV and revenue in particular should never be
-- recalculated differently in Power BI vs Tableau vs an ad-hoc query.
-- ============================================================================
CREATE OR REPLACE VIEW core.vw_sales_performance AS
SELECT
    dd.full_date,
    dd.year,
    dd.month,
    dd.week,
    dd.day_name,
    dd.is_weekend,
    ds.store_id,
    ds.clean_city                         AS city,
    dc.channel,
    COUNT(DISTINCT fo.order_id)           AS order_count,
    SUM(ros.order_revenue)                AS total_revenue,
    -- AOV defined once, here — not left to be recomputed inconsistently
    -- downstream in the BI tool.
    ROUND(SUM(ros.order_revenue) / NULLIF(COUNT(DISTINCT fo.order_id), 0), 2) AS avg_order_value,
    SUM(ros.items_per_order)              AS total_items,
    ROUND(SUM(ros.items_per_order)::DOUBLE / NULLIF(COUNT(DISTINCT fo.order_id), 0), 2) AS avg_items_per_order,
    SUM(CASE WHEN fo.promo_flag = 1 THEN 1 ELSE 0 END) AS promo_order_count,
    SUM(CASE WHEN fo.new_vs_returning_customer_flag = 'New' THEN 1 ELSE 0 END) AS new_customer_orders,
    SUM(CASE WHEN fo.new_vs_returning_customer_flag = 'Returning' THEN 1 ELSE 0 END) AS returning_customer_orders
FROM marts.fact_orders fo
JOIN marts.dim_date dd    ON fo.date_sk = dd.date_sk
JOIN marts.dim_store ds   ON fo.store_sk = ds.store_sk
JOIN marts.dim_channel dc ON fo.channel_sk = dc.channel_sk
LEFT JOIN marts.rpt_order_summary ros ON fo.order_id = ros.order_id
GROUP BY dd.full_date, dd.year, dd.month, dd.week, dd.day_name, dd.is_weekend,
         ds.store_id, ds.clean_city, dc.channel;

-- ============================================================================
-- CORE.VW_DELIVERY_PERFORMANCE — grain: one row per (date, store).
-- SLA compliance rate defined once here — same reasoning as AOV above.
-- ============================================================================
CREATE OR REPLACE VIEW core.vw_delivery_performance AS
SELECT
    dd.full_date,
    dd.year,
    dd.month,
    ds.store_id,
    ds.clean_city                         AS city,
    COUNT(*)                              AS delivery_count,
    SUM(fd.delivery_sla_met_flag)         AS sla_met_count,
    ROUND(SUM(fd.delivery_sla_met_flag)::DOUBLE / NULLIF(COUNT(*), 0) * 100, 1) AS sla_compliance_pct,
    ROUND(AVG(fd.actual_delivery_minutes), 1) AS avg_delivery_minutes,
    ROUND(AVG(fd.delay_minutes), 1)       AS avg_delay_minutes
FROM marts.fact_deliveries fd
JOIN marts.dim_date dd  ON fd.date_sk = dd.date_sk
JOIN marts.dim_store ds ON fd.store_sk = ds.store_sk
GROUP BY dd.full_date, dd.year, dd.month, ds.store_id, ds.clean_city;

-- ============================================================================
-- CORE.VW_PRODUCT_PERFORMANCE — grain: one row per (date, store, channel,
-- product). Product facts carry these dimensional keys directly, so this
-- view avoids fact-to-fact joins and stays at the item fact grain.
-- ============================================================================
CREATE OR REPLACE VIEW core.vw_product_performance AS
SELECT
    dd.full_date,
    dd.year,
    dd.month,
    ds.store_id,
    ds.clean_city                         AS city,
    dc.channel,
    dp.product_id,
    dp.unified_category                   AS category,
    SUM(foi.quantity)                     AS units_sold,
    SUM(foi.product_revenue)              AS product_revenue
FROM marts.fact_order_items foi
JOIN marts.dim_date dd     ON foi.date_sk = dd.date_sk
JOIN marts.dim_store ds    ON foi.store_sk = ds.store_sk
JOIN marts.dim_channel dc  ON foi.channel_sk = dc.channel_sk
JOIN marts.dim_product dp  ON foi.product_sk = dp.product_sk
GROUP BY dd.full_date, dd.year, dd.month, ds.store_id, ds.clean_city,
         dc.channel, dp.product_id, dp.unified_category;

-- ============================================================================
-- CORE.VW_CUSTOMER_SUMMARY — grain: one row per customer.
-- Lifetime view, not date-grained — for customer-segment dashboards
-- (new vs returning mix, lifetime value) rather than trend-over-time ones.
-- ============================================================================
CREATE OR REPLACE VIEW core.vw_customer_summary AS
SELECT
    dcu.customer_id,
    COUNT(DISTINCT fo.order_id)           AS lifetime_order_count,
    SUM(ros.order_revenue)                AS lifetime_revenue,
    MIN(fo.order_timestamp)               AS first_order_at,
    MAX(fo.order_timestamp)               AS most_recent_order_at,
    MAX(fo.customer_type)                 AS latest_customer_type
FROM marts.fact_orders fo
JOIN marts.dim_customer dcu ON fo.customer_sk = dcu.customer_sk
LEFT JOIN marts.rpt_order_summary ros ON fo.order_id = ros.order_id
GROUP BY dcu.customer_id;
