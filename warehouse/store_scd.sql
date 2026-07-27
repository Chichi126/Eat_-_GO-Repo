-- dim_store_scd.sql
-- Run this BEFORE build_marts.sql. dim_store is removed from build_marts.sql's
-- CREATE OR REPLACE block entirely — this file is now the only thing that
-- creates or modifies marts.dim_store. Safe to re-run daily: no-op on days
-- with no attribute changes and no new stores.

-- Stable surrogate key generator. A SEQUENCE, not ROW_NUMBER() — it must
-- never reassign a value that's already been given out, since fact rows
-- from prior loads still reference old store_sk values.
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SEQUENCE IF NOT EXISTS marts.seq_dim_store_sk START 1;

-- Persists across runs. NOT "CREATE OR REPLACE" — a full rebuild here would
-- destroy every historical version, defeating the point of SCD2.
CREATE TABLE IF NOT EXISTS marts.dim_store (
    store_sk        BIGINT PRIMARY KEY,
    store_id        VARCHAR,
    clean_city      VARCHAR,
    -- Add other genuinely business/reportable store attributes here as you
    -- confirm them (e.g. store_name, region) — include each one in
    -- row_hash below too, or a change in that column won't be detected.
    row_hash        VARCHAR,
    effective_from  DATE,
    effective_to    DATE,
    is_current      BOOLEAN
);

-- Latest observed version of every store from today's intermediate load.
-- row_hash fingerprints the business attributes so a change is detected
-- generically instead of comparing each column by hand.
CREATE OR REPLACE TEMP TABLE stg_store_latest AS
SELECT
    store_id,
    clean_city,
    md5(CONCAT_WS('|', store_id, clean_city)) AS row_hash
FROM staging.int_stores;

-- Resolves to Airflow's DAG logical/execution date when the caller sets it
-- (see the BashOperator command in the DAG) — this is what makes backfills
-- stamp the correct historical date instead of today's wall-clock date.
-- Falls back to CURRENT_DATE so the file still runs correctly stand-alone
-- (e.g. opened directly in the DuckDB CLI/DBeaver with no variable set).
SET VARIABLE load_date = COALESCE(getvariable('exec_date'), CURRENT_DATE);

-- On the first dimension bootstrap, use an open historical start date so
-- historical order facts can join to the initial store snapshot. Later new
-- stores and changed versions still begin on the pipeline load date.
CREATE OR REPLACE TEMP TABLE scd_run_context AS
SELECT COUNT(*) = 0 AS is_initial_bootstrap
FROM marts.dim_store;

-- MERGE handles two of the three cases in one pass:
--   MATCHED + row_hash changed  -> close out the old current version
--   NOT MATCHED                 -> brand-new store, insert as current
-- It can't also insert the new post-change version for a store that just
-- matched — MERGE takes exactly one action per matched source row, so a
-- store that "matched" (same store_id already exists) can be updated OR
-- inserted-as-new, never both in the same statement. That's what the
-- follow-up INSERT below is for.
MERGE INTO marts.dim_store d
USING stg_store_latest s
ON d.store_id = s.store_id AND d.is_current = TRUE
WHEN MATCHED AND d.row_hash != s.row_hash THEN
    UPDATE SET effective_to = getvariable('load_date') - INTERVAL 1 DAY,
               is_current = FALSE
WHEN NOT MATCHED THEN
    INSERT (store_sk, store_id, clean_city, row_hash, effective_from, effective_to, is_current)
    VALUES (
        nextval('marts.seq_dim_store_sk'),
        s.store_id,
        s.clean_city,
        s.row_hash,
        CASE
            WHEN (SELECT is_initial_bootstrap FROM scd_run_context) THEN DATE '1900-01-01'
            ELSE getvariable('load_date')
        END,
        DATE '9999-12-31',
        TRUE
    );

-- Insert the new current version for every store just closed above by the
-- MERGE's UPDATE branch. Same "no open current row" condition as before —
-- this only fires for stores that changed, since unchanged stores still
-- have their (never-touched) current row and brand-new stores were already
-- inserted directly by the MERGE.
INSERT INTO marts.dim_store (store_sk, store_id, clean_city, row_hash, effective_from, effective_to, is_current)
SELECT
    nextval('marts.seq_dim_store_sk'),
    s.store_id,
    s.clean_city,
    s.row_hash,
    getvariable('load_date'),
    DATE '9999-12-31',
    TRUE
FROM stg_store_latest s
LEFT JOIN marts.dim_store d
       ON s.store_id = d.store_id AND d.is_current = TRUE
WHERE d.store_id IS NULL;
