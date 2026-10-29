-- =============================================================================
-- ANALYTICAL QUERIES against the Gold star schema.
--
-- Run them all through the pipeline:   python -m src.pipeline
-- or only the queries:                 python -m src.pipeline --analytics-only
--
-- "Current" means the calendar month of the reference date as_of_date.
-- It defaults to today (current_date). Python (or you, in a DuckDB shell) can
-- set it first to report on another month, e.g.
--     SET VARIABLE as_of_date = DATE '2026-10-15';
-- The statement below keeps a value that was already set.
--
-- CURRENCY NOTE: amounts are stored in their original currency (USD/EUR/GBP)
-- and no exchange rates are part of the source data. Query 1 and Query 2
-- answer the questions exactly as the brief asks them (one total per category,
-- five regions), so they follow the brief's simplification and treat amounts
-- as nominal values. Each has a currency-safe companion that never adds two
-- currencies together:
--   * Query 1b = Query 1 split by currency
--   * Query 3  = Query 2 calculated separately for each currency
--
-- Each query starts with a "-- name:" line; src/warehouse.py uses that marker
-- to run and label the queries.
-- =============================================================================

SET VARIABLE as_of_date = COALESCE(getvariable('as_of_date'), current_date);

-- name: q1_monthly_revenue_by_category
-- Total transaction amount per category during the current calendar month.
-- Half-open range on the transaction timestamp:
--     first day of this month 00:00:00  <=  timestamp  <  first day of next month
-- This includes the whole last day of the month and never double-counts
-- midnight on the 1st of the next month.
SELECT
    f.category,
    SUM(f.transaction_amount)  AS total_transaction_amount,
    COUNT(*)                   AS transaction_count
FROM gold.fact_transactions AS f
WHERE f."timestamp" >= date_trunc('month', getvariable('as_of_date'))
  AND f."timestamp" <  date_trunc('month', getvariable('as_of_date')) + INTERVAL 1 MONTH
GROUP BY f.category
ORDER BY total_transaction_amount DESC, f.category;

-- name: q1b_monthly_revenue_by_category_and_currency
-- Additional query: Query 1 split by currency (same half-open month filter),
-- so no two currencies are ever added together.
SELECT
    f.category,
    f.currency,
    SUM(f.transaction_amount)  AS total_transaction_amount,
    COUNT(*)                   AS transaction_count
FROM gold.fact_transactions AS f
WHERE f."timestamp" >= date_trunc('month', getvariable('as_of_date'))
  AND f."timestamp" <  date_trunc('month', getvariable('as_of_date')) + INTERVAL 1 MONTH
GROUP BY f.category, f.currency
ORDER BY f.category, f.currency;

-- name: q2_top_5_regions_by_avg_transaction_amount_per_entity
-- Top 5 regions by the average of entity-level average transaction amounts.
--   1. entity_averages: one row per entity = that entity's mean transaction amount
--   2. join each entity to its region (dim_entity)
--   3. average those entity means per region, so every entity counts once,
--      however many transactions it has
--   4. highest first, top 5 (region name breaks ties so the output is stable)
-- Entities with no transactions have no average and are not included.
WITH entity_averages AS (
    SELECT
        f.entity_key,
        AVG(f.transaction_amount) AS entity_avg_amount
    FROM gold.fact_transactions AS f
    GROUP BY f.entity_key
)
SELECT
    e.region,
    ROUND(AVG(a.entity_avg_amount), 2)  AS average_transaction_amount_per_entity,
    COUNT(*)                            AS entity_count
FROM entity_averages AS a
JOIN gold.dim_entity AS e ON e.entity_key = a.entity_key
GROUP BY e.region
ORDER BY AVG(a.entity_avg_amount) DESC, e.region
LIMIT 5;

-- name: q3_top_5_regions_per_currency
-- Additional query: the Query 2 calculation done separately for each currency,
-- so no two currencies are ever mixed. Up to 5 regions per currency.
WITH entity_averages AS (
    SELECT
        f.entity_key,
        f.currency,
        AVG(f.transaction_amount) AS entity_avg_amount
    FROM gold.fact_transactions AS f
    GROUP BY f.entity_key, f.currency
),
region_averages AS (
    SELECT
        a.currency,
        e.region,
        AVG(a.entity_avg_amount) AS avg_of_entity_averages,
        COUNT(*)                 AS entity_count
    FROM entity_averages AS a
    JOIN gold.dim_entity AS e ON e.entity_key = a.entity_key
    GROUP BY a.currency, e.region
)
SELECT
    currency,
    region,
    ROUND(avg_of_entity_averages, 2) AS average_transaction_amount_per_entity,
    entity_count
FROM region_averages
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY currency
    ORDER BY avg_of_entity_averages DESC, region
) <= 5
ORDER BY currency, avg_of_entity_averages DESC, region;
