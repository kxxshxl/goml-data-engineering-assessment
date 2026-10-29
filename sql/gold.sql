-- =============================================================================
-- GOLD LAYER: star schema for analytics.
--
--                 dim_date (date_key PK)
--                       ^
--                       | date_key (FK)
--   dim_entity <---- fact_transactions
--   (entity_key PK)  entity_key (FK)
--
-- Grain of fact_transactions: ONE ROW PER TRANSACTION (transaction_id).
--
-- Primary and foreign keys are declared, so DuckDB itself refuses any fact row
-- whose entity_key or date_key has no matching dimension row.
--
-- Tables are dropped and rebuilt every run (fact first, because it references
-- the dimensions). The whole build runs inside one transaction, so a failure
-- leaves the previous Gold tables untouched.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS gold;

DROP TABLE IF EXISTS gold.fact_transactions;
DROP TABLE IF EXISTS gold.dim_entity;
DROP TABLE IF EXISTS gold.dim_date;

-- -----------------------------------------------------------------------------
-- dim_entity: one row per entity.
-- entity_key is a surrogate key (a compact integer owned by the warehouse);
-- entity_id is kept as the natural/business key. Keys are assigned in
-- entity_id order, so a rebuild from the same data gives the same keys.
-- -----------------------------------------------------------------------------
CREATE TABLE gold.dim_entity (
    entity_key         INTEGER PRIMARY KEY,
    entity_id          UUID    NOT NULL UNIQUE,
    first_name         VARCHAR NOT NULL,
    last_name          VARCHAR NOT NULL,
    contact_email      VARCHAR NOT NULL,
    registration_date  DATE    NOT NULL,
    region             VARCHAR NOT NULL
);

INSERT INTO gold.dim_entity
SELECT
    ROW_NUMBER() OVER (ORDER BY entity_id) AS entity_key,
    entity_id,
    first_name,
    last_name,
    contact_email,
    registration_date,
    region
FROM silver.silver_entities;

-- -----------------------------------------------------------------------------
-- dim_date: one row per calendar day, from the first to the last transaction
-- date with no gaps (days without sales still exist, which keeps calendar
-- reporting correct). date_key = YYYYMMDD, identical to silver's date_key.
-- -----------------------------------------------------------------------------
CREATE TABLE gold.dim_date (
    date_key    INTEGER PRIMARY KEY,   -- YYYYMMDD, e.g. 20261027
    full_date   DATE    NOT NULL UNIQUE,
    year        INTEGER NOT NULL,
    quarter     INTEGER NOT NULL,
    month       INTEGER NOT NULL,
    month_name  VARCHAR NOT NULL,
    day         INTEGER NOT NULL
);

INSERT INTO gold.dim_date
WITH bounds AS (
    SELECT min(transaction_date) AS first_date, max(transaction_date) AS last_date
    FROM silver.silver_transactions
),
calendar AS (
    SELECT CAST(generated.day AS DATE) AS full_date
    FROM bounds,
         generate_series(bounds.first_date, bounds.last_date, INTERVAL 1 DAY) AS generated(day)
    WHERE bounds.first_date IS NOT NULL
)
SELECT
    CAST(strftime(full_date, '%Y%m%d') AS INTEGER) AS date_key,
    full_date,
    year(full_date)      AS year,
    quarter(full_date)   AS quarter,
    month(full_date)     AS month,
    monthname(full_date) AS month_name,
    day(full_date)       AS day
FROM calendar;

-- -----------------------------------------------------------------------------
-- fact_transactions: measures plus foreign keys to both dimensions.
-- "timestamp" keeps the exact event time; date_key links to the calendar day.
-- Silver already guarantees every entity exists, so the INNER JOINs drop
-- nothing (the pipeline verifies silver count = fact count).
-- -----------------------------------------------------------------------------
CREATE TABLE gold.fact_transactions (
    transaction_id      UUID           PRIMARY KEY,
    entity_key          INTEGER        NOT NULL REFERENCES gold.dim_entity (entity_key),
    date_key            INTEGER        NOT NULL REFERENCES gold.dim_date (date_key),
    category            VARCHAR        NOT NULL,
    transaction_amount  DECIMAL(18, 2) NOT NULL,
    currency            VARCHAR        NOT NULL,
    "timestamp"         TIMESTAMP      NOT NULL
);

INSERT INTO gold.fact_transactions
SELECT
    t.transaction_id,
    e.entity_key,
    d.date_key,
    t.category,
    t.transaction_amount,
    t.currency,
    t."timestamp"
FROM silver.silver_transactions AS t
JOIN gold.dim_entity AS e ON e.entity_id = t.entity_id
JOIN gold.dim_date   AS d ON d.date_key  = t.date_key;
