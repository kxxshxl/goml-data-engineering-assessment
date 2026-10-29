-- =============================================================================
-- SILVER LAYER: cleaned, standardised, deduplicated and conformed data.
-- Input: bronze.*   Output: silver.*
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS silver;

-- -----------------------------------------------------------------------------
-- silver_entities: exactly one row per entity_id.
--
-- Step 1 (standardised): tidy text fields.
--   * names: trim, collapse repeated internal spaces (casing is kept, because
--     forcing title case would corrupt names such as "McDonald" or "da Silva")
--   * email: trim + lower-case
--   * region: trim + upper-case ('north', ' South ', 'east' -> NORTH, SOUTH, EAST)
--
-- Step 2 (ranked): ROW_NUMBER() numbers the rows inside each entity_id group.
--   The first row gets 1; every other row in the group is a duplicate.
--
-- Survivor rule: keep the row that appears FIRST in the source file
-- (lowest source_record_number). For exact duplicates every copy is identical,
-- so the choice does not matter. If the same entity_id ever arrives with
-- conflicting attributes, the rule is still deterministic, and the pipeline
-- reports how many such conflicting IDs it found (see warehouse.py).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE silver.silver_entities AS
WITH standardised AS (
    SELECT
        entity_id,
        regexp_replace(trim(first_name), '\s+', ' ', 'g')     AS first_name,
        regexp_replace(trim(last_name), '\s+', ' ', 'g')      AS last_name,
        lower(trim(contact_email))                            AS contact_email,
        registration_date,
        upper(regexp_replace(trim(region), '\s+', ' ', 'g'))  AS region,
        source_record_number
    FROM bronze.bronze_entities
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY entity_id
            ORDER BY source_record_number
        ) AS row_num
    FROM standardised
)
SELECT
    entity_id,
    first_name,
    last_name,
    contact_email,
    registration_date,
    region,
    source_record_number
FROM ranked
WHERE row_num = 1;

-- -----------------------------------------------------------------------------
-- Transactions, step 1: standardise and deduplicate on transaction_id.
--   * category: trim / collapse spaces
--   * amount: DOUBLE -> DECIMAL(18,2), the right type for money
--   * timestamp stays a TIMESTAMP; transaction_date and date_key are derived
--     for the Gold date dimension (date_key format: YYYYMMDD, e.g. 20261027)
--   * a repeated transaction_id is collapsed with the same ROW_NUMBER() rule
--     (the generator does not create these; this is a defensive check)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE standardised_transactions AS
WITH standardised AS (
    SELECT
        transaction_id,
        entity_id,
        regexp_replace(trim(category), '\s+', ' ', 'g')         AS category,
        CAST(round(transaction_amount, 2) AS DECIMAL(18, 2))    AS transaction_amount,
        currency,
        "timestamp",
        CAST("timestamp" AS DATE)                               AS transaction_date,
        CAST(strftime("timestamp", '%Y%m%d') AS INTEGER)        AS date_key,
        source_record_number
    FROM bronze.bronze_transactions
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY source_record_number
        ) AS row_num
    FROM standardised
)
SELECT * EXCLUDE (row_num)
FROM ranked
WHERE row_num = 1;

-- -----------------------------------------------------------------------------
-- Transactions, step 2: referential integrity.
-- A transaction is analytically usable only if its entity_id exists in
-- silver_entities. Orphans are NOT silently dropped: they are kept in a
-- quarantine table so the exclusion can be counted and audited.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE silver.silver_transactions AS
SELECT t.*
FROM standardised_transactions AS t
WHERE EXISTS (
    SELECT 1 FROM silver.silver_entities AS e WHERE e.entity_id = t.entity_id
);

CREATE OR REPLACE TABLE silver.quarantine_orphan_transactions AS
SELECT
    t.*,
    'entity_id not found in silver.silver_entities' AS quarantine_reason
FROM standardised_transactions AS t
WHERE NOT EXISTS (
    SELECT 1 FROM silver.silver_entities AS e WHERE e.entity_id = t.entity_id
);

DROP TABLE standardised_transactions;
