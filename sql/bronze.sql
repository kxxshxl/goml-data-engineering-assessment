-- =============================================================================
-- BRONZE LAYER: contract-valid records exactly as received.
--
-- * Only rows that passed Pydantic validation are inserted (by src/warehouse.py).
-- * No business cleaning: names are not trimmed, regions keep their original
--   casing and exact duplicate rows are kept. That is Silver's job.
-- * Values are stored with proper types (UUID / DATE / TIMESTAMP / DOUBLE).
-- * Lineage columns record where each row came from.
--
-- The source files are full snapshots, so each run rebuilds Bronze from the
-- current files (full refresh). That makes reruns idempotent.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE OR REPLACE TABLE bronze.bronze_entities (
    entity_id             UUID      NOT NULL,
    first_name            VARCHAR   NOT NULL,
    last_name             VARCHAR   NOT NULL,
    contact_email         VARCHAR   NOT NULL,
    registration_date     DATE      NOT NULL,
    region                VARCHAR   NOT NULL,
    -- lineage
    source_file           VARCHAR   NOT NULL,
    source_record_number  INTEGER   NOT NULL,
    loaded_at             TIMESTAMP NOT NULL
);

CREATE OR REPLACE TABLE bronze.bronze_transactions (
    transaction_id        UUID      NOT NULL,
    entity_id             UUID      NOT NULL,
    category              VARCHAR   NOT NULL,
    transaction_amount    DOUBLE    NOT NULL,  -- as received; Silver converts to DECIMAL
    currency              VARCHAR   NOT NULL,
    "timestamp"           TIMESTAMP NOT NULL,  -- naive UTC (see src/contracts.py)
    -- lineage
    source_file           VARCHAR   NOT NULL,
    source_record_number  INTEGER   NOT NULL,
    loaded_at             TIMESTAMP NOT NULL
);
