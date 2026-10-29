# Requirements Checklist

Every requirement in *Data_Engineering_Case_Study_goML.docx*, mapped to the
implementation and to the test that verifies it. Status reflects the verified
run on 2026-10-29 (Python 3.11.9, DuckDB 1.5.6, Pydantic 2.14.1: `133 passed`).

## Mandatory

| # | Requirement (DOCX section) | Implementation | Verified by |
|---|---|---|---|
| 1 | Python ≥ 3.10, DuckDB ≥ 0.9, Pydantic ≥ 2, pytest ≥ 7 (§2) | `requirements.txt` (DuckDB ≥ 1.1 for `SET VARIABLE`) | clean venv install + full run |
| 2 | Mock data generator script (§4) | `src/generate_data.py` | `tests/test_generate_data.py` |
| 3 | Entity CSV with the 6 required fields (§4A) | `build_entities`, `ENTITY_FIELDS` | `test_at_least_5_exact_duplicates_per_100_entity_rows` |
| 4 | ≥ 5 exact duplicates per 100 entity rows (§4A) | `DUPLICATES_PER_100`, ceiling rule | `test_at_least_5_exact_duplicates_per_100_entity_rows`, `test_quality_minimums_hold_for_any_dataset_size` |
| 5 | ≥ 5 malformed emails per 100 entity rows (§4A) | `_malformed_email` (spec patterns) | `test_at_least_5_malformed_emails_per_100_entity_rows` |
| 6 | Duplicates and malformed emails in separate records (§4A) | duplicates copy clean rows; malformed rows are new entities | `test_duplicate_and_malformed_email_records_are_separate` |
| 7 | Transaction JSON with the 6 required fields (§4B) | `build_transactions` | `test_each_invalid_transaction_type_is_present` |
| 8 | Negative amounts, missing/null `entity_id`, invalid currency (§4B) | `build_transactions` | `test_each_invalid_transaction_type_is_present` |
| 9 | Read CSV and JSON with Python (§5) | `src/ingest.py` | `tests/test_ingest.py` |
| 10 | Validate every record before DuckDB (§5) | `validate_records` → `build_warehouse` | `test_invalid_records_never_reach_bronze` |
| 11 | Entity contract rules (§5) | `EntityContract` | `tests/test_contracts.py` (entity tests) |
| 12 | Transaction contract rules incl. USD/EUR/GBP (§5) | `TransactionContract` | `tests/test_contracts.py` (transaction tests) |
| 13 | Validation failure must not stop the pipeline (§5) | per-record `try/except ValidationError` | `test_one_bad_record_does_not_stop_the_rest` |
| 14 | DLQ records original payload and error (§5) | `_dlq_entry`, `write_dlq` | `test_dlq_entry_keeps_original_payload_and_error_details`, `test_invalid_records_go_to_dlq_files` |
| 15 | `data/raw/...`, `data/dlq/invalid_*.json` layout (§5) | `src/config.py` | files present in `data/` |
| 16 | DuckDB database `warehouse.duckdb` (§6) | `data/warehouse.duckdb`, `build_warehouse` | full run |
| 17 | `bronze_entities`, `bronze_transactions`, valid records only, source values preserved (§6) | `sql/bronze.sql`, `load_bronze` | `test_invalid_records_never_reach_bronze`, `test_bronze_keeps_source_values_unstandardised` |
| 18 | Silver: dedupe entities using `ROW_NUMBER()`, one row per `entity_id` (§6) | `sql/silver.sql` | `test_silver_has_one_row_per_entity_and_standard_regions`, `test_exact_duplicates_collapse_and_conflicts_keep_first_row` |
| 19 | Silver: standardise region/text (§6) | `sql/silver.sql` | `test_bronze_keeps_source_values_unstandardised` |
| 20 | Silver: typed timestamps, derived dates, valid entity references (§6) | `sql/silver.sql` | `test_orphan_transactions_are_quarantined_not_loaded`, `test_date_key_matches_transaction_date_and_calendar_is_continuous` |
| 21 | Gold: `dim_entity` (surrogate key), `dim_date`, `fact_transactions` (§7) | `sql/gold.sql` | `test_dim_date_attributes`, `test_every_fact_foreign_key_resolves` |
| 22 | Fact correctly references dimensions (§7) | PK/FK constraints + reconciliation | `test_every_fact_foreign_key_resolves`, `test_fact_table_rejects_unknown_dimension_key` |
| 23 | Query 1: total per category, current calendar month by timestamp (§8) | `sql/analytics.sql` `q1_...` (one row per category; `q1b_...` splits it by currency) | `test_q1_monthly_revenue_uses_half_open_current_month`, `test_q1_follows_the_as_of_month`, `test_q1b_splits_the_monthly_totals_by_currency` |
| 24 | Query 2: top 5 regions by average of entity-level averages (§8) | `sql/analytics.sql` `q2_...` | `test_q2_averages_entity_level_averages` |
| 25 | ≥ 3 pytest tests: negative amount, invalid email, missing entity_id (§9) | `tests/test_contracts.py` | `test_transaction_rejects_negative_amount`, `test_entity_rejects_invalid_email`, `test_transaction_rejects_missing_entity_id` |
| 26 | Tests do not depend on exact error messages (§12) | assertions on field names/types only | code review |
| 27 | Separation of config / ingestion / validation / transformation / tests (§10) | `src/config.py`, `ingest.py`, `contracts.py`, `sql/`, `tests/` | repository layout |
| 28 | README: setup, data generation, execution, testing, architecture, design decisions, cloud scaling (§13) | `README.md` | review |
| 29 | Cloud scaling documentation (§13.7) | `docs/cloud_scaling.md` | review |
| 30 | Deliverables 1–11 (§14) | see README "Repository structure" | ZIP contents check |
| 31 | Runs locally with no cloud infrastructure (§14, §16) | DuckDB file + Python only | full run |

## Recommended/additional tests from §9 (all implemented)

| Suggested test | Test |
|---|---|
| Invalid UUID | `test_entity_rejects_invalid_uuid`, `test_transaction_rejects_invalid_uuid` |
| Unsupported currency | `test_transaction_rejects_unsupported_currency` |
| Empty first name | `test_entity_rejects_blank_required_text` |
| Missing required transaction fields | `test_transaction_rejects_missing_required_field` |
| Invalid records written to the DLQ | `test_invalid_records_go_to_dlq_files`, `test_write_dlq_produces_valid_json_and_replaces_old_file` |

## Optional bonus items (§17)

| Bonus | Status |
|---|---|
| Additional data-quality checks | Done: reconciliation checks, conflicting-ID metric, orphan quarantine, generator self-check |
| Configurable input/output paths | Done: `--raw-dir`, `--dlq-dir`, `--db-path`, `--output-dir` |
| Structured logging | Done: standard `logging` with timestamps, levels and an `event` name plus counts on every stage; optional JSON lines (`--log-format json`); no record contents logged (`src/logging_setup.py`, `tests/test_logging.py`) |
| Incremental processing | Not implemented (full refresh); discussed in `docs/cloud_scaling.md` |
| Additional analytical queries | Done: Query 1b and Query 3 (currency-safe versions of Query 1 and Query 2) |
| More comprehensive unit tests | Done: 133 tests |
| Dockerization | Not implemented (outside the core scope) |
| Proposed cloud architecture diagram | Done: Mermaid diagram in `docs/cloud_architecture.md` (proposal only, not implemented) |
