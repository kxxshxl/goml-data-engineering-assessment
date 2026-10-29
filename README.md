# GoML Data Engineering Case Study

This project is a small, local data pipeline built with Python, Pydantic, and DuckDB. It takes mock entity and transaction data, checks it for common data-quality problems, and prepares it for analysis.

The pipeline has three layers:

- **Bronze** keeps records that pass validation, close to how they arrived.
- **Silver** cleans up the data, removes duplicate entities, and sets aside transactions that refer to entities that do not exist.
- **Gold** organises the cleaned data into a star schema for reporting.

Invalid records are saved separately in a dead-letter queue (DLQ), so they can be inspected instead of silently discarded.

Everything runs locally. You do not need a cloud account, a database server, Docker, or other infrastructure.

## What you need

- Python 3.10 or newer
- A terminal (the examples below use Windows PowerShell)

The main dependencies are DuckDB, Pydantic, `email-validator`, and pytest. They are listed in `requirements.txt`.

## Set up the project

Open a terminal in the folder containing this README and run:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

These commands use the virtual environment's Python directly, so you do not need to activate it first. If you prefer to activate the environment in PowerShell, run `.\.venv\Scripts\Activate.ps1` and then use `python` in the commands below.

On macOS or Linux, create the environment with `python3 -m venv .venv` and use `.venv/bin/python` instead of `.venv\Scripts\python.exe`.

## Generate the sample data

Run:

```powershell
.\.venv\Scripts\python.exe -m src.generate_data
```

This creates the input files in `data/raw/`:

- `entities.csv`
- `transactions.json`
- `generation_manifest.json`, which records the generated test cases

The default dataset contains 500 entity rows and 5,000 valid transactions, along with deliberate data-quality issues. These include duplicate entity rows, malformed email addresses, negative transaction amounts, missing or null entity IDs, unsupported currencies, and transactions that refer to unknown entities.

The generator uses a fixed seed by default, so the same settings produce the same data. You can change the size and generation date, for example:

```powershell
.\.venv\Scripts\python.exe -m src.generate_data --entities 1000 --transactions 10000 --invalid-per-type 30 --orphans 15 --seed 7 --as-of 2026-10-27
```

To see all available options:

```powershell
.\.venv\Scripts\python.exe -m src.generate_data --help
```

## Run the pipeline

Run:

```powershell
.\.venv\Scripts\python.exe -m src.pipeline
```

The pipeline validates the input records, sends records that fail validation to the DLQ, and builds the DuckDB warehouse at `data/warehouse.duckdb`. It then runs consistency checks and prints the analytical query results.

The main output files are:

- `data/dlq/invalid_entities.json`
- `data/dlq/invalid_transactions.json`
- `data/warehouse.duckdb`

Each DLQ entry includes the original record and details explaining why it was rejected. A transaction with a valid structure but an unknown entity ID is handled separately in the Silver layer, because that problem can only be found by checking it against the entity data.

Useful options:

```powershell
# Choose which month Query 1 should report
.\.venv\Scripts\python.exe -m src.pipeline --as-of 2026-10-15

# Run the analytical queries against the existing warehouse only
.\.venv\Scripts\python.exe -m src.pipeline --analytics-only

# Build the warehouse without running the analytical queries
.\.venv\Scripts\python.exe -m src.pipeline --skip-analytics
```

The pipeline performs a full refresh. Running it again with the same input data should produce the same results.

### Logs

While it runs, the pipeline writes log messages to stderr with a timestamp and level, for example:

```text
2026-10-29 01:31:22,191 WARNING src.ingest: Validated entities.csv: 475 valid, 25 rejected
2026-10-29 01:31:22,292 INFO src.warehouse: Silver built: 450 entities, 5000 transactions, 10 orphan transactions quarantined
2026-10-29 01:31:22,321 INFO src.warehouse: Reconciliation passed: all checks succeeded
```

The logs cover the start and end of the run, file reading, validation counts, DLQ files, each Bronze/Silver/Gold step, the consistency checks, each analytical query, and any failure. Rejected records are logged as warnings because the run continues; failures are logged as errors, and unexpected errors include the full traceback. The logs contain counts, file names, and timings only. They never contain record contents, because the entity data includes names and email addresses.

For machine-readable logs, use JSON output, which writes one JSON object per line with fields such as `event`, `source_file`, and record counts:

```powershell
.\.venv\Scripts\python.exe -m src.pipeline --log-format json
.\.venv\Scripts\python.exe -m src.pipeline --log-level WARNING
```

The pipeline summary and query results are still printed to stdout, so they stay separate from the logs.

### Example run

With the default seed and sample data generated on 29 October 2026, the pipeline reported:

- 500 entity rows read; 25 rejected; 475 loaded into Bronze; 25 duplicates removed in Silver, leaving 450 entities.
- 5,070 transaction records read; 60 rejected; 5,010 loaded into Bronze.
- 10 transactions with unknown entity IDs set aside; 5,000 transactions remained in Silver and Gold.
- No unresolved entity or date keys in the Gold fact table.

Your numbers may differ if you generate a different dataset or use different command-line options.

**About the current month:** Query 1 uses the month containing the run date by default. The included sample data was generated on 29 October 2026. If you run the query in a later month, regenerate the sample data or pass `--as-of 2026-10-29` to use October 2026 for the query.

## Run the tests

Run the test suite with:

```powershell
.\.venv\Scripts\python.exe -m pytest
```

The verified test run completed with **133 tests passing**. The tests cover the data contracts, generated test cases, DLQ handling, pipeline behaviour, deduplication, referential integrity, the analytical queries, and logging.

You can run a particular test file or search for tests by name:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_contracts.py -v
.\.venv\Scripts\python.exe -m pytest -k negative
```

## How the code is organised

```text
goml_data_engineering/
├── README.md
├── requirements.txt
├── pytest.ini
├── src/
│   ├── config.py
│   ├── contracts.py
│   ├── generate_data.py
│   ├── ingest.py
│   ├── warehouse.py
│   ├── logging_setup.py
│   └── pipeline.py
├── sql/
│   ├── bronze.sql
│   ├── silver.sql
│   ├── gold.sql
│   └── analytics.sql
├── tests/
├── data/
│   ├── raw/
│   ├── dlq/
│   └── warehouse.duckdb
└── docs/
    ├── cloud_scaling.md
    ├── cloud_architecture.md
    └── requirements_checklist.md
```

- `src/generate_data.py` creates the sample data.
- `src/contracts.py` defines the Pydantic validation rules.
- `src/ingest.py` reads the input files, validates each record, and writes rejected records to the DLQ.
- `src/warehouse.py` loads Bronze and coordinates the SQL transformations and consistency checks.
- `src/pipeline.py` runs the steps in order.
- `src/logging_setup.py` configures the text and JSON log formats.
- The SQL files define the Bronze tables, Silver transformations, Gold star schema, and analytical queries.

## How the pipeline works

```text
Raw files            data/raw/entities.csv, data/raw/transactions.json
    │
    ▼
Ingestion            src/ingest.py reads the CSV and JSON files
    │
    ▼
Validation           src/contracts.py checks each record against its Pydantic contract
    │
    ├── invalid ───► DLQ: data/dlq/invalid_entities.json, data/dlq/invalid_transactions.json
    │
    ▼ valid
Bronze               bronze.bronze_entities, bronze.bronze_transactions   (sql/bronze.sql)
    │
    ▼
Silver               deduplicated entities, typed transactions             (sql/silver.sql)
    │                unknown entity IDs ───► silver.quarantine_orphan_transactions
    ▼
Gold                 gold.dim_entity, gold.dim_date, gold.fact_transactions (sql/gold.sql)
    │
    ▼
Consistency checks   11 checks; commit if they all pass, roll back if any fail
    │
    ▼
Analytical SQL       sql/analytics.sql, results printed to the terminal
```

The input files are read and validated first. If an input file is missing or cannot be parsed, the run stops before anything is written. Rejected records are then written to the DLQ, and the valid records go into DuckDB.

Loading Bronze, building Silver and Gold, and running the consistency checks all happen in one DuckDB transaction. If a SQL step fails or a check does not pass, the whole transaction is rolled back. `data/warehouse.duckdb` then keeps its previous contents instead of being left half-built. The checks confirm, for example, that every raw record ended up either in Bronze or in the DLQ, and that Silver has one row per entity. They also confirm that every key in the fact table points to a real dimension row.

## Data layers

| Layer | What it contains |
|---|---|
| Bronze | Records that pass validation, with their original values and source information |
| Silver | Cleaned and deduplicated entities, typed transactions, and a separate table for transactions with unknown entity IDs |
| Gold | `dim_entity`, `dim_date`, and `fact_transactions`, ready for analysis |

Entities are deduplicated with `ROW_NUMBER()`. If duplicate IDs have conflicting values, the first occurrence in the source file is retained and the pipeline reports the conflict. Regions are trimmed and converted to uppercase, names are trimmed, and emails are normalised in Silver.

The Gold fact table has one row per transaction. It links to the entity and date dimensions through keys, and the pipeline checks that those links are valid before committing the warehouse.

## Analytical queries

The SQL file `sql/analytics.sql` includes the two main queries from the case study:

1. **Monthly transaction total by category.** Uses the current calendar month by default and returns one row per category.
2. **Top five regions by average transaction amount per entity.** First calculates each entity's average transaction amount, then averages those entity averages by region. This means an entity with many transactions does not automatically outweigh an entity with fewer transactions.

There are also companion queries that break the results down by currency. The main queries follow the case study's requested outputs, but the source data has no exchange rates. As a result, the main totals treat amounts as nominal values; they should not be interpreted as currency-converted financial totals. In a production system, the data model would need an agreed reporting currency and exchange rates.

## Design decisions

### Why DuckDB

- **It needs no server.** DuckDB stores the whole warehouse in a single file, so the project runs on any machine after a `pip install`.
- **It is built for analytical queries.** It stores data by column, and its SQL supports window functions such as `ROW_NUMBER()`, which the Silver deduplication uses.
- **It supports transactions and constraints.** The whole build can run as one transaction, and the Gold tables can declare primary and foreign keys.
- **The SQL carries over.** The transformations are standard SQL in `sql/`, so they could move to a cloud warehouse with only minor changes.

### Why Bronze, Silver and Gold layers

- **Each layer has one job.** Bronze keeps the validated records close to how they arrived. Silver cleans and deduplicates them. Gold shapes them for reporting.
- **Problems can be traced.** Because Bronze keeps the original values, any number in Gold can be traced back to the source record it came from.
- **Each layer can be checked against the one before it.** For example, Silver entities must equal the distinct entity IDs in Bronze. The consistency checks are built on these comparisons.
- **Rules change in one place.** If a cleaning rule changes, only the Silver SQL needs updating; Bronze and the contracts stay the same.

### Why a star schema in Gold

- **The questions are about measures by descriptive attributes**, such as amounts by category, month or region. A star schema fits that directly: one fact table of transactions, plus dimension tables for entities and dates.
- **Queries stay simple.** Query 1 reads only the fact table, and Query 2 needs a single join from the fact table to `dim_entity` on an integer key.
- **Attributes are stored once.** An entity's region lives in `dim_entity` rather than being repeated on every transaction. `dim_date` holds calendar attributes (year, quarter, month) for reporting by period.
- **BI tools expect it.** Tools such as Power BI and Tableau, and cloud warehouses, are designed around this model.

### Other choices

- **Pydantic validates records before Bronze.** A bad record is sent to the DLQ without stopping processing of the remaining records.
- **File-level problems stop the run.** For example, a missing input file or malformed JSON should not be reported as a successful run with zero records.
- **Orphan transactions are kept separate from the DLQ.** They pass the individual record contract, but their entity ID cannot be found in the entity dataset.
- **The pipeline uses a full refresh.** This keeps the example straightforward and makes reruns reproducible. A production pipeline would likely process new or changed data incrementally.

## Scaling this beyond a local project

The assessment only requires a local implementation. The possible next steps for a larger system are described in [`docs/cloud_scaling.md`](docs/cloud_scaling.md). In short, a production version could use cloud object storage for raw data, incremental processing, orchestration and monitoring, and a cloud analytical warehouse. Those services are not part of this project.

A diagram of one possible cloud design is in [`docs/cloud_architecture.md`](docs/cloud_architecture.md). It is a proposal only; this project remains a local Python and DuckDB implementation.

## Further documentation

- [`docs/requirements_checklist.md`](docs/requirements_checklist.md) maps the assessment requirements to the implementation and tests.
- [`docs/cloud_scaling.md`](docs/cloud_scaling.md) discusses how the design could be adapted for larger data volumes.
- [`docs/cloud_architecture.md`](docs/cloud_architecture.md) shows a proposed cloud architecture diagram (not implemented).
