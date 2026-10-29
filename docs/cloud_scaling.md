# Cloud Scaling Discussion

This project runs on one machine: Python reads two files, Pydantic validates
records one at a time, and DuckDB holds every layer in a single file. That is
the right design for thousands or millions of rows. This document explains
what would change if the organisation grew to processing **terabytes per day**,
and *why* each change becomes necessary. Nothing here is implemented; the
assessment explicitly asks for a conceptual discussion only.

A diagram of the proposed architecture is in
[`cloud_architecture.md`](cloud_architecture.md).

## 1. What breaks first at terabyte scale

| Local design | Limit at TB/day |
|---|---|
| Raw files on a local disk | One disk cannot hold the data durably, and many producers cannot write to it |
| One Python process validating row by row | Throughput of a single CPU core (~10^4–10^5 records/s) |
| Full refresh of every layer on each run | Re-reading all history every day becomes impossible |
| Single DuckDB file | Single-node memory/disk; one writer at a time; no shared access for many analysts |
| DLQ as a local JSON file | Cannot be monitored, queried or replayed by a team |

Each section below replaces one of these limits.

## 2. Cloud object storage as the landing zone

Raw CSV/JSON (or, better, events from APIs and streams) would land in object
storage: **Amazon S3, Azure Data Lake Storage Gen2 or Google Cloud Storage**.

Why: object storage is effectively unlimited, very durable (11 nines on S3),
cheap per terabyte, and readable by every processing engine in parallel.
Storage is decoupled from compute, so data can be kept for years while compute
is paid for only while jobs run.

Layout follows the same medallion idea as this project:

```
s3://company-lake/bronze/transactions/ingest_date=2026-10-27/part-0001.parquet
s3://company-lake/silver/transactions/transaction_date=2026-10-27/...
s3://company-lake/gold/fact_transactions/date_key=20261027/...
s3://company-lake/dlq/transactions/ingest_date=2026-10-27/...
```

## 3. Columnar files and a lakehouse table format

CSV and JSON would be converted to **Parquet** at the Bronze step (columnar,
compressed, typed, and fast to read only the columns a query needs).

On top of Parquet, a table format such as **Delta Lake, Apache Iceberg or
Apache Hudi** adds what DuckDB gives us locally:

* ACID transactions: the local pipeline builds everything inside one DuckDB
  transaction; a table format gives the same "all or nothing" guarantee on object storage
* `MERGE`/upsert, which is required for incremental Silver deduplication (section 6)
* schema enforcement and controlled schema evolution
* time travel, for audits and reprocessing ("what did Gold look like yesterday?")

This is the **lakehouse**: lake-scale storage with warehouse-style reliability.
Platforms: Databricks, AWS (S3 + Glue Catalog + Athena/EMR), Azure Synapse/Fabric,
Google BigLake/Dataproc.

## 4. Distributed processing

Validation and the Silver/Gold transformations would run on a distributed engine
such as **Apache Spark** (Databricks, EMR, Dataproc, Synapse), or on the
warehouse's own SQL engine (dbt on Snowflake/BigQuery).

Why: the work splits naturally across many machines.

* **Contract validation** is applied per record, so it parallelises perfectly:
  each executor validates its own partition. The Pydantic contracts in
  `src/contracts.py` can be reused inside a Spark `mapPartitions` step, or
  translated into a Spark schema plus SQL constraint expressions for speed.
  Data-quality frameworks (Great Expectations, Soda, Delta Live Tables
  expectations, dbt tests) are the usual production equivalents.
* **`ROW_NUMBER()` deduplication** becomes a shuffle by `entity_id`; Spark SQL
  supports exactly the same window function used in `sql/silver.sql`.
* **The star-schema joins** are large fact table + small dimension, which Spark
  runs as broadcast joins (the dimension is copied to every worker).

For true real-time sources, events would arrive through **Kafka, Kinesis or
Event Hubs** and be processed with Spark Structured Streaming or Flink. The
same Bronze/Silver/Gold layers apply.

## 5. Partitioning

At TB scale, how data is physically organised decides cost and speed.

* Bronze partitioned by **ingest date**, Silver/Gold facts by **transaction
  date** (`date_key`), so the "current month" query (Query 1) reads only ~30
  daily partitions instead of the whole history (*partition pruning*).
* Within partitions, cluster/sort by a frequently filtered column (e.g. `entity_key`
  or `region`). This is Z-ordering in Delta, clustering keys in Snowflake and
  clustered tables in BigQuery.
* Avoid over-partitioning (e.g. by `entity_id`): millions of tiny files are
  slower than fewer, larger ones (target roughly 128 MB–1 GB per file). Use
  compaction jobs to merge small files.

## 6. Incremental processing

The local pipeline rebuilds every layer from scratch on every run. That is
simple and idempotent, but at TB/day re-reading all history daily is
impossible. Instead:

* **Bronze is append-only**: each run adds only the new files, tagged with a
  `batch_id` / `ingest_date`. Already processed files are tracked by
  checkpoints (Spark Structured Streaming, Databricks Auto Loader, or a
  watermark table).
* **Silver uses `MERGE`**: new entity records are merged on `entity_id` with the
  same survivor rule as today (`ROW_NUMBER()` over new + existing rows), so
  only changed keys are rewritten.
* **Gold dimensions keep stable surrogate keys**: new entities get new keys
  and existing keys never change. If attribute history matters (e.g. an entity
  moves region), `dim_entity` becomes a **Slowly Changing Dimension Type 2**
  with `valid_from` / `valid_to` columns.
* **Gold facts are appended** or partition-overwritten only for the dates
  that received data. Late-arriving data is handled by re-processing just
  the affected date partitions.
* Orchestration (Airflow, Dagster, Databricks Workflows, Azure Data Factory)
  schedules runs, retries failures and backfills history.

## 7. Scalable analytical warehouse for Gold

The Gold star schema would be served from a warehouse built for many
concurrent BI users: **Snowflake, Google BigQuery, Amazon Redshift, Azure
Synapse/Fabric or Databricks SQL**.

Why: they separate storage from compute, scale compute up and down (or
serverless), run massively parallel queries, and offer access control, row-
level security and caching. The star schema is kept unchanged: it is
exactly the model these engines and BI tools (Power BI, Tableau, Looker) are
optimised for. The queries in `sql/analytics.sql` are standard SQL and would
run with only minor dialect changes.

DuckDB still has a place: analysts can query Parquet/Iceberg files in object
storage directly with DuckDB for ad-hoc work, without a cluster.

## 8. The DLQ at scale

The local DLQ file becomes a **DLQ table or topic** (a Delta/Iceberg table, or a
Kafka dead-letter topic) partitioned by date and source, storing the same
fields as today (original payload, error list, source, record number, time).
On top of it:

* dashboards and alerts on rejection rates (a spike usually means a producer changed its schema)
* a **replay** process: once a producer fixes its data or the contract is
  updated, rejected records are re-submitted through validation
* retention and access controls, because rejected records can contain personal data

## 9. Operational concerns that appear at scale

* **Schema registry / data contracts as code**: producers and consumers share
  versioned schemas (Confluent Schema Registry, or JSON Schema exported from
  the Pydantic models with `model_json_schema()`).
* **Observability**: row counts and reconciliation per run (this project already
  computes these in `src/warehouse.py`), freshness and volume monitors, lineage
  (OpenLineage, Unity Catalog, Purview).
* **Security and governance**: `contact_email` and names are personal data, so
  they need encryption at rest, column-level masking, least-privilege IAM and
  GDPR-style deletion support.
* **Currency conversion**: a production revenue model would add a daily
  exchange-rate dimension so all amounts can be reported in one currency.
* **Cost control**: partition pruning, auto-suspending compute and storage tiering.

## 10. Summary: local component and its cloud equivalent

| This project | At terabyte scale |
|---|---|
| `data/raw/*.csv/json` | Object storage (S3 / ADLS / GCS), Kafka for streams |
| Pydantic loop in Python | Distributed validation in Spark / DLT expectations / dbt tests |
| `data/dlq/*.json` | DLQ table or topic, with monitoring and replay |
| DuckDB `bronze`/`silver` schemas | Delta / Iceberg tables on object storage (lakehouse) |
| `ROW_NUMBER()` full-refresh dedup | Incremental `MERGE` with the same window logic |
| DuckDB `gold` star schema | Snowflake / BigQuery / Redshift / Databricks SQL |
| `python -m src.pipeline` | Orchestrated, scheduled, incremental jobs (Airflow, Dagster, Workflows) |
| Reconciliation checks in Python | Data-quality monitoring and alerting |
