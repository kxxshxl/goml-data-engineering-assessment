"""DuckDB warehouse operations: Bronze loading, SQL transformations, checks, analytics."""

import json
import logging
import re
import tempfile
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import duckdb

from src.config import ANALYTICS_SQL, BRONZE_SQL, GOLD_SQL, SILVER_SQL

logger = logging.getLogger(__name__)


class ReconciliationError(Exception):
    """A row-count or referential-integrity check failed after the transformations."""


@dataclass
class QueryResult:
    name: str
    columns: list[str]
    rows: list[tuple]


# DuckDB column types used when bulk-loading the staged Bronze rows.
_BRONZE_ENTITY_COLUMNS = {
    "entity_id": "UUID",
    "first_name": "VARCHAR",
    "last_name": "VARCHAR",
    "contact_email": "VARCHAR",
    "registration_date": "DATE",
    "region": "VARCHAR",
    "source_record_number": "INTEGER",
}
_BRONZE_TRANSACTION_COLUMNS = {
    "transaction_id": "UUID",
    "entity_id": "UUID",
    "category": "VARCHAR",
    "transaction_amount": "DOUBLE",
    "currency": "VARCHAR",
    "timestamp": "TIMESTAMP",
    "source_record_number": "INTEGER",
}


def run_sql_file(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    """Execute every statement in a .sql file."""
    con.execute(path.read_text(encoding="utf-8"))


def _bulk_insert(
    con: duckdb.DuckDBPyConnection,
    table: str,
    rows: list[dict],
    columns: dict[str, str],
    source_file: str,
    loaded_at: datetime,
    staging_dir: Path,
) -> None:
    """Load validated rows into a Bronze table.

    The rows are written to a newline-delimited JSON staging file and loaded
    with one ``read_json`` statement, which is far faster than inserting row by
    row (about 0.1 s instead of more than 10 s for 5,000 rows). Explicit column
    types mean DuckDB never has to guess a type.
    """
    if not rows:
        return
    staging_file = staging_dir / f"{table.replace('.', '_')}.ndjson"
    with staging_file.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps({name: row[name] for name in columns}) + "\n")

    column_spec = "{" + ", ".join(f"'{name}': '{sql_type}'" for name, sql_type in columns.items()) + "}"
    select_list = ", ".join(f'"{name}"' for name in columns if name != "source_record_number")
    con.execute(
        f"""
        INSERT INTO {table}
        SELECT {select_list}, ? AS source_file, source_record_number, ? AS loaded_at
        FROM read_json(?, format = 'newline_delimited', columns = {column_spec})
        """,
        [source_file, loaded_at, str(staging_file)],
    )


def load_bronze(
    con: duckdb.DuckDBPyConnection,
    entity_rows: list[dict],
    transaction_rows: list[dict],
    entity_source: str,
    transaction_source: str,
) -> None:
    """(Re)create the Bronze tables and load the contract-valid rows into them."""
    started = time.perf_counter()
    run_sql_file(con, BRONZE_SQL)
    loaded_at = datetime.now().replace(microsecond=0)
    with tempfile.TemporaryDirectory(prefix="bronze_staging_") as staging:
        staging_dir = Path(staging)
        _bulk_insert(con, "bronze.bronze_entities", entity_rows, _BRONZE_ENTITY_COLUMNS,
                     entity_source, loaded_at, staging_dir)
        _bulk_insert(con, "bronze.bronze_transactions", transaction_rows, _BRONZE_TRANSACTION_COLUMNS,
                     transaction_source, loaded_at, staging_dir)
    logger.info("Bronze loaded: %d entities, %d transactions", len(entity_rows), len(transaction_rows),
                extra={"event": "bronze_loaded", "bronze_entities": len(entity_rows),
                       "bronze_transactions": len(transaction_rows), "seconds": _elapsed(started)})


def _elapsed(started: float) -> float:
    return round(time.perf_counter() - started, 3)


def build_silver_and_gold(con: duckdb.DuckDBPyConnection) -> None:
    started = time.perf_counter()
    run_sql_file(con, SILVER_SQL)
    counts = {
        "silver_entities": _scalar(con, "SELECT count(*) FROM silver.silver_entities"),
        "silver_transactions": _scalar(con, "SELECT count(*) FROM silver.silver_transactions"),
        "quarantined_orphans": _scalar(con, "SELECT count(*) FROM silver.quarantine_orphan_transactions"),
    }
    logger.info("Silver built: %(silver_entities)d entities, %(silver_transactions)d transactions, "
                "%(quarantined_orphans)d orphan transactions quarantined", counts,
                extra={"event": "silver_built", **counts, "seconds": _elapsed(started)})

    started = time.perf_counter()
    run_sql_file(con, GOLD_SQL)
    counts = {
        "dim_entity": _scalar(con, "SELECT count(*) FROM gold.dim_entity"),
        "dim_date": _scalar(con, "SELECT count(*) FROM gold.dim_date"),
        "fact_transactions": _scalar(con, "SELECT count(*) FROM gold.fact_transactions"),
    }
    logger.info("Gold built: dim_entity %(dim_entity)d, dim_date %(dim_date)d, "
                "fact_transactions %(fact_transactions)d rows", counts,
                extra={"event": "gold_built", **counts, "seconds": _elapsed(started)})


def _scalar(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    return con.execute(sql).fetchone()[0]


def collect_metrics(con: duckdb.DuckDBPyConnection) -> dict:
    """Row counts and data-quality measurements for every layer."""
    return {
        "bronze_entities": _scalar(con, "SELECT count(*) FROM bronze.bronze_entities"),
        "bronze_distinct_entity_ids": _scalar(
            con, "SELECT count(DISTINCT entity_id) FROM bronze.bronze_entities"),
        # Rows that are an exact copy (every source column equal) of an earlier row.
        "bronze_exact_duplicate_entity_rows": _scalar(con, """
            SELECT count(*) FROM (
                SELECT ROW_NUMBER() OVER (
                    PARTITION BY entity_id, first_name, last_name, contact_email,
                                 registration_date, region
                    ORDER BY source_record_number) AS row_num
                FROM bronze.bronze_entities)
            WHERE row_num > 1"""),
        # Same entity_id but different attribute values: not exact duplicates.
        "conflicting_entity_ids": _scalar(con, """
            SELECT count(*) FROM (
                SELECT entity_id
                FROM (SELECT DISTINCT entity_id, first_name, last_name, contact_email,
                                      registration_date, region
                      FROM bronze.bronze_entities)
                GROUP BY entity_id
                HAVING count(*) > 1)"""),
        "silver_entities": _scalar(con, "SELECT count(*) FROM silver.silver_entities"),
        "silver_duplicate_entity_ids": _scalar(con, """
            SELECT count(*) FROM (
                SELECT entity_id FROM silver.silver_entities
                GROUP BY entity_id HAVING count(*) > 1)"""),
        "bronze_transactions": _scalar(con, "SELECT count(*) FROM bronze.bronze_transactions"),
        "bronze_distinct_transaction_ids": _scalar(
            con, "SELECT count(DISTINCT transaction_id) FROM bronze.bronze_transactions"),
        "silver_transactions": _scalar(con, "SELECT count(*) FROM silver.silver_transactions"),
        "silver_orphan_transactions": _scalar(
            con, "SELECT count(*) FROM silver.quarantine_orphan_transactions"),
        "silver_transactions_without_entity": _scalar(con, """
            SELECT count(*) FROM silver.silver_transactions AS t
            WHERE NOT EXISTS (SELECT 1 FROM silver.silver_entities AS e
                              WHERE e.entity_id = t.entity_id)"""),
        "gold_dim_entity": _scalar(con, "SELECT count(*) FROM gold.dim_entity"),
        "gold_dim_date": _scalar(con, "SELECT count(*) FROM gold.dim_date"),
        "gold_fact_transactions": _scalar(con, "SELECT count(*) FROM gold.fact_transactions"),
        "fact_unresolved_entity_keys": _scalar(con, """
            SELECT count(*) FROM gold.fact_transactions AS f
            LEFT JOIN gold.dim_entity AS e ON e.entity_key = f.entity_key
            WHERE e.entity_key IS NULL"""),
        "fact_unresolved_date_keys": _scalar(con, """
            SELECT count(*) FROM gold.fact_transactions AS f
            LEFT JOIN gold.dim_date AS d ON d.date_key = f.date_key
            WHERE d.date_key IS NULL"""),
        "fact_date_key_mismatches": _scalar(con, """
            SELECT count(*) FROM gold.fact_transactions AS f
            JOIN gold.dim_date AS d ON d.date_key = f.date_key
            WHERE d.full_date <> CAST(f."timestamp" AS DATE)"""),
    }


def reconcile(metrics: dict, raw_counts: dict) -> list[str]:
    """Return a list of failed checks (an empty list means everything reconciles).

    ``raw_counts`` holds the Python-side numbers: records read from each file
    and records sent to each DLQ.
    """
    m = metrics
    checks = {
        "raw entities = bronze + DLQ":
            raw_counts["raw_entities"] == m["bronze_entities"] + raw_counts["dlq_entities"],
        "raw transactions = bronze + DLQ":
            raw_counts["raw_transactions"] == m["bronze_transactions"] + raw_counts["dlq_transactions"],
        "silver entities = distinct bronze entity_ids":
            m["silver_entities"] == m["bronze_distinct_entity_ids"],
        "silver has no duplicate entity_id":
            m["silver_duplicate_entity_ids"] == 0,
        "distinct bronze transactions = silver + quarantined orphans":
            m["bronze_distinct_transaction_ids"] == m["silver_transactions"] + m["silver_orphan_transactions"],
        "every silver transaction references a silver entity":
            m["silver_transactions_without_entity"] == 0,
        "dim_entity rows = silver entities":
            m["gold_dim_entity"] == m["silver_entities"],
        "fact rows = silver transactions":
            m["gold_fact_transactions"] == m["silver_transactions"],
        "every fact entity_key resolves":
            m["fact_unresolved_entity_keys"] == 0,
        "every fact date_key resolves":
            m["fact_unresolved_date_keys"] == 0,
        "fact date_key matches the timestamp's date":
            m["fact_date_key_mismatches"] == 0,
    }
    return [name for name, passed in checks.items() if not passed]


def build_warehouse(
    db_path: Path,
    entity_rows: list[dict],
    transaction_rows: list[dict],
    raw_counts: dict,
    entity_source: str,
    transaction_source: str,
) -> dict:
    """Load Bronze, build Silver and Gold, and verify the result, all in ONE transaction.

    If anything fails (SQL error or a failed reconciliation check) the
    transaction is rolled back, so the database keeps its previous, consistent
    state instead of a half-built one.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        con.begin()
        try:
            load_bronze(con, entity_rows, transaction_rows, entity_source, transaction_source)
            build_silver_and_gold(con)
            metrics = collect_metrics(con)
            failures = reconcile(metrics, raw_counts)
            if failures:
                logger.error("Reconciliation failed: %s", "; ".join(failures),
                             extra={"event": "reconciliation_failed", "failed_checks": failures})
                raise ReconciliationError("reconciliation failed: " + "; ".join(failures))
            logger.info("Reconciliation passed: all checks succeeded",
                        extra={"event": "reconciliation_passed", "failed_checks": []})
            con.commit()
        except Exception:
            con.rollback()
            # The error itself is logged by the caller; this records the outcome.
            logger.warning("Warehouse transaction rolled back; the previous warehouse is unchanged",
                           extra={"event": "warehouse_rolled_back", "db_path": str(db_path)})
            raise
        con.execute("CHECKPOINT")  # write everything into the .duckdb file itself
        logger.info("Warehouse committed to %s", db_path,
                    extra={"event": "warehouse_committed", "db_path": str(db_path)})
        return metrics
    finally:
        con.close()


def _split_named_queries(sql_text: str) -> tuple[str, list[tuple[str, str]]]:
    """Split analytics.sql into (preamble, [(query_name, query_sql), ...])."""
    parts = re.split(r"^--\s*name:\s*(\S+)\s*$", sql_text, flags=re.MULTILINE)
    preamble, rest = parts[0], parts[1:]
    queries = [(rest[i], rest[i + 1]) for i in range(0, len(rest), 2)]
    return preamble, queries


def run_analytics(db_path: Path, as_of: date | None = None, sql_path: Path = ANALYTICS_SQL) -> list[QueryResult]:
    """Run every named query in analytics.sql against the Gold layer.

    ``as_of`` chooses the "current" month for Query 1 (default: today).
    """
    preamble, queries = _split_named_queries(sql_path.read_text(encoding="utf-8"))
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if as_of is not None:
            # as_of is a datetime.date, so isoformat() can only produce YYYY-MM-DD.
            con.execute(f"SET VARIABLE as_of_date = DATE '{as_of.isoformat()}'")
        con.execute(preamble)
        results = []
        for name, query in queries:
            started = time.perf_counter()
            cursor = con.execute(query)
            columns = [description[0] for description in cursor.description]
            results.append(QueryResult(name=name, columns=columns, rows=cursor.fetchall()))
            logger.info("Query %s returned %d rows", name, len(results[-1].rows),
                        extra={"event": "query_completed", "query": name, "rows": len(results[-1].rows),
                               "seconds": _elapsed(started)})
        return results
    finally:
        con.close()
