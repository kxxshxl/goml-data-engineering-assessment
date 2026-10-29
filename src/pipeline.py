"""End-to-end pipeline: raw files -> validation/DLQ -> Bronze -> Silver -> Gold -> analytics.

Usage (from the project root)::

    python -m src.pipeline                      # full run with default paths
    python -m src.pipeline --as-of 2026-10-15   # report on another "current" month
    python -m src.pipeline --analytics-only     # only re-run the SQL queries
    python -m src.pipeline --log-format json    # machine-readable logs on stderr

Exit codes: 0 = success, 1 = the run failed (the reason is logged).
"""

import argparse
import logging
import sys
import time
from datetime import date
from decimal import Decimal
from pathlib import Path

from src.config import (
    DEFAULT_DB_PATH,
    DLQ_DIR,
    ENTITIES_FILE_NAME,
    INVALID_ENTITIES_FILE_NAME,
    INVALID_TRANSACTIONS_FILE_NAME,
    RAW_DIR,
    TRANSACTIONS_FILE_NAME,
)
from src.contracts import EntityContract, TransactionContract
from src.ingest import (
    SourceFileError,
    read_entities_csv,
    read_transactions_json,
    validate_records,
    write_dlq,
)
from src.logging_setup import LOG_FORMATS, LOG_LEVELS, configure_logging
from src.warehouse import QueryResult, ReconciliationError, build_warehouse, run_analytics

# A fixed name rather than __name__, which is "__main__" under `python -m src.pipeline`.
logger = logging.getLogger("src.pipeline")


def run_pipeline(
    raw_dir: Path = RAW_DIR,
    dlq_dir: Path = DLQ_DIR,
    db_path: Path = DEFAULT_DB_PATH,
) -> dict:
    """Run extraction, validation, DLQ routing and the warehouse build.

    Returns a summary dict with the counts of every stage. Raises
    ``SourceFileError`` for unusable input files and ``ReconciliationError``
    if the built warehouse does not reconcile.
    """
    entities_path = raw_dir / ENTITIES_FILE_NAME
    transactions_path = raw_dir / TRANSACTIONS_FILE_NAME

    # Each step below logs its own counts (see src/ingest.py and src/warehouse.py).
    # 1. Extract: file-level problems stop the run before anything is changed.
    raw_entities = read_entities_csv(entities_path)
    raw_transactions = read_transactions_json(transactions_path)

    # 2. Validate every record against its contract.
    entities = validate_records(raw_entities, EntityContract, ENTITIES_FILE_NAME)
    transactions = validate_records(raw_transactions, TransactionContract, TRANSACTIONS_FILE_NAME)

    # 3. Dead Letter Queue: rejected records with their original payload and error.
    write_dlq(entities.dlq_entries, dlq_dir / INVALID_ENTITIES_FILE_NAME)
    write_dlq(transactions.dlq_entries, dlq_dir / INVALID_TRANSACTIONS_FILE_NAME)

    # 4. Bronze -> Silver -> Gold, verified, in a single transaction.
    raw_counts = {
        "raw_entities": entities.total,
        "raw_transactions": transactions.total,
        "dlq_entities": len(entities.dlq_entries),
        "dlq_transactions": len(transactions.dlq_entries),
    }
    logger.info("Building warehouse %s", db_path, extra={"event": "warehouse_build_started", "db_path": str(db_path)})
    metrics = build_warehouse(
        db_path,
        entities.valid_rows,
        transactions.valid_rows,
        raw_counts,
        entity_source=ENTITIES_FILE_NAME,
        transaction_source=TRANSACTIONS_FILE_NAME,
    )
    return {**raw_counts, **metrics}


def format_summary(summary: dict) -> str:
    s = summary
    duplicate_rows = s["bronze_entities"] - s["silver_entities"]
    lines = [
        "",
        "=== Pipeline summary ===",
        "Entities",
        f"  raw rows read ................. {s['raw_entities']}",
        f"  rejected to DLQ ............... {s['dlq_entities']}",
        f"  loaded to bronze .............. {s['bronze_entities']}",
        f"  duplicate rows removed ........ {duplicate_rows}"
        f" (exact duplicates: {s['bronze_exact_duplicate_entity_rows']},"
        f" conflicting ids: {s['conflicting_entity_ids']})",
        f"  silver / dim_entity ........... {s['silver_entities']} / {s['gold_dim_entity']}",
        "Transactions",
        f"  raw records read .............. {s['raw_transactions']}",
        f"  rejected to DLQ ............... {s['dlq_transactions']}",
        f"  loaded to bronze .............. {s['bronze_transactions']}",
        f"  duplicate transaction_ids ..... {s['bronze_transactions'] - s['bronze_distinct_transaction_ids']}",
        f"  orphans quarantined (silver) .. {s['silver_orphan_transactions']}",
        f"  silver / fact_transactions .... {s['silver_transactions']} / {s['gold_fact_transactions']}",
        f"dim_date rows ................... {s['gold_dim_date']}",
        "Integrity: unresolved entity keys = "
        f"{s['fact_unresolved_entity_keys']}, unresolved date keys = {s['fact_unresolved_date_keys']}",
    ]
    return "\n".join(lines)


def _format_value(value) -> str:
    if isinstance(value, (float, Decimal)):
        return f"{value:,.2f}"
    return str(value)


def format_query_result(result: QueryResult) -> str:
    """Render a query result as a plain-text table (ASCII only, safe for any console)."""
    cells = [[_format_value(v) for v in row] for row in result.rows]
    widths = [max([len(c)] + [len(row[i]) for row in cells]) for i, c in enumerate(result.columns)]
    header = " | ".join(c.ljust(w) for c, w in zip(result.columns, widths))
    rule = "-+-".join("-" * w for w in widths)
    body = [" | ".join(v.rjust(w) if _is_number(v) else v.ljust(w) for v, w in zip(row, widths))
            for row in cells]
    if not body:
        body = ["(no rows)"]
    return "\n".join([f"\n--- {result.name} ---", header, rule, *body])


def _is_number(text: str) -> bool:
    return text.replace(",", "").replace(".", "", 1).lstrip("-").isdigit()


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the local Bronze/Silver/Gold ELT pipeline.")
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR, help="folder with entities.csv / transactions.json")
    parser.add_argument("--dlq-dir", type=Path, default=DLQ_DIR, help="folder for the DLQ JSON files")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH, help="DuckDB database file")
    parser.add_argument("--as-of", type=date.fromisoformat, default=None,
                        help="reference date YYYY-MM-DD for the 'current month' query (default: today)")
    parser.add_argument("--analytics-only", action="store_true",
                        help="skip the build and only run sql/analytics.sql")
    parser.add_argument("--skip-analytics", action="store_true", help="build the warehouse only")
    parser.add_argument("--log-level", choices=LOG_LEVELS, default="INFO", help="minimum log level (default: INFO)")
    parser.add_argument("--log-format", choices=LOG_FORMATS, default="text",
                        help="text for people, json for log tools (default: text)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    configure_logging(args.log_level, args.log_format)
    mode = "analytics-only" if args.analytics_only else "build-only" if args.skip_analytics else "full"
    started = time.perf_counter()
    logger.info("Pipeline started (mode: %s)", mode, extra={"event": "pipeline_started", "mode": mode})
    try:
        if not args.analytics_only:
            summary = run_pipeline(args.raw_dir, args.dlq_dir, args.db_path)
            print(format_summary(summary))
        if not args.skip_analytics:
            if not args.db_path.is_file():
                raise SourceFileError(f"database not found: {args.db_path} (run the pipeline first)")
            as_of = args.as_of or date.today()
            print(f"\n=== Analytical queries (current month = {as_of:%B %Y}) ===")
            for result in run_analytics(args.db_path, as_of):
                print(format_query_result(result))
    except (SourceFileError, ReconciliationError) as exc:
        # Expected failure types: the message says what is wrong, no traceback needed.
        logger.error("Pipeline failed: %s", exc, extra=_failure_context(exc, started))
        return 1
    except Exception as exc:
        # Unexpected: log the full traceback so the cause is never hidden.
        logger.exception("Pipeline failed with an unexpected error", extra=_failure_context(exc, started))
        return 1
    seconds = round(time.perf_counter() - started, 3)
    logger.info("Pipeline finished successfully in %.2f s", seconds,
                extra={"event": "pipeline_finished", "mode": mode, "duration_seconds": seconds})
    return 0


def _failure_context(exc: Exception, started: float) -> dict:
    return {"event": "pipeline_failed", "error_type": type(exc).__name__,
            "duration_seconds": round(time.perf_counter() - started, 3)}


if __name__ == "__main__":
    sys.exit(main())
