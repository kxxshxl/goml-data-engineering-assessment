"""Integration tests: the full pipeline on small, isolated datasets.

Each test builds its own DuckDB file under tmp_path; the real warehouse is never touched.
"""

import json
from datetime import date
from decimal import Decimal

import duckdb
import pytest

from conftest import write_sources
from src import pipeline as pipeline_module
from src import warehouse
from src.generate_data import generate
from src.ingest import SourceFileError
from src.pipeline import run_pipeline
from src.warehouse import ReconciliationError, run_analytics

AS_OF = date(2026, 10, 15)


def _uuid(n: int) -> str:
    return f"00000000-0000-4000-8000-{n:012d}"


def _entity(n: int, region: str, **overrides) -> dict:
    return {
        "entity_id": _uuid(n),
        "first_name": f"First{n}",
        "last_name": f"Last{n}",
        "contact_email": f"person{n}@example.com",
        "registration_date": "2025-01-01",
        "region": region,
        **overrides,
    }


def _txn(n: int, entity: int, amount: float, timestamp: str, currency="USD", category="Retail") -> dict:
    return {
        "transaction_id": _uuid(1000 + n),
        "entity_id": _uuid(entity),
        "category": category,
        "transaction_amount": amount,
        "currency": currency,
        "timestamp": timestamp,
    }


def _query(db_path, sql):
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


@pytest.fixture
def generated_run(workspace):
    """A pipeline run over a small generated dataset."""
    manifest = generate(workspace["raw_dir"], entity_rows=200, valid_transactions=500,
                        invalid_per_type=5, orphan_transactions=4, seed=11, as_of=AS_OF)
    summary = run_pipeline(workspace["raw_dir"], workspace["dlq_dir"], workspace["db_path"])
    return workspace, manifest, summary


# --------------------------------------------------------------------------
# DLQ and Bronze
# --------------------------------------------------------------------------

def test_invalid_records_go_to_dlq_files(generated_run):
    ws, manifest, summary = generated_run
    invalid_entities = json.loads((ws["dlq_dir"] / "invalid_entities.json").read_text(encoding="utf-8"))
    invalid_txns = json.loads((ws["dlq_dir"] / "invalid_transactions.json").read_text(encoding="utf-8"))

    assert {e["record"]["entity_id"] for e in invalid_entities} == \
        set(manifest["entities"]["malformed_email_entity_ids"])
    expected_txn_ids = set(manifest["transactions"]["negative_amount_ids"]
                           + manifest["transactions"]["missing_or_null_entity_id_ids"]
                           + manifest["transactions"]["unsupported_currency_ids"])
    assert {e["record"]["transaction_id"] for e in invalid_txns} == expected_txn_ids
    assert all(e["errors"] for e in invalid_entities + invalid_txns)


def test_invalid_records_never_reach_bronze(generated_run):
    ws, manifest, _ = generated_run
    bad_entity_ids = manifest["entities"]["malformed_email_entity_ids"]
    bad_txn_ids = (manifest["transactions"]["negative_amount_ids"]
                   + manifest["transactions"]["missing_or_null_entity_id_ids"]
                   + manifest["transactions"]["unsupported_currency_ids"])
    in_list = lambda ids: ", ".join(f"'{i}'" for i in ids)

    assert _query(ws["db_path"], f"SELECT count(*) FROM bronze.bronze_entities "
                                 f"WHERE CAST(entity_id AS VARCHAR) IN ({in_list(bad_entity_ids)})")[0][0] == 0
    assert _query(ws["db_path"], f"SELECT count(*) FROM bronze.bronze_transactions "
                                 f"WHERE CAST(transaction_id AS VARCHAR) IN ({in_list(bad_txn_ids)})")[0][0] == 0
    assert _query(ws["db_path"], "SELECT count(*) FROM bronze.bronze_transactions "
                                 "WHERE transaction_amount <= 0 OR currency NOT IN ('USD','EUR','GBP')")[0][0] == 0


def test_bronze_keeps_source_values_unstandardised(workspace):
    write_sources(workspace["raw_dir"], [_entity(1, " north ", first_name="  Ann")],
                  [_txn(1, 1, 10.0, "2026-10-02T10:00:00")])
    run_pipeline(workspace["raw_dir"], workspace["dlq_dir"], workspace["db_path"])

    assert _query(workspace["db_path"], "SELECT first_name, region FROM bronze.bronze_entities") == \
        [("  Ann", " north ")]
    assert _query(workspace["db_path"], "SELECT first_name, region FROM silver.silver_entities") == \
        [("Ann", "NORTH")]


def test_row_counts_reconcile_across_layers(generated_run):
    _, manifest, s = generated_run
    ent, txn = manifest["entities"], manifest["transactions"]
    assert s["raw_entities"] == ent["total_rows"]
    assert s["dlq_entities"] == ent["malformed_email_rows"]
    assert s["bronze_entities"] == ent["total_rows"] - ent["malformed_email_rows"]
    assert s["silver_entities"] == ent["unique_valid_entities"]
    assert s["dlq_transactions"] == (txn["negative_amount"] + txn["missing_or_null_entity_id"]
                                     + txn["unsupported_currency"])
    assert s["silver_orphan_transactions"] == txn["orphan_entity_reference"]
    assert s["gold_fact_transactions"] == txn["valid_transactions"]


# --------------------------------------------------------------------------
# Silver
# --------------------------------------------------------------------------

def test_silver_has_one_row_per_entity_and_standard_regions(generated_run):
    ws, _, _ = generated_run
    assert _query(ws["db_path"], "SELECT count(*) - count(DISTINCT entity_id) FROM silver.silver_entities")[0][0] == 0
    regions = {r[0] for r in _query(ws["db_path"], "SELECT DISTINCT region FROM silver.silver_entities")}
    assert regions == {"NORTH", "SOUTH", "EAST", "WEST", "CENTRAL", "NORTHEAST", "SOUTHWEST"}


def test_exact_duplicates_collapse_and_conflicts_keep_first_row(workspace):
    entities = [
        _entity(1, "north"),
        _entity(1, "north"),                                       # exact duplicate
        _entity(2, "South", contact_email="First.Two@Example.com"),
        _entity(2, "EAST", contact_email="other@example.com"),     # same id, conflicting values
    ]
    write_sources(workspace["raw_dir"], entities, [_txn(1, 1, 10.0, "2026-10-02T10:00:00")])
    summary = run_pipeline(workspace["raw_dir"], workspace["dlq_dir"], workspace["db_path"])

    assert summary["bronze_entities"] == 4
    assert summary["silver_entities"] == 2
    assert summary["bronze_exact_duplicate_entity_rows"] == 1
    assert summary["conflicting_entity_ids"] == 1
    survivor = _query(workspace["db_path"],
                      f"SELECT region, contact_email FROM silver.silver_entities WHERE entity_id = '{_uuid(2)}'")
    assert survivor == [("SOUTH", "first.two@example.com")]  # first row in the file wins, standardised


def test_orphan_transactions_are_quarantined_not_loaded(workspace):
    write_sources(workspace["raw_dir"], [_entity(1, "north")], [
        _txn(1, 1, 10.0, "2026-10-02T10:00:00"),
        _txn(2, 99, 20.0, "2026-10-02T11:00:00"),  # entity 99 does not exist
    ])
    summary = run_pipeline(workspace["raw_dir"], workspace["dlq_dir"], workspace["db_path"])

    assert summary["silver_orphan_transactions"] == 1
    assert summary["gold_fact_transactions"] == 1
    assert _query(workspace["db_path"], "SELECT CAST(entity_id AS VARCHAR) "
                                        "FROM silver.quarantine_orphan_transactions") == [(_uuid(99),)]


def test_transactions_of_dlq_entities_become_orphans(workspace):
    """An entity rejected for a bad email cannot be referenced by the fact table."""
    write_sources(workspace["raw_dir"],
                  [_entity(1, "north"), _entity(2, "south", contact_email="broken@")],
                  [_txn(1, 1, 10.0, "2026-10-02T10:00:00"), _txn(2, 2, 20.0, "2026-10-02T10:00:00")])
    summary = run_pipeline(workspace["raw_dir"], workspace["dlq_dir"], workspace["db_path"])
    assert summary["dlq_entities"] == 1
    assert summary["silver_orphan_transactions"] == 1
    assert summary["gold_fact_transactions"] == 1


# --------------------------------------------------------------------------
# Gold star schema
# --------------------------------------------------------------------------

def test_every_fact_foreign_key_resolves(generated_run):
    ws, _, _ = generated_run
    unresolved = _query(ws["db_path"], """
        SELECT count(*) FROM gold.fact_transactions f
        LEFT JOIN gold.dim_entity e ON e.entity_key = f.entity_key
        LEFT JOIN gold.dim_date d ON d.date_key = f.date_key
        WHERE e.entity_key IS NULL OR d.date_key IS NULL""")[0][0]
    assert unresolved == 0


def test_date_key_matches_transaction_date_and_calendar_is_continuous(generated_run):
    ws, _, _ = generated_run
    mismatches = _query(ws["db_path"], """
        SELECT count(*) FROM gold.fact_transactions f
        JOIN gold.dim_date d ON d.date_key = f.date_key
        WHERE d.full_date <> CAST(f."timestamp" AS DATE)
           OR f.date_key <> year(d.full_date) * 10000 + month(d.full_date) * 100 + day(d.full_date)""")[0][0]
    assert mismatches == 0
    rows, first, last = _query(ws["db_path"], "SELECT count(*), min(full_date), max(full_date) FROM gold.dim_date")[0]
    assert rows == (last - first).days + 1


def test_dim_date_attributes(workspace):
    write_sources(workspace["raw_dir"], [_entity(1, "north")], [_txn(1, 1, 10.0, "2026-10-27T23:59:59")])
    run_pipeline(workspace["raw_dir"], workspace["dlq_dir"], workspace["db_path"])
    assert _query(workspace["db_path"], "SELECT * FROM gold.dim_date") == \
        [(20261027, date(2026, 10, 27), 2026, 4, 10, "October", 27)]


def test_fact_table_rejects_unknown_dimension_key(generated_run):
    """The declared FOREIGN KEY constraint is enforced by DuckDB itself."""
    ws, _, _ = generated_run
    con = duckdb.connect(str(ws["db_path"]))
    try:
        with pytest.raises(duckdb.ConstraintException):
            con.execute("""INSERT INTO gold.fact_transactions VALUES
                (uuid(), -999, 20261001, 'Retail', 1.00, 'USD', TIMESTAMP '2026-10-01 10:00:00')""")
    finally:
        con.close()


# --------------------------------------------------------------------------
# Analytical queries (hand-computed expected results)
# --------------------------------------------------------------------------

@pytest.fixture
def analytics_db(workspace):
    entities = [
        _entity(1, "north"), _entity(2, "North "),   # both NORTH after standardisation
        _entity(3, "south"), _entity(4, "east"), _entity(5, "west"),
        _entity(6, "central"), _entity(7, "southwest"),
    ]
    transactions = [
        _txn(1, 1, 10.00, "2026-10-01T00:00:00"),                  # IN  (first second of month)
        _txn(2, 1, 30.00, "2026-10-31T23:59:59"),                  # IN  (last second of month)
        _txn(3, 2, 100.00, "2026-09-30T23:59:59"),                 # OUT (previous month)
        _txn(4, 3, 50.00, "2026-10-10T12:00:00", currency="EUR"),  # IN
        _txn(5, 4, 70.00, "2026-11-01T00:00:00", category="Travel"),          # OUT (next month)
        _txn(6, 5, 40.00, "2026-10-20T08:00:00", currency="GBP", category="Travel"),  # IN
        _txn(7, 6, 30.00, "2025-10-15T08:00:00", category="Travel"),          # OUT (same month last year)
        _txn(8, 7, 20.00, "2026-08-01T08:00:00", category="Groceries"),       # OUT
    ]
    write_sources(workspace["raw_dir"], entities, transactions)
    run_pipeline(workspace["raw_dir"], workspace["dlq_dir"], workspace["db_path"])
    return {r.name: r for r in run_analytics(workspace["db_path"], as_of=AS_OF)}


def test_q1_monthly_revenue_uses_half_open_current_month(analytics_db):
    q1 = analytics_db["q1_monthly_revenue_by_category"]
    assert q1.columns[:2] == ["category", "total_transaction_amount"]
    # One row per category, highest total first.
    # Retail = 10 + 30 + 50; the September 100 is excluded.
    # Travel = 40; Nov 1st 00:00 and Oct 2025 are excluded.
    assert [(r[0], r[1]) for r in q1.rows] == [
        ("Retail", Decimal("90.00")),
        ("Travel", Decimal("40.00")),
    ]


def test_q1b_splits_the_monthly_totals_by_currency(analytics_db):
    q1b = analytics_db["q1b_monthly_revenue_by_category_and_currency"]
    assert q1b.columns[:3] == ["category", "currency", "total_transaction_amount"]
    assert [(r[0], r[1], r[2]) for r in q1b.rows] == [
        ("Retail", "EUR", Decimal("50.00")),
        ("Retail", "USD", Decimal("40.00")),
        ("Travel", "GBP", Decimal("40.00")),
    ]


def test_q1_follows_the_as_of_month(workspace, analytics_db):
    september = {r.name: r for r in run_analytics(workspace["db_path"], as_of=date(2026, 9, 3))}
    rows = september["q1_monthly_revenue_by_category"].rows
    assert [(r[0], r[1]) for r in rows] == [("Retail", Decimal("100.00"))]


def test_q2_averages_entity_level_averages(analytics_db):
    q2 = analytics_db["q2_top_5_regions_by_avg_transaction_amount_per_entity"]
    assert q2.columns[:2] == ["region", "average_transaction_amount_per_entity"]
    # NORTH: entity 1 avg (10+30)/2 = 20, entity 2 avg 100 -> (20+100)/2 = 60.
    # A plain average of NORTH transactions would be 140/3 = 46.67 and rank below SOUTH.
    assert [(r[0], float(r[1])) for r in q2.rows] == [
        ("EAST", 70.0), ("NORTH", 60.0), ("SOUTH", 50.0), ("WEST", 40.0), ("CENTRAL", 30.0),
    ]  # 6 regions exist; SOUTHWEST (20) is the 6th and is cut by the top-5 limit


def test_q3_never_mixes_currencies(analytics_db):
    q3 = analytics_db["q3_top_5_regions_per_currency"]
    by_currency = {}
    for currency, region, avg, _ in q3.rows:
        by_currency.setdefault(currency, []).append((region, float(avg)))
    assert by_currency["EUR"] == [("SOUTH", 50.0)]
    assert by_currency["GBP"] == [("WEST", 40.0)]
    # SOUTH only has EUR and WEST only has GBP, so just four regions have USD.
    assert by_currency["USD"] == [("EAST", 70.0), ("NORTH", 60.0), ("CENTRAL", 30.0), ("SOUTHWEST", 20.0)]


# --------------------------------------------------------------------------
# Repeatability and failure behaviour
# --------------------------------------------------------------------------

def _snapshot(db_path):
    return {
        table: _query(db_path, f"SELECT * FROM {table} ORDER BY ALL")
        for table in ("silver.silver_entities", "silver.silver_transactions",
                      "gold.dim_entity", "gold.dim_date", "gold.fact_transactions")
    }


def test_rerunning_the_pipeline_is_idempotent(generated_run):
    ws, _, first_summary = generated_run
    first_snapshot = _snapshot(ws["db_path"])

    second_summary = run_pipeline(ws["raw_dir"], ws["dlq_dir"], ws["db_path"])

    assert second_summary == first_summary
    assert _snapshot(ws["db_path"]) == first_snapshot


def test_failed_reconciliation_rolls_back_and_keeps_previous_warehouse(generated_run, monkeypatch):
    ws, _, summary = generated_run
    before = _snapshot(ws["db_path"])

    monkeypatch.setattr(warehouse, "reconcile", lambda metrics, raw_counts: ["forced failure"])
    with pytest.raises(ReconciliationError):
        run_pipeline(ws["raw_dir"], ws["dlq_dir"], ws["db_path"])

    assert _snapshot(ws["db_path"]) == before


def test_missing_source_file_fails_the_run(workspace):
    workspace["raw_dir"].mkdir()
    with pytest.raises(SourceFileError):
        run_pipeline(workspace["raw_dir"], workspace["dlq_dir"], workspace["db_path"])
    assert not workspace["db_path"].exists()


def test_cli_returns_error_code_on_failure_and_zero_on_success(workspace, capsys):
    args = ["--raw-dir", str(workspace["raw_dir"]), "--dlq-dir", str(workspace["dlq_dir"]),
            "--db-path", str(workspace["db_path"]), "--as-of", "2026-10-15"]
    workspace["raw_dir"].mkdir()
    assert pipeline_module.main(args) == 1

    write_sources(workspace["raw_dir"], [_entity(1, "north")], [_txn(1, 1, 10.0, "2026-10-02T10:00:00")])
    assert pipeline_module.main(args) == 0
    output = capsys.readouterr().out
    assert "Pipeline summary" in output
    assert "q2_top_5_regions_by_avg_transaction_amount_per_entity" in output
