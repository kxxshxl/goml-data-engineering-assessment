"""Logging behaviour: the right events, at the right levels, without personal data.

The tests check the ``event`` name and context fields attached to each log
record (via ``extra=``), never the exact wording or timestamps, so rewording
a message does not break them.
"""

import json
import logging
import sys

from conftest import write_sources
from src import pipeline as pipeline_module
from src import warehouse
from src.logging_setup import JsonFormatter
from src.pipeline import run_pipeline


def _logged(caplog, event):
    return [r for r in caplog.records if getattr(r, "event", None) == event]


def _cli_args(workspace, *extra):
    return ["--raw-dir", str(workspace["raw_dir"]), "--dlq-dir", str(workspace["dlq_dir"]),
            "--db-path", str(workspace["db_path"]), "--as-of", "2026-09-15", *extra]


def _small_dataset(valid_entity, valid_transaction):
    bad_entity = {**valid_entity, "entity_id": "550e8400-e29b-41d4-a716-446655440002",
                  "contact_email": "secret.person@@example.com"}
    bad_transaction = {**valid_transaction, "transaction_id": "550e8400-e29b-41d4-a716-446655440009",
                       "transaction_amount": -5.0}
    return [valid_entity, bad_entity], [valid_transaction, bad_transaction]


def test_pipeline_logs_every_stage_with_counts(workspace, caplog, valid_entity, valid_transaction):
    caplog.set_level(logging.INFO)
    entities, transactions = _small_dataset(valid_entity, valid_transaction)
    write_sources(workspace["raw_dir"], entities, transactions)

    run_pipeline(workspace["raw_dir"], workspace["dlq_dir"], workspace["db_path"])

    events = [r.event for r in caplog.records if hasattr(r, "event")]
    assert events == [
        "source_file_read", "source_file_read",
        "validation_completed", "validation_completed",
        "dlq_written", "dlq_written",
        "warehouse_build_started", "bronze_loaded", "silver_built", "gold_built",
        "reconciliation_passed", "warehouse_committed",
    ]
    entity_validation = _logged(caplog, "validation_completed")[0]
    assert (entity_validation.source_file, entity_validation.valid_records,
            entity_validation.rejected_records) == ("entities.csv", 1, 1)
    assert entity_validation.levelno == logging.WARNING  # rejections are worth noticing
    assert [r.records for r in _logged(caplog, "dlq_written")] == [1, 1]
    assert _logged(caplog, "gold_built")[0].fact_transactions == 1


def test_logs_never_contain_record_contents(workspace, caplog, valid_entity, valid_transaction):
    caplog.set_level(logging.DEBUG)
    entities, transactions = _small_dataset(valid_entity, valid_transaction)
    write_sources(workspace["raw_dir"], entities, transactions)

    run_pipeline(workspace["raw_dir"], workspace["dlq_dir"], workspace["db_path"])

    assert _logged(caplog, "validation_completed")  # logs were captured, so the check below is real
    formatter = JsonFormatter()
    all_logs = "\n".join(formatter.format(r) for r in caplog.records)
    for personal_value in ("John", "Smith", "john.smith@example.com", "secret.person@@example.com"):
        assert personal_value not in all_logs


def test_failed_reconciliation_logs_error_and_rollback(workspace, caplog, monkeypatch,
                                                       valid_entity, valid_transaction):
    write_sources(workspace["raw_dir"], [valid_entity], [valid_transaction])
    monkeypatch.setattr(warehouse, "reconcile", lambda metrics, raw_counts: ["forced failure"])
    caplog.set_level(logging.INFO)

    assert pipeline_module.main(_cli_args(workspace, "--skip-analytics")) == 1

    [failed_check] = _logged(caplog, "reconciliation_failed")
    assert failed_check.levelno == logging.ERROR and failed_check.failed_checks == ["forced failure"]
    assert _logged(caplog, "warehouse_rolled_back")[0].levelno == logging.WARNING
    [failure] = _logged(caplog, "pipeline_failed")
    assert failure.levelno == logging.ERROR and failure.error_type == "ReconciliationError"
    assert not _logged(caplog, "warehouse_committed")
    assert not _logged(caplog, "pipeline_finished")


def test_unexpected_error_is_logged_with_traceback(workspace, caplog, monkeypatch):
    def broken_pipeline(*args):
        raise RuntimeError("simulated bug")

    monkeypatch.setattr(pipeline_module, "run_pipeline", broken_pipeline)
    caplog.set_level(logging.INFO)

    assert pipeline_module.main(_cli_args(workspace)) == 1

    [failure] = _logged(caplog, "pipeline_failed")
    assert failure.levelno == logging.ERROR
    assert failure.error_type == "RuntimeError"
    assert failure.exc_info is not None  # the traceback is kept, not swallowed


def test_json_log_format_writes_json_lines_to_stderr_only(workspace, capsys, valid_entity, valid_transaction):
    write_sources(workspace["raw_dir"], [valid_entity], [valid_transaction])

    assert pipeline_module.main(_cli_args(workspace, "--log-format", "json")) == 0

    captured = capsys.readouterr()
    log_lines = [json.loads(line) for line in captured.err.splitlines() if line.strip()]
    assert log_lines and all({"timestamp", "level", "logger", "message"} <= set(line) for line in log_lines)
    events = [line.get("event") for line in log_lines]
    assert events[0] == "pipeline_started" and events[-1] == "pipeline_finished"
    assert "query_completed" in events
    # The human-readable report still goes to stdout, unchanged and separate from the logs.
    assert "Pipeline summary" in captured.out and "Pipeline summary" not in captured.err


def test_json_formatter_adds_context_fields_and_exception():
    record = logging.LogRecord("src.demo", logging.ERROR, __file__, 1, "Loaded %d rows", (3,), None)
    record.event = "demo_event"
    record.source_file = "entities.csv"
    try:
        raise ValueError("bad input")
    except ValueError:
        record.exc_info = sys.exc_info()

    entry = json.loads(JsonFormatter().format(record))

    assert entry["level"] == "ERROR" and entry["logger"] == "src.demo"
    assert entry["message"] == "Loaded 3 rows"
    assert entry["event"] == "demo_event" and entry["source_file"] == "entities.csv"
    assert "ValueError: bad input" in entry["exception"]
    assert "timestamp" in entry
