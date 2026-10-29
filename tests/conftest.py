"""Shared test helpers.

Every test that touches files or a database works inside pytest's ``tmp_path``,
so the real ``data/`` folder and ``data/warehouse.duckdb`` are never modified.
"""

import csv
import json
import logging
from pathlib import Path

import pytest

from src.config import ENTITIES_FILE_NAME, ENTITY_FIELDS, TRANSACTIONS_FILE_NAME


@pytest.fixture(autouse=True)
def restore_logging():
    """Undo the log handler and level that ``pipeline.main()`` installs.

    Without this, a handler created in one test would keep pointing at that
    test's (closed) captured stderr.
    """
    root = logging.getLogger()
    level = root.level
    yield
    for handler in [h for h in root.handlers if getattr(h, "is_pipeline_handler", False)]:
        root.removeHandler(handler)
    root.setLevel(level)


@pytest.fixture
def valid_entity() -> dict:
    return {
        "entity_id": "550e8400-e29b-41d4-a716-446655440001",
        "first_name": "John",
        "last_name": "Smith",
        "contact_email": "john.smith@example.com",
        "registration_date": "2025-03-14",
        "region": "north",
    }


@pytest.fixture
def valid_transaction() -> dict:
    return {
        "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
        "entity_id": "550e8400-e29b-41d4-a716-446655440001",
        "category": "Retail",
        "transaction_amount": 150.50,
        "currency": "USD",
        "timestamp": "2026-09-24T10:00:00",
    }


def write_sources(raw_dir: Path, entities: list[dict], transactions: list) -> None:
    """Write an entities.csv and a transactions.json for a test scenario."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    with (raw_dir / ENTITIES_FILE_NAME).open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=ENTITY_FIELDS)
        writer.writeheader()
        writer.writerows(entities)
    (raw_dir / TRANSACTIONS_FILE_NAME).write_text(json.dumps(transactions), encoding="utf-8")


@pytest.fixture
def workspace(tmp_path: Path) -> dict:
    """Isolated raw / dlq / database locations for one test."""
    return {
        "raw_dir": tmp_path / "raw",
        "dlq_dir": tmp_path / "dlq",
        "db_path": tmp_path / "warehouse.duckdb",
    }
