"""Tests for the mock data generator's intentional data-quality issues."""

import csv
import json
import math
from collections import Counter
from datetime import date, datetime

import pytest

from src.config import ENTITIES_FILE_NAME, ENTITY_FIELDS, TRANSACTIONS_FILE_NAME
from src.contracts import EntityContract, TransactionContract
from src.generate_data import build_entities, generate, verify_injected_issues

AS_OF = date(2026, 10, 27)


def _passes(contract, record) -> bool:
    try:
        contract.model_validate(record)
        return True
    except ValueError:
        return False


def _read(output_dir):
    with (output_dir / ENTITIES_FILE_NAME).open(newline="", encoding="utf-8") as handle:
        entities = list(csv.DictReader(handle))
    transactions = json.loads((output_dir / TRANSACTIONS_FILE_NAME).read_text(encoding="utf-8"))
    return entities, transactions


@pytest.fixture
def generated(tmp_path):
    out = tmp_path / "raw"
    manifest = generate(out, entity_rows=200, valid_transactions=400, invalid_per_type=6,
                        orphan_transactions=3, seed=7, as_of=AS_OF)
    entities, transactions = _read(out)
    return entities, transactions, manifest


def test_at_least_5_exact_duplicates_per_100_entity_rows(generated):
    entities, _, _ = generated
    row_counts = Counter(tuple(row[f] for f in ENTITY_FIELDS) for row in entities)
    duplicate_rows = sum(count - 1 for count in row_counts.values() if count > 1)
    assert len(entities) == 200
    assert duplicate_rows >= 10


def test_at_least_5_malformed_emails_per_100_entity_rows(generated):
    entities, _, _ = generated
    malformed = [row for row in entities if not _passes(EntityContract, row)]
    assert len(malformed) >= 10
    # the only problem with these rows is the email
    for row in malformed:
        assert _passes(EntityContract, {**row, "contact_email": "fixed@example.com"})


def test_duplicate_and_malformed_email_records_are_separate(generated):
    entities, _, _ = generated
    id_counts = Counter(row["entity_id"] for row in entities)
    duplicated_ids = {entity_id for entity_id, count in id_counts.items() if count > 1}
    malformed_ids = {row["entity_id"] for row in entities if not _passes(EntityContract, row)}

    assert duplicated_ids and malformed_ids
    assert duplicated_ids.isdisjoint(malformed_ids)
    # every duplicated record is otherwise valid
    assert all(_passes(EntityContract, row) for row in entities if row["entity_id"] in duplicated_ids)


@pytest.mark.parametrize("rows", [3, 20, 101, 250, 1000])
def test_quality_minimums_hold_for_any_dataset_size(rows):
    import random
    _, manifest = build_entities(random.Random(1), rows, AS_OF)
    minimum = math.ceil(rows * 5 / 100)
    assert manifest["exact_duplicate_rows"] >= minimum
    assert manifest["malformed_email_rows"] >= minimum


def test_too_few_entity_rows_is_rejected():
    import random
    with pytest.raises(ValueError):
        build_entities(random.Random(1), 2, AS_OF)


def test_each_invalid_transaction_type_is_present(generated):
    _, transactions, manifest = generated
    invalid = [t for t in transactions if not _passes(TransactionContract, t)]

    negative = [t for t in invalid if isinstance(t.get("transaction_amount"), float)
                and t["transaction_amount"] < 0]
    missing_entity = [t for t in invalid if t.get("entity_id") is None]
    bad_currency = [t for t in invalid if t.get("currency") not in {"USD", "EUR", "GBP"}]

    assert len(negative) == len(missing_entity) == len(bad_currency) == 6
    assert len(invalid) == 18  # each invalid record has exactly one defect
    assert any("entity_id" not in t for t in missing_entity)   # key missing
    assert any("entity_id" in t for t in missing_entity)       # key present but null
    assert manifest["transactions"]["valid_transactions"] == 400


def test_orphan_transactions_pass_contract_but_reference_unknown_entities(generated):
    entities, transactions, manifest = generated
    entity_ids = {row["entity_id"] for row in entities}
    orphan_ids = set(manifest["transactions"]["orphan_transaction_ids"])
    orphans = [t for t in transactions if t["transaction_id"] in orphan_ids]

    assert len(orphans) == 3
    assert all(_passes(TransactionContract, t) for t in orphans)
    assert all(t["entity_id"] not in entity_ids for t in orphans)


def test_valid_transactions_only_reference_valid_entities(generated):
    entities, transactions, manifest = generated
    valid_entity_ids = {row["entity_id"] for row in entities if _passes(EntityContract, row)}
    orphan_ids = set(manifest["transactions"]["orphan_transaction_ids"])
    for t in transactions:
        if _passes(TransactionContract, t) and t["transaction_id"] not in orphan_ids:
            assert t["entity_id"] in valid_entity_ids


def test_current_month_has_transactions_and_nothing_after_as_of(generated):
    _, transactions, _ = generated
    timestamps = [datetime.fromisoformat(t["timestamp"]) for t in transactions]
    assert any(ts.year == AS_OF.year and ts.month == AS_OF.month for ts in timestamps)
    assert max(timestamps).date() <= AS_OF


def test_same_seed_and_date_give_identical_files(tmp_path):
    for name in ("a", "b"):
        generate(tmp_path / name, entity_rows=100, valid_transactions=50, seed=3, as_of=AS_OF)
    for file_name in (ENTITIES_FILE_NAME, TRANSACTIONS_FILE_NAME):
        assert (tmp_path / "a" / file_name).read_bytes() == (tmp_path / "b" / file_name).read_bytes()


def test_different_seed_gives_different_data(tmp_path):
    generate(tmp_path / "a", entity_rows=100, valid_transactions=50, seed=1, as_of=AS_OF)
    generate(tmp_path / "b", entity_rows=100, valid_transactions=50, seed=2, as_of=AS_OF)
    assert (tmp_path / "a" / ENTITIES_FILE_NAME).read_bytes() != (tmp_path / "b" / ENTITIES_FILE_NAME).read_bytes()


def test_verify_injected_issues_detects_an_unintended_bad_record(generated):
    entities, transactions, manifest = generated
    clean = next(row for row in entities
                 if row["entity_id"] not in manifest["entities"]["malformed_email_entity_ids"])
    broken = [dict(row) for row in entities]
    broken[entities.index(clean)]["contact_email"] = "not-an-email"
    with pytest.raises(RuntimeError):
        verify_injected_issues(broken, transactions, manifest)
