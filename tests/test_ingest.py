"""Tests for file reading, record routing and the Dead Letter Queue."""

import json

import pytest

from src.config import ENTITIES_FILE_NAME, TRANSACTIONS_FILE_NAME
from src.contracts import EntityContract, TransactionContract
from src.ingest import (
    SourceFileError,
    read_entities_csv,
    read_transactions_json,
    validate_records,
    write_dlq,
)


def test_validate_records_splits_valid_and_invalid(valid_transaction):
    records = [
        valid_transaction,
        {**valid_transaction, "transaction_id": "550e8400-e29b-41d4-a716-446655440009",
         "transaction_amount": -5.0},
    ]
    result = validate_records(records, TransactionContract, TRANSACTIONS_FILE_NAME)

    assert len(result.valid_rows) == 1
    assert len(result.dlq_entries) == 1
    assert result.total == 2


def test_dlq_entry_keeps_original_payload_and_error_details(valid_transaction):
    bad = {**valid_transaction, "entity_id": None}
    entry = validate_records([valid_transaction, bad], TransactionContract, TRANSACTIONS_FILE_NAME).dlq_entries[0]

    assert entry["record"] == bad                       # untouched original
    assert entry["record_number"] == 2                  # traceable to the source
    assert entry["source_file"] == TRANSACTIONS_FILE_NAME
    assert [e["field"] for e in entry["errors"]] == ["entity_id"]
    assert entry["error"].startswith("entity_id:")      # readable one-line summary


def test_one_bad_record_does_not_stop_the_rest(valid_entity):
    records = [
        {**valid_entity, "entity_id": f"550e8400-e29b-41d4-a716-44665544000{i}"} for i in range(5)
    ]
    records[2]["contact_email"] = "broken@"
    result = validate_records(records, EntityContract, ENTITIES_FILE_NAME)

    assert len(result.valid_rows) == 4
    assert [e["record_number"] for e in result.dlq_entries] == [3]


def test_valid_row_carries_source_record_number(valid_entity):
    row = validate_records([valid_entity], EntityContract, ENTITIES_FILE_NAME).valid_rows[0]
    assert row["source_record_number"] == 1
    assert row["entity_id"] == valid_entity["entity_id"]


@pytest.mark.parametrize("not_an_object", [42, "text", None, ["a", "list"]])
def test_non_object_json_item_goes_to_dlq(not_an_object):
    result = validate_records([not_an_object], TransactionContract, TRANSACTIONS_FILE_NAME)
    assert result.valid_rows == []
    assert result.dlq_entries[0]["record"] == not_an_object
    assert result.dlq_entries[0]["errors"][0]["type"] == "not_an_object"


def test_write_dlq_produces_valid_json_and_replaces_old_file(tmp_path, valid_transaction):
    dlq_file = tmp_path / "dlq" / "invalid_transactions.json"
    write_dlq([{"stale": True}, {"stale": True}], dlq_file)

    bad = {**valid_transaction, "currency": "JPY"}
    entries = validate_records([bad], TransactionContract, TRANSACTIONS_FILE_NAME).dlq_entries
    write_dlq(entries, dlq_file)

    content = json.loads(dlq_file.read_text(encoding="utf-8"))
    assert len(content) == 1
    assert content[0]["record"] == bad
    assert content[0]["errors"][0]["field"] == "currency"


def test_write_dlq_writes_empty_array_when_nothing_was_rejected(tmp_path):
    dlq_file = tmp_path / "invalid_entities.json"
    write_dlq([], dlq_file)
    assert json.loads(dlq_file.read_text(encoding="utf-8")) == []


def test_write_dlq_handles_nan_in_rejected_record(tmp_path, valid_transaction):
    bad = {**valid_transaction, "transaction_amount": float("nan")}
    entries = validate_records([bad], TransactionContract, TRANSACTIONS_FILE_NAME).dlq_entries
    dlq_file = tmp_path / "invalid_transactions.json"
    write_dlq(entries, dlq_file)

    # json.loads with a strict parse_constant proves the file contains no bare NaN token.
    def reject(token):
        raise AssertionError(f"non-standard JSON token {token}")
    content = json.loads(dlq_file.read_text(encoding="utf-8"), parse_constant=reject)
    assert content[0]["record"]["transaction_amount"] == "nan"


# --------------------------------------------------------------------------
# File-level problems raise SourceFileError (the run must fail visibly)
# --------------------------------------------------------------------------

def test_missing_transaction_file_raises(tmp_path):
    with pytest.raises(SourceFileError):
        read_transactions_json(tmp_path / "does_not_exist.json")


def test_missing_entity_file_raises(tmp_path):
    with pytest.raises(SourceFileError):
        read_entities_csv(tmp_path / "does_not_exist.csv")


def test_malformed_json_raises(tmp_path):
    path = tmp_path / "transactions.json"
    path.write_text('[{"transaction_id": "abc",', encoding="utf-8")
    with pytest.raises(SourceFileError):
        read_transactions_json(path)


def test_json_object_instead_of_array_raises(tmp_path):
    path = tmp_path / "transactions.json"
    path.write_text('{"transaction_id": "abc"}', encoding="utf-8")
    with pytest.raises(SourceFileError):
        read_transactions_json(path)


def test_csv_missing_required_column_raises(tmp_path):
    path = tmp_path / "entities.csv"
    path.write_text("entity_id,first_name,last_name\n1,a,b\n", encoding="utf-8")
    with pytest.raises(SourceFileError):
        read_entities_csv(path)


def test_empty_csv_raises(tmp_path):
    path = tmp_path / "entities.csv"
    path.write_text("", encoding="utf-8")
    with pytest.raises(SourceFileError):
        read_entities_csv(path)


def test_csv_row_with_extra_values_is_rejected_not_crashing(tmp_path, valid_entity):
    header = ",".join(valid_entity)
    good = ",".join(valid_entity.values())
    path = tmp_path / "entities.csv"
    path.write_text(f"{header}\n{good}\n{good},surplus\n", encoding="utf-8")

    records = read_entities_csv(path)
    result = validate_records(records, EntityContract, ENTITIES_FILE_NAME)
    assert len(result.valid_rows) == 1
    assert len(result.dlq_entries) == 1
    assert result.dlq_entries[0]["record"]["_unexpected_extra_values"] == ["surplus"]


def test_csv_with_byte_order_mark_is_read(tmp_path, valid_entity):
    path = tmp_path / "entities.csv"
    content = ",".join(valid_entity) + "\n" + ",".join(valid_entity.values()) + "\n"
    path.write_text(content, encoding="utf-8-sig")
    records = read_entities_csv(path)
    assert records[0]["entity_id"] == valid_entity["entity_id"]
