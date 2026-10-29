"""Unit tests for the Pydantic contracts (the validation boundary).

Tests assert *behaviour* (valid / rejected, and which field failed), never the
exact wording of Pydantic's error messages, which can change between versions.
"""

from datetime import date, datetime
from uuid import UUID

import pytest
from pydantic import ValidationError

from src.contracts import EntityContract, TransactionContract


def failed_fields(exc_info: pytest.ExceptionInfo) -> set[str]:
    return {".".join(str(p) for p in error["loc"]) for error in exc_info.value.errors()}


# --------------------------------------------------------------------------
# The three tests required by the assessment
# --------------------------------------------------------------------------

def test_transaction_rejects_negative_amount(valid_transaction):
    invalid_payload = {**valid_transaction, "transaction_amount": -150.50}
    with pytest.raises(ValidationError) as exc_info:
        TransactionContract(**invalid_payload)
    assert failed_fields(exc_info) == {"transaction_amount"}


def test_entity_rejects_invalid_email(valid_entity):
    invalid_payload = {**valid_entity, "contact_email": "john.example.com"}
    with pytest.raises(ValidationError) as exc_info:
        EntityContract(**invalid_payload)
    assert failed_fields(exc_info) == {"contact_email"}


@pytest.mark.parametrize("make_invalid", [
    lambda payload: payload.pop("entity_id"),             # key missing
    lambda payload: payload.update(entity_id=None),       # key present, null
], ids=["missing", "null"])
def test_transaction_rejects_missing_entity_id(valid_transaction, make_invalid):
    payload = dict(valid_transaction)
    make_invalid(payload)
    with pytest.raises(ValidationError) as exc_info:
        TransactionContract(**payload)
    assert failed_fields(exc_info) == {"entity_id"}


# --------------------------------------------------------------------------
# Valid records are accepted and typed correctly
# --------------------------------------------------------------------------

def test_valid_entity_is_accepted_and_typed(valid_entity):
    entity = EntityContract(**valid_entity)
    assert isinstance(entity.entity_id, UUID)
    assert entity.registration_date == date(2025, 3, 14)
    assert entity.region == "north"  # contracts validate; they do not standardise


def test_valid_transaction_is_accepted_and_typed(valid_transaction):
    transaction = TransactionContract(**valid_transaction)
    assert isinstance(transaction.transaction_id, UUID)
    assert transaction.timestamp == datetime(2026, 9, 24, 10, 0, 0)
    assert transaction.transaction_amount == 150.50


@pytest.mark.parametrize("currency", ["USD", "EUR", "GBP"])
def test_each_allowed_currency_is_accepted(valid_transaction, currency):
    assert TransactionContract(**{**valid_transaction, "currency": currency}).currency == currency


def test_integer_amount_is_accepted(valid_transaction):
    assert TransactionContract(**{**valid_transaction, "transaction_amount": 20}).transaction_amount == 20.0


# --------------------------------------------------------------------------
# Entity contract: additional rejections
# --------------------------------------------------------------------------

@pytest.mark.parametrize("email", [
    "john.example.com", "john@", "@example.com", "john.example", "john@@example.com", "", "john smith@example.com",
])
def test_entity_rejects_each_malformed_email_pattern(valid_entity, email):
    with pytest.raises(ValidationError):
        EntityContract(**{**valid_entity, "contact_email": email})


@pytest.mark.parametrize("field", ["first_name", "last_name", "region"])
@pytest.mark.parametrize("blank", ["", "   "], ids=["empty", "whitespace"])
def test_entity_rejects_blank_required_text(valid_entity, field, blank):
    with pytest.raises(ValidationError) as exc_info:
        EntityContract(**{**valid_entity, field: blank})
    assert failed_fields(exc_info) == {field}


@pytest.mark.parametrize("bad_id", ["not-a-uuid", "12345", "", None])
def test_entity_rejects_invalid_uuid(valid_entity, bad_id):
    with pytest.raises(ValidationError) as exc_info:
        EntityContract(**{**valid_entity, "entity_id": bad_id})
    assert failed_fields(exc_info) == {"entity_id"}


@pytest.mark.parametrize("bad_date", ["2025-02-30", "14/03/2025", "yesterday", ""])
def test_entity_rejects_invalid_registration_date(valid_entity, bad_date):
    with pytest.raises(ValidationError) as exc_info:
        EntityContract(**{**valid_entity, "registration_date": bad_date})
    assert failed_fields(exc_info) == {"registration_date"}


def test_entity_accepts_datetime_registration_and_keeps_the_date(valid_entity):
    entity = EntityContract(**{**valid_entity, "registration_date": "2025-03-14T09:30:00"})
    assert entity.registration_date == date(2025, 3, 14)


@pytest.mark.parametrize("field", ["entity_id", "first_name", "last_name", "contact_email",
                                   "registration_date", "region"])
def test_entity_rejects_missing_field(valid_entity, field):
    payload = dict(valid_entity)
    del payload[field]
    with pytest.raises(ValidationError) as exc_info:
        EntityContract(**payload)
    assert failed_fields(exc_info) == {field}


def test_entity_rejects_unexpected_field(valid_entity):
    with pytest.raises(ValidationError):
        EntityContract(**{**valid_entity, "nickname": "Johnny"})


# --------------------------------------------------------------------------
# Transaction contract: additional rejections
# --------------------------------------------------------------------------

@pytest.mark.parametrize("currency", ["JPY", "INR", "usd", "US D", "", "DOLLAR"])
def test_transaction_rejects_unsupported_currency(valid_transaction, currency):
    with pytest.raises(ValidationError) as exc_info:
        TransactionContract(**{**valid_transaction, "currency": currency})
    assert failed_fields(exc_info) == {"currency"}


def test_transaction_rejects_zero_amount(valid_transaction):
    with pytest.raises(ValidationError):
        TransactionContract(**{**valid_transaction, "transaction_amount": 0})


@pytest.mark.parametrize("amount", [float("nan"), float("inf"), "150.50", True, None],
                         ids=["nan", "inf", "string", "bool", "null"])
def test_transaction_rejects_non_numeric_or_non_finite_amount(valid_transaction, amount):
    with pytest.raises(ValidationError) as exc_info:
        TransactionContract(**{**valid_transaction, "transaction_amount": amount})
    assert failed_fields(exc_info) == {"transaction_amount"}


@pytest.mark.parametrize("field", ["transaction_id", "entity_id"])
@pytest.mark.parametrize("bad_id", ["not-a-uuid", "550e8400-e29b-41d4-a716", 42])
def test_transaction_rejects_invalid_uuid(valid_transaction, field, bad_id):
    with pytest.raises(ValidationError) as exc_info:
        TransactionContract(**{**valid_transaction, field: bad_id})
    assert failed_fields(exc_info) == {field}


@pytest.mark.parametrize("bad_timestamp", ["2026-13-01T10:00:00", "not a date", "", 1727172000],
                         ids=["bad-month", "text", "empty", "epoch-number"])
def test_transaction_rejects_invalid_timestamp(valid_transaction, bad_timestamp):
    with pytest.raises(ValidationError) as exc_info:
        TransactionContract(**{**valid_transaction, "timestamp": bad_timestamp})
    assert failed_fields(exc_info) == {"timestamp"}


@pytest.mark.parametrize("field", ["transaction_id", "entity_id", "category", "transaction_amount",
                                   "currency", "timestamp"])
def test_transaction_rejects_missing_required_field(valid_transaction, field):
    payload = dict(valid_transaction)
    del payload[field]
    with pytest.raises(ValidationError) as exc_info:
        TransactionContract(**payload)
    assert failed_fields(exc_info) == {field}


@pytest.mark.parametrize("category", ["", "   "], ids=["empty", "whitespace"])
def test_transaction_rejects_blank_category(valid_transaction, category):
    with pytest.raises(ValidationError):
        TransactionContract(**{**valid_transaction, "category": category})


def test_transaction_timestamp_with_offset_is_converted_to_utc(valid_transaction):
    transaction = TransactionContract(**{**valid_transaction, "timestamp": "2026-09-24T15:30:00+05:30"})
    assert transaction.timestamp == datetime(2026, 9, 24, 10, 0, 0)
    assert transaction.timestamp.tzinfo is None


def test_transaction_reports_every_broken_field(valid_transaction):
    payload = {**valid_transaction, "transaction_amount": -1, "currency": "JPY", "entity_id": None}
    with pytest.raises(ValidationError) as exc_info:
        TransactionContract(**payload)
    assert failed_fields(exc_info) == {"transaction_amount", "currency", "entity_id"}
