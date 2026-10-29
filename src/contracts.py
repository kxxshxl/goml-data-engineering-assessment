"""Pydantic v2 data contracts for the two raw sources.

Every raw record is validated against one of these models *before* it is
loaded into the Bronze layer. A record that breaks the contract raises
``pydantic.ValidationError`` and is routed to the Dead Letter Queue instead.

Design notes
------------
* ``extra="forbid"``: an unexpected field means the producer changed its
  schema, which is a contract violation rather than something to ignore.
* Required string fields must contain at least one non-whitespace character.
  The value itself is NOT trimmed here; Bronze keeps what was received and
  the Silver layer does the standardisation.
* ``transaction_amount`` is validated in strict mode, so ``"100"`` (a string)
  or ``true`` (a boolean) are rejected instead of being silently coerced.
  NaN and infinity are rejected as well.
"""

from datetime import date, datetime, timezone
from typing import Annotated, Any
from uuid import UUID

from pydantic import AfterValidator, BaseModel, ConfigDict, EmailStr, Field, field_validator

ALLOWED_CURRENCIES = frozenset({"USD", "EUR", "GBP"})


def _reject_blank(value: str) -> str:
    if not value.strip():
        raise ValueError("must not be empty or whitespace only")
    return value


# A required string that must contain real text ("" and "   " are rejected).
NonBlankStr = Annotated[str, Field(min_length=1), AfterValidator(_reject_blank)]


class EntityContract(BaseModel):
    """One row of ``entities.csv``."""

    model_config = ConfigDict(extra="forbid")

    entity_id: UUID
    first_name: NonBlankStr
    last_name: NonBlankStr
    contact_email: EmailStr
    registration_date: date
    region: NonBlankStr

    @field_validator("registration_date", mode="before")
    @classmethod
    def accept_date_or_datetime(cls, value: Any) -> Any:
        """Accept an ISO date ("2025-03-14") or an ISO datetime ("2025-03-14T09:30:00").

        Registration is a day-level attribute, so a datetime is reduced to its
        calendar date. An unparseable string raises ``ValueError``, which
        Pydantic reports as a normal validation error.
        """
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str) and ("T" in value or " " in value.strip()):
            return datetime.fromisoformat(value.strip()).date()
        return value


class TransactionContract(BaseModel):
    """One object of ``transactions.json``."""

    model_config = ConfigDict(extra="forbid")

    transaction_id: UUID
    entity_id: UUID
    category: NonBlankStr
    transaction_amount: float = Field(gt=0, strict=True, allow_inf_nan=False)
    currency: str
    timestamp: datetime

    @field_validator("currency")
    @classmethod
    def validate_currency(cls, value: str) -> str:
        if value not in ALLOWED_CURRENCIES:
            raise ValueError(
                f"unsupported currency {value!r}; allowed values are {sorted(ALLOWED_CURRENCIES)}"
            )
        return value

    @field_validator("timestamp", mode="before")
    @classmethod
    def timestamp_must_be_text(cls, value: Any) -> Any:
        """Reject numbers, which Pydantic would otherwise read as Unix epoch seconds."""
        if not isinstance(value, (str, datetime)):
            raise ValueError("timestamp must be an ISO-8601 datetime string")
        return value

    @field_validator("timestamp")
    @classmethod
    def normalise_to_utc(cls, value: datetime) -> datetime:
        """Store every timestamp as naive UTC.

        Timestamps without an offset are assumed to already be UTC. Timestamps
        with an offset (e.g. ``+05:30``) are converted, so all values in the
        warehouse are directly comparable.
        """
        if value.tzinfo is not None:
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
