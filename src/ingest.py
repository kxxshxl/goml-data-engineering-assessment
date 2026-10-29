"""Extraction and contract validation (Phase 1).

* Read the raw CSV / JSON files.
* Validate every record against its Pydantic contract.
* Collect valid records for Bronze and route invalid ones to the Dead Letter Queue.

Two kinds of failure are handled differently on purpose:

* **Record-level** problems (a bad email, a negative amount, ...) never stop
  the pipeline. The record goes to the DLQ together with the reason.
* **File-level** problems (file missing, not valid JSON, wrong CSV header)
  raise ``SourceFileError``. They mean the whole input is unusable, and
  reporting a "successful" run with zero rows would hide the problem.
"""

import csv
import json
import logging
import math
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ValidationError

from src.config import ENTITY_FIELDS

# Logs contain counts and file names only, never record contents (personal data).
logger = logging.getLogger(__name__)


class SourceFileError(Exception):
    """A source file is missing, unreadable, or has the wrong overall structure."""


@dataclass
class ValidationResult:
    """Outcome of validating one source file."""

    valid_rows: list[dict] = field(default_factory=list)   # ready for Bronze
    dlq_entries: list[dict] = field(default_factory=list)  # ready for the DLQ file

    @property
    def total(self) -> int:
        return len(self.valid_rows) + len(self.dlq_entries)


def read_entities_csv(path: Path) -> list[dict]:
    """Read the entity CSV into a list of dicts (all values are strings)."""
    if not path.is_file():
        raise SourceFileError(f"entity source file not found: {path}")
    try:
        # utf-8-sig also accepts files saved with a byte-order mark (e.g. by Excel).
        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None:
                raise SourceFileError(f"entity source file is empty: {path}")
            missing = [name for name in ENTITY_FIELDS if name not in reader.fieldnames]
            if missing:
                raise SourceFileError(f"{path.name} header is missing required columns: {missing}")

            records = []
            for row in reader:
                # DictReader stores surplus values under the key None. Keep them
                # under a readable name so the contract rejects the row and the
                # DLQ shows exactly what was received.
                extra_values = row.pop(None, None)
                if extra_values:
                    row["_unexpected_extra_values"] = extra_values
                records.append(row)
    except (UnicodeDecodeError, csv.Error) as exc:
        raise SourceFileError(f"could not parse {path.name} as UTF-8 CSV: {exc}") from exc
    _log_source_read(path, len(records))
    return records


def _log_source_read(path: Path, record_count: int) -> None:
    logger.info("Read %d records from %s", record_count, path.name,
                extra={"event": "source_file_read", "source_file": path.name, "records": record_count})


def read_transactions_json(path: Path) -> list[Any]:
    """Read the transaction JSON file, which must be a top-level array."""
    if not path.is_file():
        raise SourceFileError(f"transaction source file not found: {path}")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SourceFileError(f"{path.name} is not valid UTF-8 JSON: {exc}") from exc
    if not isinstance(data, list):
        raise SourceFileError(
            f"{path.name} must contain a JSON array of transaction objects, got {type(data).__name__}"
        )
    _log_source_read(path, len(data))
    return data


def describe_validation_error(exc: ValidationError) -> list[dict]:
    """Turn a Pydantic error into a list of {field, type, message} dictionaries."""
    return [
        {
            "field": ".".join(str(part) for part in error["loc"]) or "<record>",
            "type": error["type"],
            "message": error["msg"],
        }
        for error in exc.errors()
    ]


def _dlq_entry(record: Any, errors: list[dict], source_file: str, record_number: int) -> dict:
    return {
        "source_file": source_file,
        "record_number": record_number,
        "rejected_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "record": record,
        # One readable line (matches the example in the brief) plus the details.
        "error": "; ".join(f"{e['field']}: {e['message']}" for e in errors),
        "errors": errors,
    }


def validate_records(records: list[Any], contract: type[BaseModel], source_file: str) -> ValidationResult:
    """Validate each record; one bad record never prevents the others from loading.

    ``record_number`` is the 1-based position of the record in its source file
    (for the CSV, data row 1 is the first line after the header). It is kept in
    both Bronze and the DLQ so any row can be traced back to its origin.
    """
    result = ValidationResult()
    for record_number, record in enumerate(records, start=1):
        if not isinstance(record, dict):
            errors = [{"field": "<record>", "type": "not_an_object",
                       "message": f"record must be a JSON object, got {type(record).__name__}"}]
            result.dlq_entries.append(_dlq_entry(record, errors, source_file, record_number))
            continue
        try:
            validated = contract.model_validate(record)
        except ValidationError as exc:
            result.dlq_entries.append(
                _dlq_entry(record, describe_validation_error(exc), source_file, record_number)
            )
            continue
        row = validated.model_dump(mode="json")
        row["source_record_number"] = record_number
        result.valid_rows.append(row)

    # Rejected records are expected data-quality findings, not a pipeline
    # failure, so they are reported as a WARNING (and the run continues).
    rejected = len(result.dlq_entries)
    logger.log(logging.WARNING if rejected else logging.INFO,
               "Validated %s: %d valid, %d rejected", source_file, len(result.valid_rows), rejected,
               extra={"event": "validation_completed", "source_file": source_file,
                      "valid_records": len(result.valid_rows), "rejected_records": rejected})
    return result


def _json_safe(value: Any) -> Any:
    """Replace NaN / Infinity (which Python's json reader accepts) with strings.

    The DLQ must be strictly valid JSON, and a rejected record may contain
    exactly those values.
    """
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def write_dlq(entries: list[dict], path: Path) -> None:
    """Write DLQ entries as a JSON array, replacing the previous run's file.

    The file is always written (an empty run produces ``[]``) so a stale DLQ
    from an earlier run can never be mistaken for the current one. Writing to a
    temporary file first means a crash cannot leave a half-written DLQ behind.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8") as handle:
        json.dump(_json_safe(entries), handle, indent=2, allow_nan=False, default=str)
        handle.write("\n")
    os.replace(temp_path, path)
    logger.info("Wrote %d rejected records to %s", len(entries), path,
                extra={"event": "dlq_written", "dlq_file": str(path), "records": len(entries)})
