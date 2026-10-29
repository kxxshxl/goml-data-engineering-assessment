"""Logging set-up for the pipeline CLI (standard library only).

Logs are written to stderr, so they never mix with the summary and query
tables that the pipeline prints to stdout. Two formats are available:

* ``text`` (default), for people reading a terminal::

    2026-10-29 10:00:00,123 INFO src.ingest: Read 500 records from entities.csv

* ``json``, one JSON object per line, for log collectors and dashboards::

    {"timestamp": "2026-10-29T04:30:00.123+00:00", "level": "INFO", "logger": "src.ingest",
     "message": "Read 500 records from entities.csv", "event": "source_file_read",
     "source_file": "entities.csv", "records": 500}

Modules attach machine-readable context with ``extra={"event": ..., ...}``.
The text format shows the message only (it already contains the numbers);
the JSON format also adds every context field as its own key.

Log messages contain counts, file names and timings only, never record
contents: entity records hold personal data (names and email addresses).
"""

import json
import logging
from datetime import datetime, timezone

TEXT_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR"]
LOG_FORMATS = ["text", "json"]

# Attributes every LogRecord has. Anything else was passed in through ``extra``.
_STANDARD_ATTRIBUTES = set(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    """Format each log record as a single-line JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        entry.update({key: value for key, value in vars(record).items() if key not in _STANDARD_ATTRIBUTES})
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def configure_logging(level: str = "INFO", log_format: str = "text") -> None:
    """Send log records to stderr in the chosen format.

    Only the handler added by an earlier call is replaced, so calling this
    more than once (as the tests do) never duplicates log lines or removes
    handlers that other code installed.
    """
    root = logging.getLogger()
    for handler in [h for h in root.handlers if getattr(h, "is_pipeline_handler", False)]:
        root.removeHandler(handler)
    handler = logging.StreamHandler()
    handler.is_pipeline_handler = True
    handler.setFormatter(JsonFormatter() if log_format == "json" else logging.Formatter(TEXT_FORMAT))
    root.addHandler(handler)
    root.setLevel(level)
