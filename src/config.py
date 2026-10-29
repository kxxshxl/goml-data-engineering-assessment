"""Central configuration: default file locations and source field lists.

All paths are derived from the project root, so the pipeline works no matter
which directory it is started from and contains no machine-specific paths.
Every path can be overridden from the command line.
"""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
DLQ_DIR = DATA_DIR / "dlq"
SQL_DIR = PROJECT_ROOT / "sql"
DEFAULT_DB_PATH = DATA_DIR / "warehouse.duckdb"

ENTITIES_FILE_NAME = "entities.csv"
TRANSACTIONS_FILE_NAME = "transactions.json"
GENERATION_MANIFEST_FILE_NAME = "generation_manifest.json"
INVALID_ENTITIES_FILE_NAME = "invalid_entities.json"
INVALID_TRANSACTIONS_FILE_NAME = "invalid_transactions.json"

# Column order of the raw sources. The generator writes these and the
# ingestion step checks that the CSV header contains all of them.
ENTITY_FIELDS = [
    "entity_id",
    "first_name",
    "last_name",
    "contact_email",
    "registration_date",
    "region",
]
TRANSACTION_FIELDS = [
    "transaction_id",
    "entity_id",
    "category",
    "transaction_amount",
    "currency",
    "timestamp",
]

# SQL scripts executed (in this order) to build the warehouse.
BRONZE_SQL = SQL_DIR / "bronze.sql"
SILVER_SQL = SQL_DIR / "silver.sql"
GOLD_SQL = SQL_DIR / "gold.sql"
ANALYTICS_SQL = SQL_DIR / "analytics.sql"
