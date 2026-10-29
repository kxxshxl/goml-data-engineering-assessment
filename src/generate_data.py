"""Generate reproducible mock source data with intentional data-quality issues.

Outputs (default location ``data/raw``):

* ``entities.csv``       - entity records (customers / users)
* ``transactions.json``  - transaction records as a JSON array
* ``generation_manifest.json`` - what was generated and which issues were injected

Entity quality issues (separate records, so they can be tested independently):

* at least 5 per 100 rows are EXACT duplicates of another (valid) entity row
* at least 5 per 100 rows have a malformed email address

Counts are calculated with a ceiling, so any dataset of 3+ rows contains at
least one of each issue and the 5% minimum always holds.

Transaction quality issues (each invalid record has exactly ONE defect):

* negative ``transaction_amount``
* missing ``entity_id`` key, or ``entity_id`` = null
* unsupported ``currency``

In addition, a few contract-valid "orphan" transactions reference an entity_id
that does not exist. They pass Pydantic validation but are excluded from the
Gold fact table by the Silver referential-integrity check.

The output is fully determined by ``--seed`` and ``--as-of``: running the
generator twice with the same values produces byte-identical files.

Usage::

    python -m src.generate_data
    python -m src.generate_data --entities 1000 --transactions 10000 --seed 7
"""

import argparse
import csv
import json
import math
import random
import uuid
from datetime import date, datetime, time, timedelta
from pathlib import Path

from src.config import (
    ENTITIES_FILE_NAME,
    ENTITY_FIELDS,
    GENERATION_MANIFEST_FILE_NAME,
    RAW_DIR,
    TRANSACTIONS_FILE_NAME,
    TRANSACTION_FIELDS,
)
from src.contracts import EntityContract, TransactionContract

DEFAULT_SEED = 42
DEFAULT_ENTITY_ROWS = 500
DEFAULT_VALID_TRANSACTIONS = 5000
DEFAULT_INVALID_PER_TYPE = 20
DEFAULT_ORPHAN_TRANSACTIONS = 10

# Required by the assessment: at least 5 of every 100 entity rows.
DUPLICATES_PER_100 = 5
MALFORMED_EMAILS_PER_100 = 5
MIN_ENTITY_ROWS = 3  # one clean row, one duplicate of it, one malformed email

# How far back registrations and transactions are spread from the as-of date.
REGISTRATION_HISTORY_DAYS = 3 * 365
TRANSACTION_HISTORY_DAYS = 365
# Share of valid transactions forced into the current calendar month, so the
# "current month" analytical query always has data to report.
CURRENT_MONTH_SHARE = 0.15
# Probability that a clean entity gets cosmetic noise (stray spaces, mixed case)
# that the Silver layer must standardise. This noise is still contract-valid.
TEXT_NOISE_PROBABILITY = 0.10
# Spread of per-entity activity (sigma of a log-normal weight): some entities
# transact several times more often than others, without one dominating.
ACTIVITY_SKEW = 0.9

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Priya", "Arjun", "Aisha", "Wei", "Mateo", "Sofia",
    "Kenji", "Amara", "Lucas", "Fatima", "Noah", "Olivia",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Wilson", "Anderson", "Thomas", "Taylor",
    "Moore", "Jackson", "Martin", "Lee", "Patel", "Kumar", "Chen", "Nakamura",
    "Okafor", "Rossi", "Muller", "Silva", "Khan", "Nguyen",
]
# Reserved documentation domains (RFC 2606). Note: ".test" / ".local" style
# domains are NOT usable here - email-validator rejects special-use domains.
EMAIL_DOMAINS = ["example.com", "example.org", "example.net", "mail.example.com"]

# Canonical region -> average spend multiplier, so regions differ in Query 2.
REGION_SPEND_FACTOR = {
    "NORTH": 1.00,
    "SOUTH": 0.85,
    "EAST": 1.15,
    "WEST": 1.30,
    "CENTRAL": 0.95,
    "NORTHEAST": 1.10,
    "SOUTHWEST": 0.75,
}

# Category -> (min, max) base amount before the region multiplier.
CATEGORY_AMOUNT_RANGE = {
    "Retail": (5.0, 250.0),
    "Electronics": (50.0, 1500.0),
    "Groceries": (5.0, 180.0),
    "Travel": (80.0, 2500.0),
    "Healthcare": (20.0, 900.0),
    "Entertainment": (10.0, 300.0),
}

CURRENCY_WEIGHTS = {"USD": 0.60, "EUR": 0.25, "GBP": 0.15}
UNSUPPORTED_CURRENCIES = ["JPY", "INR", "AUD", "CAD", "BTC"]


def _new_uuid(rng: random.Random) -> str:
    """A random version-4 UUID drawn from the seeded generator (reproducible)."""
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def _messy_region(rng: random.Random, region: str) -> str:
    """Return the region with inconsistent casing / padding, e.g. 'north', ' East '."""
    variant = rng.choice([region.lower(), region.title(), region.upper()])
    if rng.random() < TEXT_NOISE_PROBABILITY:
        variant = f" {variant} "
    return variant


def _malformed_email(rng: random.Random, first: str, last: str, pattern_index: int) -> str:
    """Build an invalid email using the patterns listed in the assessment."""
    local = f"{first}.{last}".lower()
    patterns = [
        f"{local}.example.com",   # no @ at all
        f"{first.lower()}@",      # nothing after the @
        "@example.com",           # nothing before the @
        local,                    # no @ and no domain
        f"{local}@@example.com",  # two @ signs
    ]
    return patterns[pattern_index % len(patterns)]


def _random_date(rng: random.Random, start: date, end: date) -> date:
    return start + timedelta(days=rng.randint(0, (end - start).days))


def _random_datetime(rng: random.Random, start: datetime, end: datetime) -> datetime:
    seconds = int((end - start).total_seconds())
    return start + timedelta(seconds=rng.randint(0, seconds))


def build_entities(rng: random.Random, total_rows: int, as_of: date) -> tuple[list[dict], dict]:
    """Return (rows, manifest) for the entity CSV.

    ``total_rows`` is the number of rows in the file *including* the
    duplicate and malformed-email rows.
    """
    if total_rows < MIN_ENTITY_ROWS:
        raise ValueError(f"at least {MIN_ENTITY_ROWS} entity rows are required, got {total_rows}")

    duplicate_count = math.ceil(total_rows * DUPLICATES_PER_100 / 100)
    malformed_count = math.ceil(total_rows * MALFORMED_EMAILS_PER_100 / 100)
    clean_count = total_rows - duplicate_count - malformed_count

    regions = list(REGION_SPEND_FACTOR)
    registration_start = as_of - timedelta(days=REGISTRATION_HISTORY_DAYS)

    def make_entity(sequence: int) -> dict:
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        email = f"{first}.{last}{sequence}@{rng.choice(EMAIL_DOMAINS)}"
        if rng.random() < TEXT_NOISE_PROBABILITY:
            first = f"  {first}"  # stray whitespace for Silver to trim
        if rng.random() >= TEXT_NOISE_PROBABILITY:
            email = email.lower()  # most emails lower-case, some mixed case
        return {
            "entity_id": _new_uuid(rng),
            "first_name": first,
            "last_name": last,
            "contact_email": email,
            "registration_date": _random_date(rng, registration_start, as_of).isoformat(),
            "region": _messy_region(rng, rng.choice(regions)),
        }

    clean_rows = [make_entity(i) for i in range(clean_count)]

    # Exact duplicates: copies of distinct clean rows (identical in every field).
    duplicate_rows = [dict(row) for row in rng.sample(clean_rows, duplicate_count)]

    # Malformed emails: brand-new entities (own entity_id), never duplicated.
    malformed_rows = []
    for i in range(malformed_count):
        row = make_entity(clean_count + i)
        row["contact_email"] = _malformed_email(rng, row["first_name"].strip(), row["last_name"], i)
        malformed_rows.append(row)

    rows = clean_rows + duplicate_rows + malformed_rows
    rng.shuffle(rows)

    manifest = {
        "total_rows": total_rows,
        "unique_valid_entities": clean_count,
        "exact_duplicate_rows": duplicate_count,
        "malformed_email_rows": malformed_count,
        "duplicated_entity_ids": sorted(row["entity_id"] for row in duplicate_rows),
        "malformed_email_entity_ids": sorted(row["entity_id"] for row in malformed_rows),
    }
    return rows, manifest


def build_transactions(
    rng: random.Random,
    valid_entities: list[dict],
    valid_count: int,
    invalid_per_type: int,
    orphan_count: int,
    as_of: date,
) -> tuple[list[dict], dict]:
    """Return (records, manifest) for the transaction JSON file."""
    if not valid_entities:
        raise ValueError("at least one valid entity is required to generate transactions")

    period_end = datetime.combine(as_of, time(23, 59, 59))
    history_start = datetime.combine(as_of - timedelta(days=TRANSACTION_HISTORY_DAYS), time.min)
    month_start = datetime.combine(as_of.replace(day=1), time.min)

    # Skewed activity: a few entities transact far more often than others. This
    # is exactly the case where "average of entity averages" differs from a
    # plain average over all transactions.
    activity_weights = [rng.lognormvariate(0, ACTIVITY_SKEW) for _ in valid_entities]
    currencies, currency_weights = zip(*CURRENCY_WEIGHTS.items())
    categories = list(CATEGORY_AMOUNT_RANGE)

    def make_transaction(entity: dict) -> dict:
        category = rng.choice(categories)
        low, high = CATEGORY_AMOUNT_RANGE[category]
        region_factor = REGION_SPEND_FACTOR[entity["region"].strip().upper()]
        if rng.random() < CURRENT_MONTH_SHARE:
            moment = _random_datetime(rng, month_start, period_end)
        else:
            moment = _random_datetime(rng, history_start, period_end)
        return {
            "transaction_id": _new_uuid(rng),
            "entity_id": entity["entity_id"],
            "category": category,
            "transaction_amount": round(rng.uniform(low, high) * region_factor, 2),
            "currency": rng.choices(currencies, weights=currency_weights)[0],
            "timestamp": moment.isoformat(),
        }

    def random_valid_transaction() -> dict:
        entity = rng.choices(valid_entities, weights=activity_weights)[0]
        return make_transaction(entity)

    records = [random_valid_transaction() for _ in range(valid_count)]

    negative = []
    for _ in range(invalid_per_type):
        record = random_valid_transaction()
        record["transaction_amount"] = -record["transaction_amount"]
        negative.append(record)

    missing_entity = []
    for i in range(invalid_per_type):
        record = random_valid_transaction()
        if i % 2 == 0:
            del record["entity_id"]        # key absent
        else:
            record["entity_id"] = None     # key present, value null
        missing_entity.append(record)

    bad_currency = []
    for i in range(invalid_per_type):
        record = random_valid_transaction()
        record["currency"] = UNSUPPORTED_CURRENCIES[i % len(UNSUPPORTED_CURRENCIES)]
        bad_currency.append(record)

    orphans = []
    for _ in range(orphan_count):
        record = random_valid_transaction()
        record["entity_id"] = _new_uuid(rng)  # valid UUID, but no such entity
        orphans.append(record)

    records += negative + missing_entity + bad_currency + orphans
    rng.shuffle(records)

    manifest = {
        "total_records": len(records),
        "valid_transactions": valid_count,
        "negative_amount": len(negative),
        "missing_or_null_entity_id": len(missing_entity),
        "unsupported_currency": len(bad_currency),
        "orphan_entity_reference": len(orphans),
        "negative_amount_ids": sorted(r["transaction_id"] for r in negative),
        "missing_or_null_entity_id_ids": sorted(r["transaction_id"] for r in missing_entity),
        "unsupported_currency_ids": sorted(r["transaction_id"] for r in bad_currency),
        "orphan_transaction_ids": sorted(r["transaction_id"] for r in orphans),
    }
    return records, manifest


def _passes(contract, record: dict) -> bool:
    try:
        contract.model_validate(record)
        return True
    except ValueError:  # pydantic.ValidationError is a subclass of ValueError
        return False


def verify_injected_issues(entities: list[dict], transactions: list[dict], manifest: dict) -> None:
    """Fail loudly if the data does not contain exactly the intended issues.

    Every record meant to be valid must pass its contract and every record
    meant to be invalid must fail it. This stops the generator from
    accidentally adding unrelated bad records (for example an email domain the
    validator does not accept), which would hide whether the intended
    scenarios work.
    """
    malformed_ids = set(manifest["entities"]["malformed_email_entity_ids"])
    txn = manifest["transactions"]
    invalid_txn_ids = set(txn["negative_amount_ids"] + txn["missing_or_null_entity_id_ids"]
                          + txn["unsupported_currency_ids"])
    problems = [
        f"entity {row['entity_id']} expected {'invalid' if row['entity_id'] in malformed_ids else 'valid'}"
        for row in entities
        if _passes(EntityContract, row) == (row["entity_id"] in malformed_ids)
    ] + [
        f"transaction {rec['transaction_id']} expected "
        f"{'invalid' if rec['transaction_id'] in invalid_txn_ids else 'valid'}"
        for rec in transactions
        if _passes(TransactionContract, rec) == (rec["transaction_id"] in invalid_txn_ids)
    ]
    if problems:
        raise RuntimeError(f"generated data has {len(problems)} unintended issues, e.g. {problems[:3]}")


def generate(
    output_dir: Path = RAW_DIR,
    entity_rows: int = DEFAULT_ENTITY_ROWS,
    valid_transactions: int = DEFAULT_VALID_TRANSACTIONS,
    invalid_per_type: int = DEFAULT_INVALID_PER_TYPE,
    orphan_transactions: int = DEFAULT_ORPHAN_TRANSACTIONS,
    seed: int = DEFAULT_SEED,
    as_of: date | None = None,
) -> dict:
    """Generate both source files and return the generation manifest."""
    as_of = as_of or date.today()
    rng = random.Random(seed)

    entities, entity_manifest = build_entities(rng, entity_rows, as_of)

    malformed_ids = set(entity_manifest["malformed_email_entity_ids"])
    unique_valid_entities = {
        row["entity_id"]: row for row in entities if row["entity_id"] not in malformed_ids
    }
    # Sort so the choice of entities does not depend on the shuffled file order.
    valid_entities = [unique_valid_entities[key] for key in sorted(unique_valid_entities)]

    transactions, transaction_manifest = build_transactions(
        rng, valid_entities, valid_transactions, invalid_per_type, orphan_transactions, as_of
    )

    manifest = {
        "seed": seed,
        "as_of_date": as_of.isoformat(),
        "entities": entity_manifest,
        "transactions": transaction_manifest,
    }
    verify_injected_issues(entities, transactions, manifest)

    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / ENTITIES_FILE_NAME).open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=ENTITY_FIELDS)
        writer.writeheader()
        writer.writerows(entities)

    with (output_dir / TRANSACTIONS_FILE_NAME).open("w", encoding="utf-8") as handle:
        json.dump(transactions, handle, indent=2)
        handle.write("\n")

    with (output_dir / GENERATION_MANIFEST_FILE_NAME).open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)
        handle.write("\n")
    return manifest


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate mock entities.csv and transactions.json.")
    parser.add_argument("--output-dir", type=Path, default=RAW_DIR, help="where to write the files")
    parser.add_argument("--entities", type=int, default=DEFAULT_ENTITY_ROWS,
                        help="total entity rows, including duplicates and malformed emails")
    parser.add_argument("--transactions", type=int, default=DEFAULT_VALID_TRANSACTIONS,
                        help="number of fully valid transactions")
    parser.add_argument("--invalid-per-type", type=int, default=DEFAULT_INVALID_PER_TYPE,
                        help="invalid transactions per defect type (negative / missing id / currency)")
    parser.add_argument("--orphans", type=int, default=DEFAULT_ORPHAN_TRANSACTIONS,
                        help="contract-valid transactions that reference an unknown entity")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="random seed")
    parser.add_argument("--as-of", type=date.fromisoformat, default=None,
                        help="reference date YYYY-MM-DD (default: today); data ends on this date")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    manifest = generate(
        output_dir=args.output_dir,
        entity_rows=args.entities,
        valid_transactions=args.transactions,
        invalid_per_type=args.invalid_per_type,
        orphan_transactions=args.orphans,
        seed=args.seed,
        as_of=args.as_of,
    )
    ent, txn = manifest["entities"], manifest["transactions"]
    print(f"Wrote mock data to {args.output_dir} (seed={manifest['seed']}, as_of={manifest['as_of_date']})")
    print(f"  entities.csv      : {ent['total_rows']} rows = {ent['unique_valid_entities']} unique valid"
          f" + {ent['exact_duplicate_rows']} exact duplicates + {ent['malformed_email_rows']} malformed emails")
    print(f"  transactions.json : {txn['total_records']} records = {txn['valid_transactions']} valid"
          f" + {txn['negative_amount']} negative amount + {txn['missing_or_null_entity_id']} missing/null entity_id"
          f" + {txn['unsupported_currency']} unsupported currency + {txn['orphan_entity_reference']} orphans")


if __name__ == "__main__":
    main()
