"""Read-only before/after warehouse counts for an unchanged batch rerun."""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path

from ingestion.connection import connect, load_settings
from ingestion.sql import ENTITIES, _check_identifier


def collect_counts(cursor, database: str, raw_schema: str, batch_date: str) -> dict:
    _check_identifier(database, "database")
    _check_identifier(raw_schema, "raw_schema")
    counts = {}
    for entity in ENTITIES:
        table = f"{database}.{raw_schema}.{entity}"
        cursor.execute(f"select count(*), count_if(_batch_date = %s::date) from {table}", (batch_date,))
        total, batch = cursor.fetchone()
        counts[f"{table}.total"] = int(total)
        counts[f"{table}.batch"] = int(batch or 0)
    for schema, table in [("SNAPSHOTS", "SNAP_CUSTOMERS"), ("MARTS", "DIM_CUSTOMER"),
                          ("MARTS", "DIM_PRODUCT"), ("MARTS", "FCT_ORDER_ITEMS")]:
        name = f"{database}.{schema}.{table}"
        cursor.execute(f"select count(*) from {name}")
        counts[f"{name}.total"] = int(cursor.fetchone()[0])
    cursor.execute(f"select count(order_item_id) - count(distinct order_item_id), count_if(order_item_id is null) from {database}.MARTS.FCT_ORDER_ITEMS")
    duplicates, nulls = cursor.fetchone()
    counts["fact_duplicate_keys"] = int(duplicates)
    counts["fact_null_keys"] = int(nulls or 0)
    return counts


def compare_counts(before: dict, after: dict) -> list[str]:
    changes = [f"{key}: {before.get(key)} -> {after.get(key)}"
               for key in sorted(before.keys() | after.keys()) if before.get(key) != after.get(key)]
    for key in ("fact_duplicate_keys", "fact_null_keys"):
        if after.get(key, 0):
            changes.append(f"{key} must be zero (found {after[key]})")
    return changes


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("capture", "verify"))
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--baseline", required=True, type=Path)
    args = parser.parse_args()
    settings = load_settings()
    baseline = None
    if args.mode == "verify":
        baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
        if baseline["batch_date"] != args.date.isoformat() or baseline["database"] != settings.database:
            raise ValueError("Baseline date/database does not match this check.")
    with connect(settings) as connection:
        with connection.cursor() as cursor:
            counts = collect_counts(cursor, settings.database, settings.raw_schema, args.date.isoformat())
    if args.mode == "capture":
        if any(counts[f"{settings.database}.{settings.raw_schema}.{entity}.batch"] == 0 for entity in ENTITIES):
            raise ValueError("Choose an already loaded date with rows in every RAW entity.")
        args.baseline.parent.mkdir(parents=True, exist_ok=True)
        # Exclusive creation prevents a retry from silently replacing the baseline.
        with args.baseline.open("x", encoding="utf-8") as output:
            json.dump({"batch_date": args.date.isoformat(), "database": settings.database,
                       "captured_at": datetime.now(timezone.utc).isoformat(), "counts": counts}, output, indent=2)
        print(f"Baseline saved to {args.baseline}")
    else:
        changes = compare_counts(baseline["counts"], counts)
        print(json.dumps({"batch_date": args.date.isoformat(), "counts": counts, "changes": changes}, indent=2))
        if changes:
            raise SystemExit("FAILED: rerun counts changed or fact keys are invalid. Inspect the report.")
        print("PASS: counts unchanged; fact keys are unique and not null. This does not prove equal row contents.")


if __name__ == "__main__":
    main()
