"""Load the simulator's files into Snowflake RAW tables.

    python -m ingestion.load --date 2026-11-03
    python -m ingestion.load --start 2026-10-05 --end 2026-11-03
    python -m ingestion.load --date 2026-11-03 --dry-run
    python -m ingestion.load --verify
    python -m ingestion.load --check-idempotency 2026-10-30

Each entity's batch runs as one transaction: delete the batch, then copy it
in. A failure rolls back that entity; earlier entities may already be committed.

Parquet only. The simulator's --format csv exists for opening a file and
looking at it; loading relies on Parquet carrying its own schema, which is
what lets a whole record land in a single VARIANT column.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

from .sql import (
    ENTITIES,
    batch_counts_sql,
    copy_into_sql,
    create_table_sql,
    delete_batch_sql,
    discover_files,
    put_sql,
    total_count_sql,
)


def ensure_tables(cursor, database: str, schema: str) -> None:
    """Create the four RAW tables if they are not already there."""
    for entity in ENTITIES:
        cursor.execute(create_table_sql(database, schema, entity))


def load_batch(
    connection,
    settings,
    batch_date: date,
    data_dir: Path,
    file_format: str = "parquet",
    dry_run: bool = False,
) -> dict[str, int]:
    """Load one day for every entity that has a file.

    Returns rows loaded per entity. Entities with no file are skipped and
    reported by the caller, because a partially generated day is worth seeing
    rather than silently treating as zero.
    """
    files = discover_files(data_dir, batch_date, file_format)
    if not files:
        return {}

    missing = [entity for entity in ENTITIES if entity not in files]
    if missing:
        print(f"  ! no file for: {', '.join(missing)}")

    def statements_for(entity: str, path: Path) -> tuple[str, str, str]:
        return (
            put_sql(path, settings.stage, entity),
            delete_batch_sql(settings.database, settings.raw_schema, entity, batch_date),
            copy_into_sql(
                settings.database,
                settings.raw_schema,
                entity,
                settings.stage,
                settings.file_format_parquet,
                batch_date,
                path.name,
            ),
        )

    # A dry run never touches the connection, so it works with no credentials.
    if dry_run:
        for entity, path in files.items():
            put, delete, copy = statements_for(entity, path)
            print(f"\n-- {entity} --\n{put};\n{delete};\n{copy};")
        return dict.fromkeys(files, 0)

    results: dict[str, int] = {}
    cursor = connection.cursor()

    try:
        for entity, path in files.items():
            put, delete, copy = statements_for(entity, path)

            # PUT is outside the transaction: staging a file is not something
            # that needs rolling back, and the next PUT overwrites it anyway.
            cursor.execute(put)

            cursor.execute("begin")
            try:
                cursor.execute(delete)
                deleted = cursor.rowcount or 0
                cursor.execute(copy)
                loaded = sum(int(row[2]) for row in cursor.fetchall())
                cursor.execute("commit")
            except Exception:
                cursor.execute("rollback")
                raise

            results[entity] = loaded
            note = f" (replaced {deleted})" if deleted else ""
            print(f"  {entity:<14} {loaded:>6} rows{note}")
    finally:
        cursor.close()

    return results


def verify(connection, settings) -> None:
    """Print what is actually in RAW, per entity and batch date."""
    cursor = connection.cursor()
    try:
        for entity in ENTITIES:
            cursor.execute(total_count_sql(settings.database, settings.raw_schema, entity))
            total = cursor.fetchone()[0]
            print(f"\n{entity}  —  {total:,} rows")

            cursor.execute(batch_counts_sql(settings.database, settings.raw_schema, entity))
            rows = cursor.fetchall()
            if not rows:
                continue
            print(f"  {'batch_date':<12} {'rows':>8} {'events':>8}")
            for batch_date, loaded, events, _ in rows[:10]:
                print(f"  {str(batch_date):<12} {loaded:>8} {events:>8}")
            if len(rows) > 10:
                print(f"  ... {len(rows) - 10} more batch dates")
    finally:
        cursor.close()


def check_idempotency(connection, settings, batch_date: date, data_dir: Path) -> bool:
    """Reload one day and confirm nothing changed.

    Requires local files for all four entities and a complete reload result.
    Counts must be identical before and after. Load this date once beforehand.
    This verifies count stability, not byte-for-byte equality of row contents.
    """
    files = discover_files(data_dir, batch_date)
    missing = [entity for entity in ENTITIES if entity not in files]
    if missing:
        print(f"FAIL  cannot check idempotency: missing files for {', '.join(missing)}")
        return False

    cursor = connection.cursor()

    def snapshot() -> dict[str, int]:
        counts = {}
        for entity in ENTITIES:
            cursor.execute(total_count_sql(settings.database, settings.raw_schema, entity))
            counts[entity] = cursor.fetchone()[0]
        return counts

    try:
        before = snapshot()
        print(f"before reload: {before}")

        print(f"\nreloading {batch_date} ...")
        loaded = load_batch(connection, settings, batch_date, data_dir)
        if set(loaded) != set(ENTITIES):
            print("FAIL  reload did not report all four entities; idempotency was not verified.")
            return False

        after = snapshot()
        print(f"\nafter reload:  {after}")

        if before == after:
            print(f"\nPASS  counts unchanged after reloading {batch_date}")
            return True

        print("\nFAIL  counts changed. Confirm this date was already loaded and inspect the reload.")
        for entity in ENTITIES:
            if before[entity] != after[entity]:
                delta = after[entity] - before[entity]
                print(f"  {entity}: {before[entity]} -> {after[entity]} ({delta:+})")
        return False
    finally:
        cursor.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load simulator output into Snowflake RAW.")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--date", help="Load a single batch date, YYYY-MM-DD")
    mode.add_argument("--start", help="First batch date of a backfill, YYYY-MM-DD")
    mode.add_argument("--verify", action="store_true", help="Report what is in RAW")
    mode.add_argument(
        "--check-idempotency",
        metavar="YYYY-MM-DD",
        help="Reload one day and assert the row counts do not change",
    )
    parser.add_argument("--end", help="Last batch date of a backfill, YYYY-MM-DD")
    parser.add_argument("--data-dir", default="data", help="Where the files are (default: data)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the SQL without connecting or loading",
    )

    args = parser.parse_args(argv)
    if args.start and not args.end:
        parser.error("--start requires --end")
    return args


def _batch_dates(args: argparse.Namespace) -> list[date]:
    if args.date:
        return [date.fromisoformat(args.date)]
    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    if end < start:
        raise SystemExit("--end must not precede --start")
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    data_dir = Path(args.data_dir)

    from .connection import connect, load_settings

    # A dry run builds every statement without opening a connection, which is
    # the quickest way to see exactly what would be issued. It tolerates a
    # missing .env so the SQL can be reviewed before anything is configured.
    if args.dry_run:
        settings = load_settings(strict=False)
        for batch_date in _batch_dates(args):
            print(f"\n===== {batch_date} =====")
            load_batch(None, settings, batch_date, data_dir, dry_run=True)
        return 0

    settings = load_settings()
    connection = connect(settings)
    try:
        cursor = connection.cursor()
        ensure_tables(cursor, settings.database, settings.raw_schema)
        connection.commit()
        cursor.close()

        if args.verify:
            verify(connection, settings)
            return 0

        if args.check_idempotency:
            target = date.fromisoformat(args.check_idempotency)
            return 0 if check_idempotency(connection, settings, target, data_dir) else 1

        totals: dict[str, int] = {}
        for batch_date in _batch_dates(args):
            print(f"\n{batch_date}")
            results = load_batch(connection, settings, batch_date, data_dir)
            if not results:
                print("  ! no files found for this date")
            for entity, loaded in results.items():
                totals[entity] = totals.get(entity, 0) + loaded

        if totals:
            print("\n" + "-" * 40)
            for entity in ENTITIES:
                if entity in totals:
                    print(f"  {entity:<14} {totals[entity]:>8} rows loaded")
        return 0
    finally:
        connection.close()


if __name__ == "__main__":
    sys.exit(main())
