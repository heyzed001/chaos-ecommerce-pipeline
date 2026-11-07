"""Task preflight. No database connections and no credential values in output."""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from zoneinfo import ZoneInfo
from datetime import datetime


def validate_batch_date(value: str, today: date) -> date:
    try:
        batch_date = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("Choose batch_date as YYYY-MM-DD in the manual trigger form.") from exc
    if value != batch_date.isoformat():
        raise ValueError("batch_date must use the exact YYYY-MM-DD format.")
    if batch_date < date(2026, 11, 4):
        raise ValueError("This daily DAG starts after the manual history window: use 2026-11-04 or later.")
    if batch_date >= today:
        raise ValueError("Process completed days only: batch_date must be before today in Africa/Lagos.")
    return batch_date


def main() -> None:
    today = datetime.now(ZoneInfo("Africa/Lagos")).date()
    batch_date = validate_batch_date(os.environ.get("PIPELINE_BATCH_DATE", ""), today)
    from ingestion.connection import load_settings

    settings = load_settings()
    if not settings.password:
        raise RuntimeError("This local DAG uses dbt's dev/password profile; configure SNOWFLAKE_PASSWORD on the worker.")
    if settings.uses_key_pair:
        raise RuntimeError("Remove the key-path override for this password-based local DAG, or configure a Linux key mount and prod profile first.")
    for name in ("simulator/generate.py", "ingestion/load.py", "dbt_project/dbt_project.yml", "profiles/profiles.yml"):
        if not (Path("/opt/pipeline") / name).is_file():
            raise RuntimeError(f"Required container mount missing: {name}")
    if not Path("/opt/pipeline/data").is_dir():
        raise RuntimeError("The shared data directory is not mounted.")
    print(f"Pipeline configuration ready for {batch_date}; credentials loaded without connecting.")


if __name__ == "__main__":
    main()
