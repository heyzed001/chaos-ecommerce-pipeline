# Chaos e-commerce pipeline

An e-commerce data engineering project using Python, Snowflake, dbt, Airflow,
and Docker. The simulator generates repeatable source data with duplicates,
late arrivals, missing values, incorrect totals, and optional schema drift.

## Continue on another computer

Install Git and Python 3.12, then clone this repository:

```powershell
git clone https://github.com/heyzed001/chaos-ecommerce-pipeline.git
cd chaos-ecommerce-pipeline
py -3.12 -m venv .venv
& .\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

On macOS or Linux, use `python3.12 -m venv .venv` and
`source .venv/bin/activate` instead. Select the new `.venv` interpreter in your
editor. Virtual environments are machine-specific and must be recreated.

Credentials are not committed. Recreate any needed `.env` file locally using
your credentials. Airflow runs separately in Docker; do not install it into
the dbt environment.

## Generate source data

From the repository root with the environment activated:

```powershell
python -m simulator.generate --start 2026-10-05 --end 2026-11-03
```

This writes Parquet datasets and daily chaos logs under `data/`. Generated
data is excluded from Git; regenerate it on each computer. The same dates,
seed, and configuration produce the same records. Rerunning replaces the
files for the requested dates.

## Sync work between computers

Before starting work, run `git pull --ff-only`. After making changes:

```powershell
git add .
git commit -m "Describe your changes"
git push
```

Commit and push before switching computers. See `docs/journal.md` for project
notes and `docs/` for the architecture and data models.
