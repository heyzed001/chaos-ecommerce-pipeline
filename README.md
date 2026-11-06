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

Create local configuration from the credential-free templates:

```powershell
Copy-Item .env.example .env
Copy-Item dbt_project\profiles.yml.example dbt_project\profiles.yml
```

Edit root `.env` with your account and credentials; keep real secrets out of
Git. Ingestion reads this file independently of the working directory. Your
existing `.gitignore/.env` is supported only as a legacy fallback when root
`.env` is absent; new clones use a normal `.gitignore` file, not a directory.
Keep root `.env` as the authoritative local configuration.

Load variables into PowerShell and install the dbt packages:

```powershell
. .\dbt_project\load_env.ps1
dbt deps --project-dir dbt_project --profiles-dir dbt_project
dbt debug --project-dir dbt_project --profiles-dir dbt_project
```

On bash-compatible shells, use `set -a; source .env; set +a` instead of the
PowerShell helper. The dbt profile template contains environment references,
not credentials. `profiles.yml` stays local and ignored; its `.example` is
included in Git. If your VS Code dbt extension requires a project-local `.env`,
copy root `.env` to `dbt_project/.env` and refresh that copy after credential
changes. Both real environment files are ignored.

Airflow runs separately in Docker; do not install it into the dbt environment.

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
