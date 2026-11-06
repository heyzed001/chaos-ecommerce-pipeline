# Load .env into the current PowerShell session.
#
# WHY THIS EXISTS
# The Python scripts read .env themselves through python-dotenv. The dbt CLI
# does not: its env_var() calls read real environment variables. So .env has to
# be loaded into the shell before any dbt command, or dbt fails with
# "Env var required but not provided: SNOWFLAKE_ACCOUNT".
#
# USAGE (dot-source it, so the variables survive the script ending)
#   . .\dbt_project\load_env.ps1
#   dbt debug --project-dir dbt_project --profiles-dir dbt_project
#
# The bash equivalent, for the Airflow container and CI:
#   set -a; source .env; set +a

param(
    [string]$EnvFile = (Join-Path (Split-Path -Parent $PSScriptRoot) '.env')
)

if (-not (Test-Path $EnvFile)) {
    Write-Error "$EnvFile not found. Copy .env.example to .env and fill it in."
    throw 'Environment file is required; see .env.example at the repository root.'
}

$loaded = 0
Get-Content $EnvFile | ForEach-Object {
    $line = $_.Trim()

    # Skip blanks and comments
    if ($line -eq "" -or $line.StartsWith("#")) { return }
    if ($line -notmatch "=") { return }

    $parts = $line -split "=", 2
    $name  = $parts[0].Trim()
    $value = $parts[1].Trim()

    # Strip surrounding quotes if someone added them. dbt would otherwise
    # treat them as part of the value and the connection would fail with a
    # message that does not mention quoting at all.
    if ($value.Length -ge 2) {
        if (($value.StartsWith('"') -and $value.EndsWith('"')) -or
            ($value.StartsWith("'") -and $value.EndsWith("'"))) {
            $value = $value.Substring(1, $value.Length - 2)
        }
    }

    if ($name -ne "") {
        [Environment]::SetEnvironmentVariable($name, $value, "Process")
        $loaded++
    }
}

Write-Host "Loaded $loaded variables from $EnvFile" -ForegroundColor Green

# Do not print variable values; future authentication methods may add new secrets.
