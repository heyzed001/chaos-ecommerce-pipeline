"""Snowflake connection, configured entirely from .env.

Supports both password and key-pair authentication. Which one is used depends
on whether SNOWFLAKE_PRIVATE_KEY_PATH is set, so switching later needs no code
change.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_FILE = PROJECT_ROOT / ".env"
LEGACY_ENV_FILE = PROJECT_ROOT / ".gitignore" / ".env"


@dataclass(frozen=True)
class SnowflakeSettings:
    """Everything the pipeline needs to reach Snowflake."""

    account: str
    user: str
    role: str
    warehouse: str
    database: str
    raw_schema: str
    stage: str
    file_format_parquet: str
    password: str | None = None
    private_key_path: str | None = None

    @property
    def uses_key_pair(self) -> bool:
        return bool(self.private_key_path)


def _require(name: str, strict: bool = True) -> str:
    value = os.environ.get(name, "").strip()
    if value:
        return value
    if not strict:
        # Dry runs should work with no .env at all, so an unset variable shows
        # up in the printed SQL as UNSET_<NAME> rather than stopping the run.
        return f"UNSET_{name}"
    raise RuntimeError(
        f"{name} is missing from your environment.\n"
        f"Copy .env.example to .env and fill it in, then re-run."
    )


def load_settings(
    env_file: str | Path | None = None, strict: bool = True
) -> SnowflakeSettings:
    """Read the project's root .env and build the settings.

    The default path is relative to this module, so it works regardless of
    the terminal's working directory. Pass env_file to use another location.
    The old .gitignore/.env location is a fallback only when root .env is absent.

    python-dotenv is optional: if it is not installed, real environment
    variables still work, which is how the Airflow container will supply them
    in Phase 10.

    strict=False skips the credential check so `--dry-run` can print the SQL
    on a machine that has never been configured.
    """
    try:
        from dotenv import load_dotenv

        env_path = Path(env_file) if env_file is not None else DEFAULT_ENV_FILE
        if env_file is None and not env_path.is_file() and LEGACY_ENV_FILE.is_file():
            env_path = LEGACY_ENV_FILE
        load_dotenv(env_path, override=False)
    except ImportError:
        pass

    password = os.environ.get("SNOWFLAKE_PASSWORD", "").strip() or None
    key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "").strip() or None

    if strict and not password and not key_path:
        raise RuntimeError(
            "No credentials found. Set SNOWFLAKE_PASSWORD, or "
            "SNOWFLAKE_PRIVATE_KEY_PATH for key-pair authentication."
        )

    database = _require("SNOWFLAKE_DATABASE", strict)
    raw_schema = os.environ.get("SNOWFLAKE_RAW_SCHEMA", "RAW").strip()

    return SnowflakeSettings(
        account=_require("SNOWFLAKE_ACCOUNT", strict),
        user=_require("SNOWFLAKE_USER", strict),
        role=os.environ.get("SNOWFLAKE_ROLE", "ECOMMERCE_ROLE").strip(),
        warehouse=_require("SNOWFLAKE_WAREHOUSE", strict),
        database=database,
        raw_schema=raw_schema,
        stage=os.environ.get(
            "SNOWFLAKE_STAGE", f"{database}.{raw_schema}.STG_LANDING"
        ).strip(),
        file_format_parquet=os.environ.get(
            "SNOWFLAKE_FILE_FORMAT_PARQUET", f"{database}.{raw_schema}.FF_PARQUET"
        ).strip(),
        password=password,
        private_key_path=key_path,
    )


def _private_key_der(path: str) -> bytes:
    """Read a PKCS#8 private key and return it in the DER form the driver wants."""
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    key_file = Path(path)
    if not key_file.is_file():
        raise RuntimeError(f"Private key not found at {key_file}")

    passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "").strip()
    loaded = serialization.load_pem_private_key(
        key_file.read_bytes(),
        password=passphrase.encode() if passphrase else None,
        backend=default_backend(),
    )
    return loaded.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def connect(settings: SnowflakeSettings | None = None):
    """Open a connection. The caller is responsible for closing it."""
    import snowflake.connector

    settings = settings or load_settings()

    params = {
        "account": settings.account,
        "user": settings.user,
        "role": settings.role,
        "warehouse": settings.warehouse,
        "database": settings.database,
        "schema": settings.raw_schema,
        # Without this, a failed statement mid-transaction leaves the session
        # in a state where every following statement also fails, and the real
        # error gets buried under the noise.
        "autocommit": False,
        "client_session_keep_alive": True,
        "application": "chaos_ecommerce_pipeline",
    }

    if settings.uses_key_pair:
        params["private_key"] = _private_key_der(settings.private_key_path)
    else:
        params["password"] = settings.password

    return snowflake.connector.connect(**params)
