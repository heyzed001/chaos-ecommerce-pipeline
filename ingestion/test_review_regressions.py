"""Regression tests for review findings; no Snowflake calls or real credentials."""

from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from ingestion import connection as settings_module
from ingestion import load


@pytest.fixture
def isolated_settings(monkeypatch, tmp_path):
    import os

    for key in list(os.environ):
        if key.startswith("SNOWFLAKE_"):
            monkeypatch.delenv(key)
    root_env = tmp_path / ".env"
    legacy_env = tmp_path / ".gitignore" / ".env"
    monkeypatch.setattr(settings_module, "DEFAULT_ENV_FILE", root_env)
    monkeypatch.setattr(settings_module, "LEGACY_ENV_FILE", legacy_env)
    return root_env, legacy_env


def test_root_env_works_with_a_real_gitignore_file(isolated_settings, monkeypatch, tmp_path):
    root_env, _ = isolated_settings
    (tmp_path / ".gitignore").write_text(".env\n")
    root_env.write_text("SNOWFLAKE_ACCOUNT=root-account\n")
    other_directory = tmp_path / "somewhere_else"
    other_directory.mkdir()
    monkeypatch.chdir(other_directory)
    assert settings_module.load_settings(strict=False).account == "root-account"


def test_root_env_takes_priority_over_legacy(isolated_settings):
    root_env, legacy_env = isolated_settings
    root_env.write_text("SNOWFLAKE_ACCOUNT=root-account\n")
    legacy_env.parent.mkdir()
    legacy_env.write_text("SNOWFLAKE_ACCOUNT=legacy-account\n")
    assert settings_module.load_settings(strict=False).account == "root-account"


def test_legacy_env_still_works_when_root_is_missing(isolated_settings):
    _, legacy_env = isolated_settings
    legacy_env.parent.mkdir()
    legacy_env.write_text("SNOWFLAKE_ACCOUNT=legacy-account\n")
    assert settings_module.load_settings(strict=False).account == "legacy-account"


def test_explicit_env_file_is_honored(isolated_settings, tmp_path):
    root_env, _ = isolated_settings
    root_env.write_text("SNOWFLAKE_ACCOUNT=root-account\n")
    custom = tmp_path / "custom.env"
    custom.write_text("SNOWFLAKE_ACCOUNT=custom-account\n")
    assert settings_module.load_settings(custom, strict=False).account == "custom-account"


def test_existing_process_environment_is_not_overridden(isolated_settings, monkeypatch):
    root_env, _ = isolated_settings
    root_env.write_text("SNOWFLAKE_ACCOUNT=file-account\n")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "process-account")
    assert settings_module.load_settings(strict=False).account == "process-account"


@pytest.mark.parametrize("entities", [[], ["customers", "products", "orders"]])
def test_idempotency_refuses_missing_or_partial_files(tmp_path, monkeypatch, entities):
    batch_date = date(2026, 11, 3)
    for entity in entities:
        folder = tmp_path / entity
        folder.mkdir()
        (folder / f"{batch_date}.parquet").write_bytes(b"fixture")
    reload_batch = Mock()
    monkeypatch.setattr(load, "load_batch", reload_batch)
    conn = Mock()
    assert not load.check_idempotency(conn, Mock(), batch_date, tmp_path)
    conn.cursor.assert_not_called()
    reload_batch.assert_not_called()


@pytest.mark.parametrize("complete_result", [True, False])
def test_idempotency_requires_complete_reload_results(tmp_path, monkeypatch, complete_result):
    batch_date = date(2026, 11, 3)
    for entity in load.ENTITIES:
        folder = tmp_path / entity
        folder.mkdir()
        (folder / f"{batch_date}.parquet").write_bytes(b"fixture")
    loaded = dict.fromkeys(load.ENTITIES, 10) if complete_result else {"customers": 10}
    reload_batch = Mock(return_value=loaded)
    monkeypatch.setattr(load, "load_batch", reload_batch)
    conn = Mock()
    conn.cursor.return_value.fetchone.return_value = (10,)
    settings = SimpleNamespace(database="TEST_DB", raw_schema="RAW")
    assert load.check_idempotency(conn, settings, batch_date, tmp_path) == complete_result
    reload_batch.assert_called_once()
    conn.cursor.return_value.close.assert_called_once()
