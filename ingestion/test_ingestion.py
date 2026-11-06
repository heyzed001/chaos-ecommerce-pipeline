"""Tests for the ingestion SQL builders and file discovery.

No Snowflake connection is needed. The statements this pipeline issues are
built as strings, so the interesting properties -- that reloading deletes the
batch first, that FORCE is set, that an unknown entity is rejected -- can all
be asserted offline.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from ingestion.sql import (
    ENTITIES,
    batch_counts_sql,
    copy_into_sql,
    create_table_sql,
    delete_batch_sql,
    discover_files,
    put_sql,
    total_count_sql,
)

DB, SCHEMA = "ECOMMERCE", "RAW"
STAGE = "ECOMMERCE.RAW.STG_LANDING"
FF = "ECOMMERCE.RAW.FF_PARQUET"
BATCH = date(2026, 11, 3)


def copy_for(entity: str = "orders") -> str:
    return copy_into_sql(
        DB, SCHEMA, entity, STAGE, FF, BATCH, f"{BATCH.isoformat()}.parquet"
    )


def test_every_entity_has_ddl():
    for entity in ENTITIES:
        ddl = create_table_sql(DB, SCHEMA, entity)
        assert f"{DB}.{SCHEMA}.{entity}" in ddl
        assert "if not exists" in ddl


def test_raw_table_stores_the_whole_record_as_variant():
    """RAW must not impose a column list, or schema drift breaks ingestion."""
    ddl = create_table_sql(DB, SCHEMA, "orders")
    assert "_record      variant" in ddl
    # Business columns belong in staging, not here.
    for business_column in ("order_id", "customer_id", "order_total", "quantity"):
        assert business_column not in ddl


def test_raw_table_has_the_load_metadata():
    ddl = create_table_sql(DB, SCHEMA, "orders")
    for column in ("_event_id", "_batch_date", "_source_file", "_loaded_at"):
        assert column in ddl


def test_copy_forces_a_reload():
    """Without FORCE, a reload after DELETE silently loads nothing.

    Snowflake keeps the filename in its load history and skips it, which would
    leave the table missing exactly the day that was just deleted.
    """
    assert "force = true" in copy_for()


def test_copy_sets_the_named_file_format_on_the_copy_statement():
    sql = copy_for()
    assert f")\nfile_format = (format_name = '{FF}')" in sql
    assert "file_format =>" not in sql


def test_copy_aborts_rather_than_skipping_bad_rows():
    assert "on_error = 'abort_statement'" in copy_for()


def test_copy_binds_the_batch_date_instead_of_reading_it():
    """The loader knows the date; it should not depend on the file's encoding."""
    sql = copy_for()
    assert f"'{BATCH.isoformat()}'::date" in sql
    assert "$1:_batch_date" not in sql


def test_copy_reads_the_whole_row_into_one_column():
    sql = copy_for()
    assert "select\n        $1," in sql
    assert "metadata$filename" in sql


def test_copy_targets_the_specific_file():
    """Loading a whole folder would re-ingest every day on every run."""
    sql = copy_for("customers")
    assert f"@{STAGE}/customers/{BATCH.isoformat()}.parquet" in sql


def test_delete_scopes_to_one_batch_date():
    sql = delete_batch_sql(DB, SCHEMA, "orders", BATCH)
    assert sql.startswith(f"delete from {DB}.{SCHEMA}.orders")
    assert f"_batch_date = '{BATCH.isoformat()}'::date" in sql


def test_put_disables_double_compression():
    sql = put_sql(Path("data/orders/2026-11-03.parquet"), STAGE, "orders")
    assert "auto_compress = false" in sql
    assert "overwrite = true" in sql


def test_put_uses_an_absolute_path_with_forward_slashes():
    """PUT rejects a relative path, and on Windows it wants file://C:/...

    Both failures are quiet in different ways: a relative path errors at
    runtime, and a backslash path errors only on Windows.
    """
    sql = put_sql(Path("data") / "orders" / "2026-11-03.parquet", STAGE, "orders")
    assert "\\" not in sql
    assert sql.startswith("put 'file://")
    staged_path = sql.removeprefix("put 'file://").split(f"' @{STAGE}/orders/", 1)[0]
    staged_path = staged_path.replace("''", "'")
    assert Path(staged_path).is_absolute() or staged_path[1:3] == ":/"
    assert staged_path == (Path("data") / "orders" / "2026-11-03.parquet").resolve().as_posix()
    assert staged_path.endswith("/data/orders/2026-11-03.parquet")


@pytest.mark.parametrize("folder", ["directory with spaces", "Aziz's data"])
def test_put_quotes_paths_with_spaces_and_escapes_apostrophes(tmp_path, folder):
    local_path = tmp_path / folder / "2026-11-03.parquet"
    sql = put_sql(local_path, STAGE, "orders")
    expected_uri = ("file://" + local_path.resolve().as_posix()).replace("'", "''")
    assert sql == (
        f"put '{expected_uri}' @{STAGE}/orders/ "
        "auto_compress = false overwrite = true"
    )


def test_unknown_entity_is_rejected():
    """Entity names reach the SQL text, so only the known four are allowed."""
    for builder in (create_table_sql, delete_batch_sql):
        with pytest.raises(ValueError):
            if builder is delete_batch_sql:
                builder(DB, SCHEMA, "orders; drop table users", BATCH)
            else:
                builder(DB, SCHEMA, "orders; drop table users")


def test_bad_identifier_is_rejected():
    with pytest.raises(ValueError):
        create_table_sql("ecommerce; drop database x", SCHEMA, "orders")


def test_discover_files_finds_what_exists(tmp_path):
    for entity in ("customers", "orders"):
        folder = tmp_path / entity
        folder.mkdir()
        (folder / f"{BATCH.isoformat()}.parquet").write_bytes(b"x")

    found = discover_files(tmp_path, BATCH)
    assert set(found) == {"customers", "orders"}
    assert found["orders"].name == "2026-11-03.parquet"


def test_discover_files_ignores_other_dates(tmp_path):
    folder = tmp_path / "orders"
    folder.mkdir()
    (folder / "2026-11-02.parquet").write_bytes(b"x")
    assert discover_files(tmp_path, BATCH) == {}


def test_discover_files_handles_csv_format(tmp_path):
    folder = tmp_path / "orders"
    folder.mkdir()
    (folder / f"{BATCH.isoformat()}.csv").write_bytes(b"x")
    assert set(discover_files(tmp_path, BATCH, "csv")) == {"orders"}


def test_count_queries_are_scoped_to_the_table():
    assert total_count_sql(DB, SCHEMA, "products").endswith("ECOMMERCE.RAW.products")
    assert "_batch_date" in batch_counts_sql(DB, SCHEMA, "products")
