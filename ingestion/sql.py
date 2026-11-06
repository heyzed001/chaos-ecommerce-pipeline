"""SQL statement builders.

Pure string construction with no database dependency, so every statement this
pipeline issues can be unit-tested without a Snowflake connection.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

#: The four source entities. Nothing outside this tuple is accepted as an
#: entity name, which is also what keeps these builders injection-safe.
ENTITIES = ("customers", "products", "orders", "order_items")


def _check_entity(entity: str) -> str:
    if entity not in ENTITIES:
        raise ValueError(f"unknown entity {entity!r}; expected one of {ENTITIES}")
    return entity


def _check_identifier(value: str, label: str) -> str:
    """Allow only plain SQL identifiers, optionally dotted."""
    cleaned = value.replace("_", "").replace(".", "")
    if not value or not cleaned.isalnum():
        raise ValueError(f"{label} must be a plain identifier, got {value!r}")
    return value


def create_table_sql(database: str, schema: str, entity: str) -> str:
    """DDL for one RAW table.

    The whole source record goes into `_record` as a VARIANT. Only the columns
    needed to manage the load itself are extracted beside it:

      _event_id     unique per emitted event; lets a true duplicate be told
                    apart from a legitimate re-send
      _batch_date   which file the row came from; the key the reload deletes on
      _source_file  the exact staged file, from Snowflake's METADATA$FILENAME
      _loaded_at    when this pipeline wrote the row, not when the source made it
    """
    _check_entity(entity)
    _check_identifier(database, "database")
    _check_identifier(schema, "schema")
    return f"""
create table if not exists {database}.{schema}.{entity} (
    _record      variant       not null,
    _event_id    string        not null,
    _batch_date  date          not null,
    _source_file string        not null,
    _loaded_at   timestamp_ltz not null
)
comment = 'RAW {entity}: as received. No cleaning, typing or deduplication.'
""".strip()


def put_sql(local_path: Path, stage: str, entity: str, overwrite: bool = True) -> str:
    """Upload one local file to the stage.

    The path is resolved to an absolute one, because PUT does not accept a
    relative path: file://data/orders/x.parquet fails, while
    file://C:/Users/me/project/data/orders/x.parquet works.

    as_posix() matters on Windows. PUT wants forward slashes, so
    C:\\Users\\me\\data becomes C:/Users/me/data.
    Quote the URI so spaces in directory names remain part of the path.

    auto_compress is off because Parquet is already compressed; gzipping it
    again costs time and makes the staged file harder to inspect.
    """
    _check_entity(entity)
    absolute = local_path.expanduser().resolve()
    file_uri = f"file://{absolute.as_posix()}".replace("'", "''")
    return (
        f"put '{file_uri}' @{stage}/{entity}/ "
        f"auto_compress = false overwrite = {str(overwrite).lower()}"
    )


def delete_batch_sql(database: str, schema: str, entity: str, batch_date: date) -> str:
    """Remove a batch so it can be loaded again.

    This is half of what makes the load idempotent. Snowflake's own COPY load
    history is the obvious alternative, but it is not good enough here: it
    expires after 64 days, and it skips a file whose name it has seen even if
    the contents changed. Regenerating a day from the simulator produces the
    same filename with different bytes, so load history would quietly ignore it.
    """
    _check_entity(entity)
    _check_identifier(database, "database")
    _check_identifier(schema, "schema")
    return (
        f"delete from {database}.{schema}.{entity} "
        f"where _batch_date = '{batch_date.isoformat()}'::date"
    )


def copy_into_sql(
    database: str,
    schema: str,
    entity: str,
    stage: str,
    file_format: str,
    batch_date: date,
    file_name: str,
) -> str:
    """Load one staged Parquet file into a RAW table.

    `$1` is the entire Parquet row as a VARIANT object, which is why the whole
    record lands in one column and a new source column can never break this
    statement.

    The batch date is bound from the caller rather than read out of the file.
    The loader already knows which day it is loading, and binding it avoids
    depending on how pyarrow chose to encode a date column.

    force = true is required, not optional. The DELETE above has already
    removed the batch, but Snowflake still has this filename in its load
    history and would skip it, leaving the table short by one day. Taking
    responsibility for idempotency means opting out of its bookkeeping.

    on_error = abort_statement fails the whole file rather than skipping bad
    rows. A pipeline built to surface defects must not silently drop them.
    """
    _check_entity(entity)
    _check_identifier(database, "database")
    _check_identifier(schema, "schema")
    _check_identifier(stage, "stage")
    _check_identifier(file_format, "file format")
    return f"""
copy into {database}.{schema}.{entity}
    (_record, _event_id, _batch_date, _source_file, _loaded_at)
from (
    select
        $1,
        $1:_event_id::string,
        '{batch_date.isoformat()}'::date,
        metadata$filename,
        current_timestamp()
    from @{stage}/{entity}/{file_name}
)
file_format = (format_name = '{file_format}')
on_error = 'abort_statement'
force = true
""".strip()


def batch_counts_sql(database: str, schema: str, entity: str) -> str:
    """Row counts per batch date, for verifying a load."""
    _check_entity(entity)
    _check_identifier(database, "database")
    _check_identifier(schema, "schema")
    return f"""
select
    _batch_date,
    count(*)                        as rows_loaded,
    count(distinct _event_id)       as distinct_events,
    max(_loaded_at)                 as last_loaded_at
from {database}.{schema}.{entity}
group by _batch_date
order by _batch_date
""".strip()


def total_count_sql(database: str, schema: str, entity: str) -> str:
    _check_entity(entity)
    _check_identifier(database, "database")
    _check_identifier(schema, "schema")
    return f"select count(*) from {database}.{schema}.{entity}"


def discover_files(data_dir: Path, batch_date: date, file_format: str = "parquet") -> dict[str, Path]:
    """Find one day's file for each entity.

    Returns only files that exist. A missing entity is reported by the caller
    rather than raised here, because a partial day is a real situation worth
    seeing in the log.
    """
    found: dict[str, Path] = {}
    for entity in ENTITIES:
        candidate = data_dir / entity / f"{batch_date.isoformat()}.{file_format}"
        if candidate.is_file():
            found[entity] = candidate
    return found
