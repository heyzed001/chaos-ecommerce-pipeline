"""Offline checks; these never connect to Snowflake or execute snapshots."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
import sqlite3

import pytest
import sqlglot
from jinja2 import Environment, StrictUndefined


ROOT = Path(__file__).resolve().parents[1]


def fail(message):
    raise RuntimeError(message)


@pytest.mark.parametrize(
    "input_counts,existing,backwards,error",
    [
        ((0, 0, 0), False, 0, "No customer rows"),
        ((2, 1, 0), False, 0, "missing keys/timestamps"),
        ((2, 0, 1), False, 0, "duplicate customers"),
        ((2, 0, 0), True, 1, "backwards"),
        ((2, 0, 0), True, 0, None),
        ((2, 0, 0), False, 0, None),
        ((2, None, 0), False, 0, None),
        ((2, None, 0), True, 0, None),
    ],
)
def test_snapshot_guard(input_counts, existing, backwards, error):
    text = (ROOT / "macros/customer_snapshot.sql").read_text()
    guard = "{% macro validate_customer_snapshot_input() %}" + text.split(
        "{% macro validate_customer_snapshot_input() %}", 1
    )[1]
    query = Mock(side_effect=[
        SimpleNamespace(rows=[input_counts]),
        SimpleNamespace(rows=[(backwards,)]),
    ])
    env = Environment(undefined=StrictUndefined)
    env.globals.update({
        "execute": True,
        "customer_snapshot_rows": lambda: "select * from snapshot_input",
        "run_query": query,
        "adapter": SimpleNamespace(get_relation=lambda **kwargs: "history" if existing else None),
        "this": SimpleNamespace(database="ECOMMERCE", schema="SNAPSHOTS", identifier="SNAP_CUSTOMERS"),
        "exceptions": SimpleNamespace(raise_compiler_error=fail),
        "return": lambda value: value,
    })
    snapshot_guard = env.from_string(guard).module.validate_customer_snapshot_input
    if error:
        with pytest.raises(RuntimeError, match=error):
            snapshot_guard()
    else:
        assert snapshot_guard().strip() == "select 1"
    # Missing/invalid input is rejected before inspecting existing versions.
    valid_input = input_counts[0] > 0 and not (input_counts[1] or 0) and not (input_counts[2] or 0)
    assert query.call_count == (2 if existing and valid_input else 1)


def test_customer_history_check_rejects_no_current_version():
    class SnowflakeCountIf:
        def __init__(self):
            self.total = 0

        def step(self, value):
            self.total += int(bool(value))

        def finalize(self):
            # Reproduce Snowflake's NULL result when no condition is satisfied.
            return self.total or None

    env = Environment(undefined=StrictUndefined)
    env.globals["ref"] = lambda name: name
    sql = env.from_string((ROOT / "tests/customer_history_one_current.sql").read_text()).render()
    conn = sqlite3.connect(":memory:")
    try:
        conn.create_aggregate("count_if", 1, SnowflakeCountIf)
        conn.executescript("""
            create table int_customer_history (customer_id text, is_current integer);
            insert into int_customer_history values
                ('correct', 0), ('correct', 1),
                ('all_closed', 0), ('all_closed', 0),
                ('two_current', 1), ('two_current', 1);
        """)
        assert {row[0] for row in conn.execute(sql)} == {"all_closed", "two_current"}
    finally:
        conn.close()


@pytest.mark.parametrize("batch_date", [None, "2026-10-05"])
def test_new_models_and_history_tests_render_as_snowflake_sql(batch_date):
    env = Environment(undefined=StrictUndefined)
    types = env.from_string((ROOT / "macros/source_types.sql").read_text()).module
    env.globals.update(
        source_date=types.source_date,
        source_timestamp=types.source_timestamp,
        source=lambda schema, name: f"ECOMMERCE.RAW.{name}",
        ref=lambda name: f"ECOMMERCE.STAGING.{name}",
        config=lambda **kwargs: "",
        var=lambda name, default=None: default,
        customer_snapshot_date=lambda: batch_date,
        date_dimension_bounds=lambda: {
            "start_date": "2024-01-01", "end_date": "2028-12-31", "day_count": 1827
        },
    )
    text = (ROOT / "macros/customer_snapshot.sql").read_text()
    rows = "{% macro customer_snapshot_rows() %}" + text.split(
        "{% macro customer_snapshot_rows() %}", 1
    )[1].split("{% macro validate_customer_snapshot_input()", 1)[0]
    env.globals["customer_snapshot_rows"] = env.from_string(rows).module.customer_snapshot_rows
    paths = (
        list((ROOT / "models/intermediate").glob("*.sql"))
        + list((ROOT / "tests").glob("*.sql"))
        + list((ROOT / "analyses").glob("*.sql"))
    )
    for path in paths:
        rendered = env.from_string(path.read_text()).render()
        sqlglot.parse_one(rendered, read="snowflake")
        if path.stem == "int_customer_snapshot_input":
            assert ("where _batch_date =" in rendered) == (batch_date is not None)
            if batch_date:
                assert f"where _batch_date = '{batch_date}'::date" in rendered
