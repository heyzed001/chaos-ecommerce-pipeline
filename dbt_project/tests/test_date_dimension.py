"""Offline calendar-boundary checks; no warehouse connection is opened."""

import datetime
import re
from pathlib import Path
from types import SimpleNamespace

import pytest
import sqlglot
from jinja2 import Environment, StrictUndefined


ROOT = Path(__file__).resolve().parents[1]


class MacroReturn(Exception):
    """Emulate dbt's return control flow without importing internal APIs."""

    def __init__(self, value):
        self.value = value


def return_value(value):
    raise MacroReturn(value)


def fail(message):
    raise ValueError(message)


def render_bounds(variables):
    env = Environment(undefined=StrictUndefined)
    env.globals.update({
        "var": lambda name, default=None: variables.get(name, default),
        "modules": SimpleNamespace(datetime=datetime, re=re),
        "exceptions": SimpleNamespace(raise_compiler_error=fail),
        "return": return_value,
    })
    macro = env.from_string((ROOT / "macros/date_dimension_bounds.sql").read_text()).module
    try:
        macro.date_dimension_bounds()
    except MacroReturn as result:
        return result.value
    raise AssertionError("Calendar macro did not return bounds")


@pytest.mark.parametrize(
    "start,end,expected",
    [
        ("2024-02-28", "2024-03-01", 3),
        ("2025-02-28", "2025-03-01", 2),
        ("2026-11-05", "2026-11-05", 1),
        ("2024-01-01", "2028-12-31", 1827),
    ],
)
def test_inclusive_calendar_bounds(start, end, expected):
    bounds = render_bounds({"dim_date_start_date": start, "dim_date_end_date": end})
    assert bounds == {"start_date": start, "end_date": end, "day_count": expected}
    env = Environment(undefined=StrictUndefined)
    env.globals["date_dimension_bounds"] = lambda: bounds
    sql = env.from_string((ROOT / "models/marts/dim_date.sql").read_text()).render()
    sqlglot.parse_one(sql, read="snowflake")
    assert f"generator(rowcount => {expected})" in sql


@pytest.mark.parametrize(
    "variables",
    [
        {"dim_date_start_date": "2026-11-06", "dim_date_end_date": "2026-11-05"},
        {"dim_date_start_date": "2025-02-29"},
        {"dim_date_end_date": "2028-12-31'; select 1"},
        {"dim_date_start_date": "2026-1-1"},
    ],
)
def test_invalid_calendar_bounds_are_rejected(variables):
    with pytest.raises(ValueError):
        render_bounds(variables)
