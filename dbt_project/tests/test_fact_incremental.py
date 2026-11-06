"""Exercise actual rendered fact-selection SQL in an in-memory database.

This verifies candidate selection and dimension joins, not Snowflake's MERGE
execution. Warehouse uniqueness/reconciliation tests verify the built result.
"""

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest
import sqlglot
from jinja2 import Environment, StrictUndefined


ROOT = Path(__file__).resolve().parents[1]


def fail(message):
    raise ValueError(message)


def fact_sql(incremental, lookback=8):
    env = Environment(undefined=StrictUndefined)
    env.globals.update({
        "config": lambda **kwargs: "",
        "is_incremental": lambda: incremental,
        "var": lambda name, default=None: lookback,
        "ref": lambda name: name,
        "this": "fct_order_items",
        "exceptions": SimpleNamespace(raise_compiler_error=fail),
    })
    helper = env.from_string((ROOT / "macros/fact_incremental_filter.sql").read_text()).module
    env.globals["fact_incremental_filter"] = helper.fact_incremental_filter
    rendered = env.from_string((ROOT / "models/marts/fct_order_items.sql").read_text()).render()
    return sqlglot.transpile(rendered, read="snowflake", write="sqlite")[0]


@pytest.fixture
def database():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    fields = {
        "order_item_id": "old", "order_id": "O1", "line_number": 1,
        "customer_id": "C1", "product_id": "P1", "customer_version_id": "V1",
        "order_placed_at": "2026-09-01 12:00:00", "order_date": "2026-09-01",
        "order_status": "delivered", "order_currency": "NGN", "payment_method": "card",
        "quantity": 1, "unit_price": 100, "unit_cost": 60, "discount_amount": 0,
        "line_total": 100, "gross_line_amount": 100, "line_cost": 60, "line_margin": 40,
        "revenue_amount": 100, "is_revenue_eligible": 1, "is_order_missing": 0,
        "is_product_missing": 0, "is_customer_history_missing": 0,
        "is_quantity_invalid": 0, "is_unit_cost_missing": 0,
        "_batch_date": "2026-09-01", "_event_id": "E1", "_source_file": "file.parquet",
        "_source_loaded_at": "2026-09-01 01:00:00",
    }
    columns = ", ".join(
        f"{name} {'integer' if isinstance(value, int) else 'text'}"
        for name, value in fields.items()
    )
    conn.execute(f"create table int_order_items_enriched ({columns})")
    placeholders = ",".join("?" for _ in fields)
    conn.execute(f"insert into int_order_items_enriched values ({placeholders})", list(fields.values()))
    recent = dict(fields, order_item_id="recent", order_id="O2", _batch_date="2026-11-03",
                  _source_loaded_at="2026-11-03 01:00:00")
    conn.execute(f"insert into int_order_items_enriched values ({placeholders})", list(recent.values()))
    conn.executescript("""
        create table dim_product (product_id text, product_key text);
        insert into dim_product values ('P1', 'PK1');
        create table dim_customer (customer_version_id text, customer_key text);
        insert into dim_customer values ('V1', 'CK1');
        create table dim_date (full_date text, date_key integer);
        insert into dim_date values ('2026-09-01', 20260901);
    """)
    conn.execute("create table fct_order_items as " + fact_sql(False))
    yield conn
    conn.close()


def candidates(conn, lookback=8):
    return {row["order_item_id"]: dict(row) for row in conn.execute(fact_sql(True, lookback))}


def test_unchanged_old_rows_are_not_reprocessed(database):
    assert set(candidates(database)) == {"recent"}


def test_new_backfill_outside_the_window_is_selected(database):
    database.execute("""
        insert into int_order_items_enriched
        select * from int_order_items_enriched where order_item_id = 'old'
    """)
    database.execute("""
        update int_order_items_enriched set order_item_id = 'late'
        where rowid = (select max(rowid) from int_order_items_enriched)
    """)
    assert "late" in candidates(database)


def test_old_correction_uses_load_timestamp_not_business_date(database):
    database.execute("""
        update int_order_items_enriched
        set quantity = 3, line_total = 300, _source_loaded_at = '2026-11-05 01:00:00'
        where order_item_id = 'old'
    """)
    result = candidates(database, lookback=0)
    assert result["old"]["quantity"] == 3
    assert result["old"]["line_total"] == 300


def test_history_key_repair_reprocesses_old_fact(database):
    database.execute("update dim_customer set customer_key = 'CK2'")
    assert candidates(database)["old"]["customer_key"] == "CK2"


def test_revenue_policy_change_reprocesses_old_fact(database):
    database.execute("""
        update int_order_items_enriched set is_revenue_eligible = 0, revenue_amount = 0
        where order_item_id = 'old'
    """)
    assert candidates(database)["old"]["revenue_amount"] == 0


def test_empty_existing_target_still_selects_every_item(database):
    database.execute("delete from fct_order_items")
    assert set(candidates(database)) == {"old", "recent"}


@pytest.mark.parametrize("lookback", [-1, 1.5, "8", True])
def test_invalid_lookback_is_rejected(lookback):
    with pytest.raises(ValueError, match="non-negative integer"):
        fact_sql(True, lookback)
