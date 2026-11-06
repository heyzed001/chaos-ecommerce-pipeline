"""Run reporting SQL against fixtures, independently of Snowflake."""

import sqlite3
from pathlib import Path

import pytest
import sqlglot
from jinja2 import Environment, StrictUndefined


ROOT = Path(__file__).resolve().parents[1]


def environment():
    env = Environment(undefined=StrictUndefined)
    env.globals.update(ref=lambda name: name, config=lambda **kwargs: "")
    helper = env.from_string((ROOT / "macros/revenue_summary_columns.sql").read_text()).module
    env.globals["revenue_summary_columns"] = helper.revenue_summary_columns
    return env


def sql_file(path):
    rendered = environment().from_string((ROOT / path).read_text()).render()
    return sqlglot.transpile(rendered, read="snowflake", write="sqlite")[0]


@pytest.fixture
def data():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    defaults = {
        "order_item_id": "I1", "order_id": "O1", "customer_key": "OLD",
        "order_date_key": 20261101, "order_date": "2026-11-01", "order_currency": "NGN",
        "quantity": 1, "unit_price": 100, "discount_amount": 0, "line_total": 100,
        "unit_cost": 60, "cost_amount": 60, "is_quantity_invalid": 0,
        "is_order_missing": 0, "is_revenue_eligible": 1, "is_customer_history_missing": 0,
    }
    columns = ", ".join(
        f"{k} {'numeric' if isinstance(v, int) else 'text'}" for k, v in defaults.items()
    )
    conn.execute(f"create table fct_order_items ({columns})")
    conn.executescript("""
        create table dim_customer (customer_key text, country text, region text);
        insert into dim_customer values ('OLD', 'Nigeria', 'Lagos'), ('NEW', 'Nigeria', 'Abuja FCT');
    """)
    conn.execute("create view revenue_line_items as " + sql_file("models/marts/revenue_line_items.sql"))

    def insert(**changes):
        row = dict(defaults, **changes)
        conn.execute(
            "insert into fct_order_items values (" + ",".join("?" for _ in row) + ")",
            list(row.values()),
        )

    yield conn, insert
    conn.close()


def summary(conn, regional=False):
    name = "mart_daily_region_revenue" if regional else "mart_daily_revenue"
    return [dict(r) for r in conn.execute(sql_file(f"models/marts/{name}.sql"))]


def test_discounts_costs_and_distinct_orders(data):
    conn, insert = data
    insert(quantity=2, discount_amount=10, line_total=190, cost_amount=120)
    insert(order_item_id="I2", line_total=100)
    row = summary(conn)[0]
    assert row["observed_item_count"] == 2
    assert row["order_count"] == 1
    assert row["units_sold"] == 3
    assert row["gross_sales_amount"] == 300
    assert row["discount_amount"] == 10
    assert row["revenue_amount"] == 290
    assert row["cost_amount"] == 180
    assert row["margin_amount"] == 110


def test_excluded_statuses_and_invalid_lines_do_not_contribute(data):
    conn, insert = data
    insert()
    insert(order_item_id="cancelled", is_revenue_eligible=0)
    insert(order_item_id="returned", is_revenue_eligible=0)
    insert(order_item_id="negative", quantity=-1, line_total=-100, is_quantity_invalid=1)
    insert(order_item_id="broken_line", line_total=999)
    row = summary(conn)[0]
    assert row["revenue_amount"] == 100
    assert row["eligible_item_count"] == 1
    assert row["excluded_status_item_count"] == 2
    assert row["invalid_item_count"] == 2


def test_missing_cost_retains_revenue_but_not_partial_margin(data):
    conn, insert = data
    insert()
    insert(order_item_id="missing_cost", unit_cost=None, cost_amount=None)
    row = summary(conn)[0]
    assert row["revenue_amount"] == 200
    assert row["cost_amount"] is None
    assert row["margin_amount"] is None
    assert row["missing_or_invalid_cost_item_count"] == 1


def test_regions_use_customer_version_keys_and_keep_unknown_geography(data):
    conn, insert = data
    insert(customer_key="OLD")
    insert(order_item_id="I2", customer_key="NEW")
    insert(order_item_id="I3", customer_key=None, is_customer_history_missing=1)
    rows = {r["customer_region"]: r for r in summary(conn, regional=True)}
    assert set(rows) == {"Lagos", "Abuja FCT", None}
    assert all(r["revenue_amount"] == 100 for r in rows.values())
    assert sum(r["revenue_amount"] for r in rows.values()) == summary(conn)[0]["revenue_amount"]


def test_currencies_are_never_combined(data):
    conn, insert = data
    insert(order_currency="NGN")
    insert(order_item_id="I2", order_currency="USD")
    assert {r["order_currency"]: r["revenue_amount"] for r in summary(conn)} == {"NGN": 100, "USD": 100}


def test_warehouse_reconciliation_sql_accepts_valid_summaries(data):
    conn, insert = data
    insert()
    insert(order_item_id="I2", customer_key="NEW", unit_cost=None, cost_amount=None)
    for name in ["mart_daily_revenue", "mart_daily_region_revenue"]:
        conn.execute(f"create table {name} as " + sql_file(f"models/marts/{name}.sql"))
    assert conn.execute(sql_file("tests/revenue_region_matches_daily.sql")).fetchall() == []
    assert conn.execute(sql_file("tests/revenue_summary_arithmetic.sql")).fetchall() == []
    conn.execute("update mart_daily_region_revenue set revenue_amount = revenue_amount + 1")
    assert conn.execute(sql_file("tests/revenue_region_matches_daily.sql")).fetchall()


def test_actual_customer_history_join_switches_at_valid_from():
    env = environment()
    helpers = env.from_string((ROOT / "macros/source_types.sql").read_text()).module
    env.globals.update(source_timestamp=helpers.source_timestamp, var=lambda name, default=None: default)
    rendered = env.from_string((ROOT / "models/intermediate/int_order_items_enriched.sql").read_text()).render()
    tree = sqlglot.parse_one(rendered, read="snowflake")
    join = next(j for j in tree.find_all(sqlglot.exp.Join) if j.this.alias_or_name == "h")
    query = sqlglot.parse_one("select o.order_id, h.customer_version_id from stg_orders o")
    query = query.join(join.this, on=join.args["on"], join_type="left")
    conn = sqlite3.connect(":memory:")
    try:
        conn.executescript("""
            create table stg_orders (order_id text, customer_id text, order_placed_at text);
            insert into stg_orders values
                ('before', 'C1', '2026-11-04 01:59:59'),
                ('boundary', 'C1', '2026-11-04 02:00:00'),
                ('after', 'C1', '2026-11-05 12:00:00');
            create table int_customer_history
                (customer_id text, customer_version_id text, valid_from text, valid_to text, is_current integer);
            insert into int_customer_history values
                ('C1', 'OLD', '2026-10-01 02:00:00', '2026-11-04 02:00:00', 0),
                ('C1', 'NEW', '2026-11-04 02:00:00', null, 1);
        """)
        rows = conn.execute(query.sql(dialect="sqlite")).fetchall()
        assert dict(rows) == {"before": "OLD", "boundary": "NEW", "after": "NEW"}
        assert len(rows) == 3
    finally:
        conn.close()
