"""Tests for the chaos simulator.

The important ones are determinism and the chaos assertions: if the simulator
is not reproducible, nothing downstream can prove idempotency, and if the
chaos never fires there is nothing for the pipeline to survive.
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import pytest

from simulator.config import SimulatorConfig
from simulator.generate import build_frames
from simulator.generators import (
    build_customer_master,
    build_product_master,
    customer_snapshot,
)

BATCH_DATE = date(2026, 11, 3)
WINDOW = [BATCH_DATE - timedelta(days=offset) for offset in range(14)]


@pytest.fixture(scope="module")
def cfg() -> SimulatorConfig:
    return SimulatorConfig(n_customers=200, n_products=60)


@pytest.fixture(scope="module")
def masters(cfg):
    return build_customer_master(cfg), build_product_master(cfg)


def frames_for(cfg, masters, batch_date=BATCH_DATE):
    customers, products = masters
    frames, log = build_frames(cfg, batch_date, customers, products)
    return frames, log


def test_generation_is_deterministic(cfg, masters):
    """Same seed and date must produce identical output, every time."""
    first, _ = frames_for(cfg, masters)
    second, _ = frames_for(cfg, masters)
    for name in first:
        pd.testing.assert_frame_equal(first[name], second[name])


def test_masters_are_rebuilt_identically(cfg):
    """Rebuilding the master population must not change anybody."""
    a = build_customer_master(cfg)
    b = build_customer_master(cfg)
    assert [c["customer_id"] for c in a] == [c["customer_id"] for c in b]
    assert [c["email"] for c in a] == [c["email"] for c in b]


def test_metadata_columns_present(cfg, masters):
    frames, _ = frames_for(cfg, masters)
    for name, frame in frames.items():
        assert {"_event_id", "_emitted_at", "_batch_date"} <= set(frame.columns), name
        assert (frame["_batch_date"] == BATCH_DATE).all(), name


def test_duplicates_appear(cfg, masters):
    """Some order must arrive more than once in a single file."""
    frames, _ = frames_for(cfg, masters)
    counts = frames["orders"]["order_id"].value_counts()
    assert (counts > 1).any()
    # Duplicated rows are distinct events, so _event_id must still be unique.
    assert frames["orders"]["_event_id"].is_unique


def test_late_arrivals_appear(cfg, masters):
    """Some order in today's file must have been placed on an earlier day."""
    frames, _ = frames_for(cfg, masters)
    placed = pd.to_datetime(frames["orders"]["order_placed_at"]).dt.date
    assert (placed < BATCH_DATE).any()


def test_bad_values_appear(cfg, masters):
    frames, _ = frames_for(cfg, masters)
    assert (frames["order_items"]["quantity"] < 0).any()
    assert frames["customers"]["email"].isna().any()


def test_unit_cost_is_carried_on_the_line(cfg, masters):
    """Margin must be computable from the fact alone.

    unit_cost has to live on the order line, because dim_product is SCD Type 1
    and overwrites cost_price. Deriving margin from the dimension instead would
    restate last month's profit every time a product is repriced.
    """
    frames, _ = frames_for(cfg, masters)
    items = frames["order_items"]
    assert "unit_cost" in items.columns
    assert (items["unit_cost"] > 0).all()
    assert (items["unit_cost"] < items["unit_price"]).all()


def test_broken_totals_appear(cfg, masters):
    """At least one header total must disagree with the sum of its lines."""
    frames, _ = frames_for(cfg, masters)
    orders = frames["orders"].drop_duplicates(subset="order_id")
    lines = frames["order_items"].drop_duplicates(subset="order_item_id")
    derived = lines.groupby("order_id")["line_total"].sum()

    merged = orders.set_index("order_id").join(derived.rename("derived"))
    variance = (merged["order_total"] - merged["shipping_cost"] - merged["derived"]).abs()
    assert (variance > 0.01).any()


def test_chaos_log_counts_match_data(cfg, masters):
    """The log must agree with what actually landed in the files."""
    frames, log = frames_for(cfg, masters)
    negatives = int((frames["order_items"]["quantity"] < 0).sum())
    # Each defective line is duplicated when its order is, so the log is a
    # lower bound on rows but must not exceed them.
    assert 0 < log.counts["negative_quantity"] <= negatives


def test_chaos_free_run_is_clean(cfg, masters):
    """The clean baseline must contain no defects at all."""
    clean = cfg.without_chaos()
    customers, products = build_customer_master(clean), build_product_master(clean)
    frames, log = build_frames(clean, BATCH_DATE, customers, products)

    assert frames["orders"]["order_id"].is_unique
    assert (frames["order_items"]["quantity"] > 0).all()
    assert frames["customers"]["email"].notna().all()
    assert sum(log.counts.values()) == 0

    placed = pd.to_datetime(frames["orders"]["order_placed_at"]).dt.date
    assert (placed == BATCH_DATE).all()


def test_customer_moves_land_inside_the_history_window(cfg, masters):
    """Enough customers must change region within the generated history.

    This is the test that protects the SCD2 showpiece. If moves drift outside
    the window the dimension still builds, but every customer has exactly one
    version and the point-in-time join proves nothing.
    """
    customers, _ = masters
    window = [
        cfg.history_anchor - timedelta(days=offset) for offset in range(cfg.history_days + 1)
    ]

    movers = 0
    for customer in customers:
        regions = {
            snapshot["region"]
            for day in window
            if (snapshot := customer_snapshot(customer, day)) is not None
        }
        if len(regions) > 1:
            movers += 1

    assert movers >= 0.05 * len(customers), f"only {movers} movers in the window"


def test_a_move_always_changes_region(cfg, masters):
    """Consecutive versions must differ in region, or the move is invisible."""
    customers, _ = masters
    for customer in customers:
        versions = customer["versions"]
        for earlier, later in zip(versions, versions[1:]):
            assert earlier["region"] != later["region"], customer["customer_id"]


def test_updated_at_tracks_the_version_not_the_day(cfg, masters):
    """updated_at must only move when something actually changed.

    If it tracked the batch date, dbt snapshot would record a new version
    every single day for every customer.
    """
    customers, _ = masters
    customer = customers[0]
    first = customer_snapshot(customer, BATCH_DATE)
    second = customer_snapshot(customer, BATCH_DATE + timedelta(days=1))
    assert first["updated_at"] == second["updated_at"]


def test_schema_drift_adds_the_column_only_after_the_cutover(cfg, masters):
    from dataclasses import replace

    drifted = replace(cfg, schema_drift_date=BATCH_DATE)
    customers, products = masters

    before, _ = build_frames(drifted, BATCH_DATE - timedelta(days=1), customers, products)
    after, log = build_frames(drifted, BATCH_DATE, customers, products)

    assert "loyalty_tier" not in before["orders"].columns
    assert "loyalty_tier" in after["orders"].columns
    assert log.counts["schema_drift"] == 1


def test_population_grows_over_time(cfg, masters):
    """Customers appear as they sign up, rather than all on day one."""
    customers, products = masters
    early, _ = build_frames(cfg, date(2024, 3, 1), customers, products)
    late, _ = build_frames(cfg, BATCH_DATE, customers, products)
    assert len(early["customers"]) < len(late["customers"])
