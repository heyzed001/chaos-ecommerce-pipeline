"""CLI entry point: write one day's source files, or backfill a range.

    python -m simulator.generate --date 2026-11-03
    python -m simulator.generate --start 2026-10-05 --end 2026-11-03
    python -m simulator.generate --date 2026-11-03 --chaos-free
    python -m simulator.generate --date 2026-11-03 --schema-drift-date 2026-10-25
    python -m simulator.generate --date 2026-11-03 --duplicate-rate 0.25

Output layout, one file per entity per day:

    data/customers/2026-11-03.parquet
    data/products/2026-11-03.parquet
    data/orders/2026-11-03.parquet
    data/order_items/2026-11-03.parquet
    data/_chaos_log/2026-11-03.json
"""

from __future__ import annotations

import argparse
import json
from dataclasses import replace
from datetime import date, datetime, time, timedelta
from pathlib import Path

import pandas as pd

from .chaos import ChaosLog, drift_active
from .config import SimulatorConfig, rng_for, stable_id
from .generators import (
    build_customer_master,
    build_product_master,
    collect_batch,
    customer_snapshot,
    product_snapshot,
)

ENTITIES = ("customers", "products", "orders", "order_items")

ORDER_COLUMNS = [
    "order_id", "customer_id", "order_placed_at", "order_status", "order_currency",
    "shipping_cost", "order_total", "payment_method", "updated_at",
]

ITEM_COLUMNS = [
    "order_item_id", "order_id", "product_id", "line_number",
    "quantity", "unit_price", "unit_cost", "discount_amount", "line_total",
]

META_COLUMNS = ["_event_id", "_emitted_at", "_batch_date"]


def _emitted_at(cfg: SimulatorConfig, batch_date: date, key: str) -> datetime:
    """A deterministic emission timestamp.

    Deliberately not datetime.now(): a wall-clock value would make two runs of
    the same day differ, and idempotency would be impossible to assert.
    """
    offset = rng_for(cfg.master_seed, "emit", key, batch_date.isoformat()).randint(0, 5400)
    return datetime.combine(batch_date, time(1, 0)) + timedelta(seconds=offset)


def _stamp(row: dict, cfg: SimulatorConfig, batch_date: date, key: str) -> dict:
    row["_event_id"] = stable_id(key, batch_date.isoformat())
    row["_emitted_at"] = _emitted_at(cfg, batch_date, key)
    row["_batch_date"] = batch_date
    return row


def build_frames(
    cfg: SimulatorConfig,
    batch_date: date,
    customers: list[dict],
    products: list[dict],
) -> tuple[dict[str, pd.DataFrame], ChaosLog]:
    """Build one day's four frames plus the chaos log for that day."""
    log = ChaosLog(batch_date=batch_date)

    customer_rows = []
    for customer in customers:
        snapshot = customer_snapshot(customer, batch_date)
        if snapshot is None:
            continue
        if snapshot["email"] is None:
            log.record("null_email", snapshot["customer_id"])
        if snapshot["phone"] is None:
            log.record("null_phone", snapshot["customer_id"])
        customer_rows.append(
            _stamp(snapshot, cfg, batch_date, f"customer:{snapshot['customer_id']}")
        )

    product_rows = [
        _stamp(product_snapshot(p, batch_date), cfg, batch_date, f"product:{p['product_id']}")
        for p in products
    ]

    order_rows: list[dict] = []
    item_rows: list[dict] = []
    drift_on = drift_active(cfg, batch_date)

    for order, occurrence in collect_batch(cfg, batch_date, customers, products, log):
        for copy_index in range(order["copies_in_file"]):
            key = f"order:{order['order_id']}:{occurrence}:{copy_index}"
            row = {column: order.get(column) for column in ORDER_COLUMNS}
            row["updated_at"] = order["order_placed_at"]
            if drift_on:
                row[cfg.schema_drift_column] = order["loyalty_tier"]
            order_rows.append(_stamp(row, cfg, batch_date, key))

            for item in order["items"]:
                item_row = {column: item[column] for column in ITEM_COLUMNS}
                item_key = f"item:{item['order_item_id']}:{occurrence}:{copy_index}"
                item_rows.append(_stamp(item_row, cfg, batch_date, item_key))

    if drift_on:
        log.record("schema_drift", cfg.schema_drift_column)

    frames = {
        "customers": pd.DataFrame(customer_rows),
        "products": pd.DataFrame(product_rows),
        "orders": pd.DataFrame(order_rows),
        "order_items": pd.DataFrame(item_rows),
    }
    return frames, log


def write_batch(
    cfg: SimulatorConfig,
    batch_date: date,
    out_dir: Path,
    customers: list[dict],
    products: list[dict],
    file_format: str = "parquet",
) -> dict[str, int]:
    frames, log = build_frames(cfg, batch_date, customers, products)

    for name, frame in frames.items():
        target = out_dir / name
        target.mkdir(parents=True, exist_ok=True)
        path = target / f"{batch_date.isoformat()}.{file_format}"
        if file_format == "parquet":
            frame.to_parquet(path, index=False)
        else:
            frame.to_csv(path, index=False)

    log_dir = out_dir / "_chaos_log"
    log_dir.mkdir(parents=True, exist_ok=True)
    (log_dir / f"{batch_date.isoformat()}.json").write_text(
        json.dumps(log.to_dict(), indent=2, default=str)
    )

    counts = {name: len(frame) for name, frame in frames.items()}
    counts["chaos_events"] = sum(log.counts.values())
    return counts


def _config_from_args(args: argparse.Namespace) -> SimulatorConfig:
    cfg = SimulatorConfig(master_seed=args.seed)
    if args.chaos_free:
        cfg = cfg.without_chaos()

    overrides = {}
    for field_name in (
        "duplicate_rate", "late_arrival_rate", "negative_quantity_rate", "broken_total_rate"
    ):
        value = getattr(args, field_name)
        if value is not None:
            overrides[field_name] = value
    if args.schema_drift_date:
        overrides["schema_drift_date"] = date.fromisoformat(args.schema_drift_date)
    if args.customers:
        overrides["n_customers"] = args.customers
    if args.products:
        overrides["n_products"] = args.products
    if args.history_anchor:
        overrides["history_anchor"] = date.fromisoformat(args.history_anchor)
    if args.history_days:
        overrides["history_days"] = args.history_days

    return replace(cfg, **overrides) if overrides else cfg


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate chaotic e-commerce source data.")
    window = parser.add_mutually_exclusive_group(required=True)
    window.add_argument("--date", help="Single batch date, YYYY-MM-DD")
    window.add_argument("--start", help="First batch date of a backfill, YYYY-MM-DD")
    parser.add_argument("--end", help="Last batch date of a backfill, YYYY-MM-DD")

    parser.add_argument("--out-dir", default="data", help="Output root (default: data)")
    parser.add_argument("--format", default="parquet", choices=["parquet", "csv"])
    parser.add_argument("--seed", type=int, default=42)

    parser.add_argument("--chaos-free", action="store_true", help="Clean baseline run")
    parser.add_argument("--duplicate-rate", type=float, default=None)
    parser.add_argument("--late-arrival-rate", type=float, default=None)
    parser.add_argument("--negative-quantity-rate", type=float, default=None)
    parser.add_argument("--broken-total-rate", type=float, default=None)
    parser.add_argument("--schema-drift-date", default=None, help="Add the extra column from this date")
    parser.add_argument("--customers", type=int, default=None)
    parser.add_argument("--products", type=int, default=None)
    parser.add_argument(
        "--history-anchor",
        default=None,
        help="Last day of your intended history. Customer moves cluster before it. "
        "Set once and keep it constant.",
    )
    parser.add_argument("--history-days", type=int, default=None)

    args = parser.parse_args(argv)
    if args.start and not args.end:
        parser.error("--start requires --end")
    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    cfg = _config_from_args(args)

    if args.date:
        batch_dates = [date.fromisoformat(args.date)]
    else:
        start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
        if end < start:
            raise SystemExit("--end must not precede --start")
        batch_dates = [start + timedelta(days=offset) for offset in range((end - start).days + 1)]

    out_dir = Path(args.out_dir)
    customers = build_customer_master(cfg)
    products = build_product_master(cfg)

    totals: dict[str, int] = {}
    for batch_date in batch_dates:
        counts = write_batch(cfg, batch_date, out_dir, customers, products, args.format)
        for key, value in counts.items():
            totals[key] = totals.get(key, 0) + value
        print(
            f"{batch_date}  "
            + "  ".join(f"{name}={counts[name]}" for name in ENTITIES)
            + f"  chaos={counts['chaos_events']}"
        )

    if len(batch_dates) > 1:
        print("-" * 72)
        print(
            f"{len(batch_dates)} days  "
            + "  ".join(f"{name}={totals[name]}" for name in ENTITIES)
            + f"  chaos={totals['chaos_events']}"
        )
    print(f"written to {out_dir.resolve()}")


if __name__ == "__main__":
    main()
