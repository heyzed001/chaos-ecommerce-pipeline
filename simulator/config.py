"""Simulator configuration and determinism helpers."""

from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass, replace
from datetime import date


@dataclass(frozen=True)
class SimulatorConfig:
    """All knobs in one place.

    Rates are probabilities between 0.0 and 1.0. Raising one is how Phase 11
    stress-tests the pipeline, so nothing here is hard-coded further down.
    """

    # --- determinism -------------------------------------------------------
    master_seed: int = 42

    # --- volumes -----------------------------------------------------------
    n_customers: int = 500
    n_products: int = 200
    orders_per_day_min: int = 40
    orders_per_day_max: int = 90
    max_lines_per_order: int = 4

    # --- the history window you intend to generate -------------------------
    # Customer moves and repricing are concentrated inside this window so that
    # SCD Type 2 has visible changes in the data you actually load. Spread over
    # the whole master timeline instead, a 30-day history would contain almost
    # no movers and dim_customer would look pointless.
    #
    # Keep these constant once chosen. Changing them relocates every move, so
    # a daily run and a backfill would disagree and idempotency would break.
    history_anchor: date = date(2026, 11, 3)
    history_days: int = 30

    # --- legitimate business change (NOT chaos) ----------------------------
    # Customers moving house is real behaviour, not a defect. It is the reason
    # dim_customer is SCD Type 2, so it stays switched on even in clean runs.
    customer_mover_share: float = 0.15
    max_moves_per_customer: int = 2
    product_repricing_share: float = 0.20

    # --- chaos -------------------------------------------------------------
    duplicate_rate: float = 0.03
    late_arrival_rate: float = 0.05
    max_late_days: int = 7
    null_email_rate: float = 0.02
    null_phone_rate: float = 0.08
    negative_quantity_rate: float = 0.01
    broken_total_rate: float = 0.02

    # Schema drift is off by default. Phase 11 switches it on deliberately so
    # the failure is observed rather than absorbed unnoticed.
    schema_drift_date: date | None = None
    schema_drift_column: str = "loyalty_tier"

    def without_chaos(self) -> SimulatorConfig:
        """A clean baseline.

        Useful for proving the pipeline produces correct numbers before any
        defects are introduced. Customer moves and repricing survive, because
        those are business change rather than data quality problems.
        """
        return replace(
            self,
            duplicate_rate=0.0,
            late_arrival_rate=0.0,
            null_email_rate=0.0,
            null_phone_rate=0.0,
            negative_quantity_rate=0.0,
            broken_total_rate=0.0,
            schema_drift_date=None,
        )


def rng_for(seed: int, *parts: object) -> random.Random:
    """Return a deterministic RNG bound to one logical entity.

    rng_for(42, "order", "ORD-20261101-00007") always yields the same stream,
    whatever order days are generated in and however many times a day is
    regenerated. Using a single global RNG instead would make a day's output
    depend on what was generated before it, and reruns would not match.
    """
    key = "|".join(str(p) for p in parts)
    digest = hashlib.sha256(f"{seed}|{key}".encode()).hexdigest()
    return random.Random(int(digest[:16], 16))


def stable_id(*parts: object) -> str:
    """A deterministic 32-character hex id, used for _event_id."""
    key = "|".join(str(p) for p in parts)
    return hashlib.sha256(key.encode()).hexdigest()[:32]


def money(value: float) -> float:
    """Round to 2 decimal places.

    Stored as float64 in the files; staging casts to NUMBER(12,2) in Snowflake.
    """
    return round(value, 2)
