"""Chaos injection: the deliberate defects the pipeline must survive.

Each decision is driven by an entity's own deterministic RNG, so the same
defect lands on the same record every time a day is regenerated. Every firing
is recorded in a ChaosLog that gets written next to the batch, which gives
Phase 11 ground truth to check the pipeline's own detection against.
"""

from __future__ import annotations

import random
from collections import Counter
from dataclasses import dataclass, field
from datetime import date

from .config import SimulatorConfig, money

#: What each chaos type breaks, and which pipeline phase is meant to handle it.
CHAOS_TYPES = {
    "duplicate_in_file": "Same order twice in one file. Handled by dedup in staging (Phase 6).",
    "duplicate_next_day": "Order re-emitted in the next file. Handled by the MERGE key (Phase 8).",
    "late_arrival": "Order lands in a later file than its event date. Needs a lookback window (Phase 8).",
    "null_email": "Customer has no email. Staging decides filter, default or flag (Phase 6).",
    "null_phone": "Customer has no phone. Tolerated, not an error.",
    "negative_quantity": "Impossible quantity. Must fail a test before reaching marts (Phase 9).",
    "broken_total": "Header total disagrees with its lines. Caught by a custom test (Phase 9).",
    "schema_drift": "A new column appears mid-history. Ingestion must not break (Phase 4).",
}


@dataclass
class ChaosLog:
    """Counts of every defect injected into one batch."""

    batch_date: date
    counts: Counter = field(default_factory=Counter)
    affected_keys: dict[str, list[str]] = field(default_factory=dict)

    def record(self, kind: str, key: str) -> None:
        self.counts[kind] += 1
        # Keep a sample rather than every key, so the log stays readable.
        sample = self.affected_keys.setdefault(kind, [])
        if len(sample) < 25:
            sample.append(key)

    def to_dict(self) -> dict:
        return {
            "batch_date": self.batch_date.isoformat(),
            "counts": dict(sorted(self.counts.items())),
            "sample_keys": self.affected_keys,
            "legend": {k: v for k, v in CHAOS_TYPES.items() if k in self.counts},
        }


def decide_delay(rng: random.Random, cfg: SimulatorConfig) -> int:
    """How many days late this order arrives. 0 means on time.

    Skewed towards short delays, because that is how real feeds behave: most
    stragglers are a day behind, a few are much worse.
    """
    if rng.random() >= cfg.late_arrival_rate:
        return 0
    weights = [0.0] + [1.0 / day for day in range(1, cfg.max_late_days + 1)]
    days = list(range(cfg.max_late_days + 1))
    return rng.choices(days[1:], weights=weights[1:], k=1)[0]


def decide_duplicates(rng: random.Random, cfg: SimulatorConfig) -> tuple[bool, bool]:
    """Return (appears twice in its file, re-appears in the next file).

    Two distinct failure modes. The first is caught by deduplication inside a
    batch; the second only by a MERGE on a stable key across batches.
    """
    return rng.random() < cfg.duplicate_rate, rng.random() < cfg.duplicate_rate


def maybe_negative_quantity(
    rng: random.Random, cfg: SimulatorConfig, quantity: int
) -> tuple[int, bool]:
    if rng.random() < cfg.negative_quantity_rate:
        return -quantity, True
    return quantity, False


def maybe_break_total(
    rng: random.Random, cfg: SimulatorConfig, true_total: float
) -> tuple[float, bool]:
    """Report a header total that disagrees with the sum of the lines."""
    if rng.random() >= cfg.broken_total_rate:
        return money(true_total), False
    drift = rng.choice([0.5, 0.9, 1.1, 1.5, 2.0])
    return money(true_total * drift), True


def drift_active(cfg: SimulatorConfig, batch_date: date) -> bool:
    """True once the source has started sending the extra column."""
    return cfg.schema_drift_date is not None and batch_date >= cfg.schema_drift_date
