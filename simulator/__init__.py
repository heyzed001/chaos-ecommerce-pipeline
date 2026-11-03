"""Chaos simulator for the e-commerce pipeline.

Generates deliberately messy source data: duplicates, late arrivals, nulls,
bad values, broken totals and schema drift. Every value is derived from the
master seed plus the entity's own identity, so any single day can be
regenerated in isolation and come out identical. That property is what makes
the pipeline's idempotency testable in Phase 10.
"""

from .config import SimulatorConfig

__all__ = ["SimulatorConfig"]
