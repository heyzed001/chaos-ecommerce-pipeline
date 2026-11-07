"""Local preflight tests; Airflow and Snowflake are not imported."""

import importlib.util
from datetime import date
from pathlib import Path

import pytest


path = Path(__file__).resolve().parents[1] / "scripts" / "validate_pipeline.py"
spec = importlib.util.spec_from_file_location("pipeline_preflight", path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


@pytest.mark.parametrize("value", ["2026-11-04", "2026-11-05", "2026-11-06"])
def test_completed_daily_batches_are_allowed(value):
    assert module.validate_batch_date(value, date(2026, 11, 7)).isoformat() == value


@pytest.mark.parametrize("value", ["", "20261104", "2026-11-03", "2026-11-07", "2026-11-08", "2026-02-30", "2026-11-04; echo unsafe"])
def test_invalid_or_out_of_window_dates_are_rejected(value):
    with pytest.raises(ValueError):
        module.validate_batch_date(value, date(2026, 11, 7))
