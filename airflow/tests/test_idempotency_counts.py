"""Offline count-check tests; no warehouse queries or pipeline runs."""

from unittest.mock import Mock

import pytest

from ingestion.check_idempotency import collect_counts, compare_counts


def test_same_counts_pass():
    counts = {"raw.total": 100, "fact_duplicate_keys": 0, "fact_null_keys": 0}
    assert compare_counts(counts, counts.copy()) == []


@pytest.mark.parametrize("after", [{"raw.total": 101}, {"raw.total": 99}, {}])
def test_count_increase_decrease_or_missing_table_fails(after):
    assert compare_counts({"raw.total": 100}, after)


def test_existing_invalid_fact_keys_fail_even_without_count_change():
    counts = {"fact_duplicate_keys": 1, "fact_null_keys": 1}
    assert len(compare_counts(counts, counts.copy())) == 2


def test_queries_cover_raw_batch_totals_history_and_fact_keys():
    cursor = Mock()
    cursor.fetchone.side_effect = [(100, 10)] * 4 + [(25,)] * 4 + [(0, 0)]
    counts = collect_counts(cursor, "ECOMMERCE", "RAW", "2026-11-07")
    assert counts["ECOMMERCE.RAW.customers.batch"] == 10
    assert counts["ECOMMERCE.SNAPSHOTS.SNAP_CUSTOMERS.total"] == 25
    assert counts["ECOMMERCE.MARTS.FCT_ORDER_ITEMS.total"] == 25
    assert cursor.execute.call_args_list[0].args[1] == ("2026-11-07",)
    assert counts["fact_duplicate_keys"] == 0


def test_invalid_identifier_rejected_before_query():
    cursor = Mock()
    with pytest.raises(ValueError):
        collect_counts(cursor, "bad; drop table x", "RAW", "2026-11-07")
    cursor.execute.assert_not_called()
