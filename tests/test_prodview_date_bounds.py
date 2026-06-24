"""Calendar bounds for Prodview Snowflake mode (`run_quick_update`) and full-rebuild caps."""

from datetime import date
from unittest.mock import patch

import prodview_date_bounds as pdb


def test_prodview_effective_end_date_respects_lag():
    with patch.object(pdb, "_today", return_value=date(2026, 4, 19)):
        assert pdb.prodview_effective_end_date() == date(2026, 4, 17)
        assert pdb.prodview_effective_end_date(0) == date(2026, 4, 19)
        assert pdb.prodview_effective_end_date(5) == date(2026, 4, 14)


def test_quick_update_date_range_12_months():
    with patch.object(pdb, "_today", return_value=date(2026, 4, 19)):
        s, e = pdb.quick_update_date_range()
        assert e == date(2026, 4, 17)
        assert s == date(2025, 4, 17)


def test_snowflake_cda_gap_range_skips_when_current():
    end = date(2026, 5, 25)
    assert pdb.snowflake_cda_gap_range(date(2026, 5, 25), end) is None
    assert pdb.snowflake_cda_gap_range(date(2026, 5, 26), end) is None


def test_snowflake_cda_gap_range_incremental():
    end = date(2026, 5, 25)
    assert pdb.snowflake_cda_gap_range(date(2026, 5, 20), end) == (
        date(2026, 5, 21),
        date(2026, 5, 25),
    )


def test_snowflake_cda_gap_range_empty_cda_uses_rolling_window():
    end = date(2026, 5, 25)
    start, out_end = pdb.snowflake_cda_gap_range(None, end)
    assert out_end == end
    assert start == pdb.quick_update_start_date(end)


def test_gathered_water_backfill_when_gas_present_water_missing():
    end = date(2026, 5, 25)
    out = pdb.gathered_water_backfill_range(100, 0, end)
    assert out == (pdb.quick_update_start_date(end), end)


def test_gathered_water_backfill_skipped_when_water_present():
    end = date(2026, 5, 25)
    assert pdb.gathered_water_backfill_range(100, 5, end) is None


def test_merge_inclusive_date_ranges():
    a = (date(2026, 5, 1), date(2026, 5, 10))
    b = (date(2026, 4, 1), date(2026, 5, 25))
    assert pdb.merge_inclusive_date_ranges(a, b) == (date(2026, 4, 1), date(2026, 5, 25))


def test_rolling_window_snowflake_range_matches_quick_update():
    with patch.object(pdb, "_today", return_value=date(2026, 4, 19)):
        assert pdb.rolling_window_snowflake_range() == pdb.quick_update_date_range()
