"""Calendar bounds for Prodview Snowflake mode (`run_quick_update`) and full-rebuild caps."""

from datetime import date
from unittest.mock import patch

import prodview_date_bounds as pdb


def test_prodview_effective_end_date_respects_lag():
    with patch.object(pdb, "_today", return_value=date(2026, 4, 19)):
        assert pdb.prodview_effective_end_date() == date(2026, 4, 17)


def test_quick_update_date_range_18_months():
    with patch.object(pdb, "_today", return_value=date(2026, 4, 19)):
        s, e = pdb.quick_update_date_range()
        assert e == date(2026, 4, 17)
        assert s == date(2024, 10, 17)


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
