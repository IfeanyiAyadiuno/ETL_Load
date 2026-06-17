"""Prodview Snowflake mode (`run_quick_update`) uses rolling date bounds (see prodview_date_bounds)."""

from datetime import date
from unittest.mock import MagicMock, patch

import prodview_date_bounds as pdb


def test_quick_update_window_ordering():
    with patch.object(pdb, "_today", return_value=date(2026, 1, 10)):
        s, e = pdb.quick_update_date_range()
        assert s < e


def test_refresh_rolling_window_cda_uses_data_lag_days():
    from prodview_update_gui import refresh_rolling_window_cda

    with patch.object(pdb, "_today", return_value=date(2026, 4, 19)):
        with patch(
            "prodview_update_gui.refresh_pce_cda_from_snowflake",
            return_value=0,
        ) as mock_refresh:
            start, end, n = refresh_rolling_window_cda(
                log_callback=MagicMock(),
                conn=MagicMock(),
                data_lag_days=5,
            )
            assert start == date(2024, 10, 14)
            assert end == date(2026, 4, 14)
            mock_refresh.assert_called_once_with(
                date(2024, 10, 14),
                date(2026, 4, 14),
                log_callback=mock_refresh.call_args.kwargs.get("log_callback"),
                conn=mock_refresh.call_args.kwargs.get("conn"),
                progress_callback=None,
            )
            assert n == 0
