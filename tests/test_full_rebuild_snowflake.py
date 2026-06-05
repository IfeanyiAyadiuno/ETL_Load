"""Full rebuild refreshes Snowflake for the full CDA lifespan, not only ~18 months."""

from datetime import date
from unittest.mock import MagicMock, patch

import prodview_date_bounds as pdb
from production_update import _RebuildProgress


def test_rebuild_progress_cda_sales_midpoint():
    seen = []

    progress = _RebuildProgress(seen.append)
    progress.phase_part("cda_sales", 72, 181)
    assert seen[-1] == 41


def test_rebuild_progress_does_not_hit_ninety_nine_early():
    seen = []

    progress = _RebuildProgress(seen.append)
    for i in range(50):
        progress.emit(i)
    progress.phase_part("cda_sales", 72, 181)
    assert seen[-1] < 50


def test_full_rebuild_snowflake_range_uses_earliest_query_start():
    end = date(2026, 4, 17)
    assert pdb.full_rebuild_snowflake_range(date(2018, 3, 1), end) == (
        date(2018, 3, 1),
        end,
    )


def test_full_rebuild_snowflake_range_empty_cda_falls_back_to_rolling_start():
    with patch.object(pdb, "_today", return_value=date(2026, 4, 19)):
        end = date(2026, 4, 17)
        start, out_end = pdb.full_rebuild_snowflake_range(None, end)
        assert out_end == end
        assert start == pdb.quick_update_start_date(end)


def test_main_always_calls_full_lifespan_snowflake_refresh():
    """Even when CDA max equals end_cap, Full Rebuild must refresh Snowflake."""
    end_cap = date(2026, 4, 17)
    cda_min = date(2015, 1, 1)
    span = (cda_min, end_cap)

    import production_update

    with patch.object(pdb, "_today", return_value=date(2026, 4, 19)):
        with patch("prodview_update_gui.query_pce_cda_min_date", return_value=cda_min):
            with patch(
                "prodview_update_gui.query_pce_cda_max_date",
                return_value=end_cap,
            ):
                with patch(
                    "prodview_update_gui.refresh_full_rebuild_cda",
                    return_value=(span[0], span[1], 100),
                ) as mock_refresh:
                    with patch.object(
                        production_update,
                        "_refresh_cda_sales_from_allocation_factors",
                        return_value=True,
                    ):
                        with patch.object(
                            production_update, "clear_pce_production", return_value=0
                        ):
                            mock_conn = MagicMock()
                            mock_conn.__enter__.return_value.cursor.return_value.rowcount = 0
                            with patch.object(
                                production_update, "get_sql_conn", return_value=mock_conn
                            ):
                                with patch.object(
                                    production_update,
                                    "fetch_well_mapping",
                                    return_value=({}, {}),
                                ):
                                    with patch.object(
                                        production_update,
                                        "fetch_cda_data",
                                        return_value=__import__("pandas").DataFrame(),
                                    ):
                                        production_update.main()

    mock_refresh.assert_called_once()
    assert mock_refresh.call_args.kwargs.get("log_callback") is print
