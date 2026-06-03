"""Full rebuild always refreshes the Prodview rolling Snowflake window."""

from datetime import date
from unittest.mock import MagicMock, patch

import prodview_date_bounds as pdb


def test_main_always_calls_rolling_window_snowflake_refresh():
    """Even when CDA max equals end_cap, Full Rebuild must not skip Snowflake."""
    end_cap = date(2026, 4, 17)
    window = (date(2024, 10, 17), end_cap)

    import production_update

    with patch.object(pdb, "_today", return_value=date(2026, 4, 19)):
        with patch(
            "prodview_update_gui.query_pce_cda_max_date",
            return_value=end_cap,
        ):
            with patch(
                "prodview_update_gui.refresh_rolling_window_cda",
                return_value=(window[0], window[1], 100),
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
