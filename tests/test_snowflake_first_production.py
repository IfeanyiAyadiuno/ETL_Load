"""Full rebuild first production comes from Snowflake per well, not PCE_CDA."""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd

from prodview_update_gui import (
    _first_production_by_well_from_snowflake_frames,
    refresh_full_rebuild_cda,
)


def _mapping_df():
    return pd.DataFrame(
        {
            "Well Name": ["Alpha", "Beta"],
            "GasIDREC": [100, 200],
            "PressuresIDREC": [10, 20],
            "Formation Producer": [None, None],
            "Layer Producer": [None, None],
            "Fault Block": [None, None],
            "Pad Name": [None, None],
            "Lateral Length": [None, None],
            "Orient": [None, None],
        }
    )


def test_first_production_maps_gas_and_gathered_to_earliest_per_well():
    gaswh = pd.DataFrame(
        {"GASIDREC": [100, 200], "FIRSTPRODDATE": [date(2019, 5, 1), date(2020, 1, 1)]}
    )
    alloc = pd.DataFrame(
        {"PRESSURESIDREC": [10, 20], "FIRSTPRODDATE": [date(2018, 12, 1), date(2021, 3, 1)]}
    )

    out = _first_production_by_well_from_snowflake_frames(_mapping_df(), gaswh, alloc)

    assert out["Alpha"] == date(2018, 12, 1)
    assert out["Beta"] == date(2020, 1, 1)


def test_first_production_normalizes_numeric_ids():
    mapping = _mapping_df().copy()
    mapping["GasIDREC"] = mapping["GasIDREC"].astype(object)
    mapping.loc[0, "GasIDREC"] = "100.0"
    gaswh = pd.DataFrame({"GasIDREC": [100], "FirstProdDate": [date(2017, 6, 15)]})

    out = _first_production_by_well_from_snowflake_frames(mapping, gaswh, pd.DataFrame())

    assert out["Alpha"] == date(2017, 6, 15)


def test_full_rebuild_uses_snowflake_first_production_not_cda():
    end = date(2026, 4, 17)
    first_prod = pd.Series({"Alpha": date(2015, 2, 1), "Beta": date(2016, 8, 10)})
    mapping = _mapping_df()

    mock_conn = MagicMock()
    mock_cursor = mock_conn.cursor.return_value

    with patch("prodview_update_gui.get_sql_conn", return_value=mock_conn):
        with patch(
            "prodview_update_gui._fetch_well_mapping", return_value=mapping
        ):
            with patch(
                "prodview_update_gui.fetch_first_production_by_well_from_snowflake",
                return_value=first_prod,
            ) as mock_sf_first:
                with patch(
                    "prodview_update_gui.refresh_pce_cda_from_snowflake",
                    return_value=500_000,
                ) as mock_refresh:
                    with patch(
                        "prodview_update_gui.prodview_effective_end_date",
                        return_value=end,
                    ):
                        query_start, out_end, n = refresh_full_rebuild_cda()

    mock_sf_first.assert_called_once()
    assert mock_sf_first.call_args[0][0].equals(mapping)
    assert mock_sf_first.call_args[0][1] == end

    mock_refresh.assert_called_once()
    kwargs = mock_refresh.call_args.kwargs
    assert kwargs["well_first_production_start"].equals(first_prod)
    assert kwargs["replace_entire_cda"] is True
    assert query_start == date(2015, 2, 1)
    assert out_end == end
    assert n == 500_000
