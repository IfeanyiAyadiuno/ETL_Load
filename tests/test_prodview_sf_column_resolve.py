"""Snowflake alloc column resolution for gathered water."""

import pandas as pd

from prodview_update_gui import _prepare_sf_df


def test_prepare_sf_df_reads_gathered_water_from_volprod_alias():
    raw = pd.DataFrame(
        {
            "IDRECCOMP": ["100"],
            "PRODDATE": ["2024-02-01"],
            "VOLPRODGATHWATER": [12.5],
        }
    )
    out = _prepare_sf_df(raw, "IDRECCOMP", "PRODDATE", ["Gathered_Water_Production"])
    assert float(out["Gathered_Water_Production"].iloc[0]) == 12.5


def test_prepare_sf_df_reads_gathered_water_from_gathered_water_alias():
    raw = pd.DataFrame(
        {
            "PRESSURESIDREC": ["100"],
            "PRODDATE": ["2024-02-01"],
            "GATHERED_WATER": [3.0],
        }
    )
    out = _prepare_sf_df(raw, "PRESSURESIDREC", "PRODDATE", ["Gathered_Water_Production"])
    assert float(out["Gathered_Water_Production"].iloc[0]) == 3.0
