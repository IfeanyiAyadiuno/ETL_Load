"""Spine uses per-well first production, not global MIN(ProdDate)."""

from datetime import date

import pandas as pd

from prodview_update_gui import _build_spine_per_well_starts


def test_spine_starts_at_each_well_first_production():
    mapping = pd.DataFrame(
        {
            "Well Name": ["A", "B"],
            "GasIDREC": [1, 2],
            "PressuresIDREC": [10, 20],
            "Formation Producer": [None, None],
            "Layer Producer": [None, None],
            "Fault Block": [None, None],
            "Pad Name": [None, None],
            "Lateral Length": [None, None],
            "Orient": [None, None],
        }
    )
    first_prod = pd.Series(
        { "A": date(2020, 1, 5), "B": date(2021, 6, 1)},
    )
    end = date(2020, 1, 7)
    default = date(2010, 1, 1)

    spine = _build_spine_per_well_starts(mapping, first_prod, end, default)
    a_dates = set(spine.loc[spine["Well Name"] == "A", "ProdDate"])
    b_dates = set(spine.loc[spine["Well Name"] == "B", "ProdDate"])

    assert a_dates == {date(2020, 1, 5), date(2020, 1, 6), date(2020, 1, 7)}
    assert b_dates == set()  # first prod after end → no rows
