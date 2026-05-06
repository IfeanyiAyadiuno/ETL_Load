"""Pure helpers from prodview_update_gui (no DB, no Snowflake)."""

from datetime import date, datetime

import pandas as pd

from prodview_update_gui import (
    _build_spine,
    _coerce_to_date,
    _merge_sf_data,
    _month_boundaries,
)


def test_coerce_to_date_from_iso_string():
    assert _coerce_to_date("2009-01-01", "start") == date(2009, 1, 1)


def test_coerce_to_date_datetime_and_plain_date():
    assert _coerce_to_date(datetime(2024, 6, 15, 12, 0), "x") == date(2024, 6, 15)
    assert _coerce_to_date(date(2024, 6, 15), "x") == date(2024, 6, 15)


def test_month_boundaries_january():
    first, last = _month_boundaries(datetime(2024, 1, 15))
    assert first == date(2024, 1, 1)
    assert last == date(2024, 1, 31)


def test_month_boundaries_december():
    first, last = _month_boundaries(datetime(2024, 12, 1))
    assert first == date(2024, 12, 1)
    assert last == date(2024, 12, 31)


def test_build_spine_row_count():
    mapping = pd.DataFrame(
        {
            "GasIDREC": [1],
            "PressuresIDREC": [2],
            "Well Name": ["W1"],
            "Formation Producer": [None],
            "Layer Producer": [None],
            "Fault Block": [None],
            "Pad Name": [None],
            "Lateral Length": [None],
            "Orient": [None],
        }
    )
    dr = [date(2024, 1, 1), date(2024, 1, 2)]
    spine = _build_spine(mapping, dr)
    assert len(spine) == 2
    assert set(spine["Well Name"]) == {"W1"}


def test_merge_sf_data_all_empty_frames():
    mapping = pd.DataFrame(
        {
            "GasIDREC": [1],
            "PressuresIDREC": [2],
            "Well Name": ["W1"],
            "Formation Producer": [None],
            "Layer Producer": [None],
            "Fault Block": [None],
            "Pad Name": [None],
            "Lateral Length": [None],
            "Orient": [None],
        }
    )
    dr = [date(2024, 1, 1)]
    spine = _build_spine(mapping, dr)
    sf_data = {
        "ecf": pd.DataFrame(),
        "gaswh": pd.DataFrame(),
        "cgr_water": pd.DataFrame(),
        "wgr": pd.DataFrame(),
        "pressures": pd.DataFrame(),
        "alloc": pd.DataFrame(),
    }
    out = _merge_sf_data(spine, sf_data)
    assert len(out) == 1
    assert "ECF_Ratio" in out.columns
