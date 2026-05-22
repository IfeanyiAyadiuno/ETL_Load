"""Pad Name alignment from Well Master (no database)."""

import pandas as pd

from production_update import apply_pad_name_from_well_master


def test_apply_pad_name_from_well_master_by_well_name():
    df = pd.DataFrame(
        {
            "Well Name": ["  Alpha  ", "Beta"],
            "Pad Name": ["wrong", "keep"],
        }
    )
    lookup = {"Alpha": "PAD-A", "Beta": "PAD-B"}
    out = apply_pad_name_from_well_master(df, pad_lookup=lookup)
    assert out.loc[0, "Pad Name"] == "PAD-A"
    assert out.loc[1, "Pad Name"] == "PAD-B"


def test_apply_pad_name_composite_label_gets_same_pad():
    """Production row labeled with composite string maps to WM pad."""
    df = pd.DataFrame({"Well Name": ["CompDisplay"], "Pad Name": ["x"]})
    lookup = {"WM-Key": "P1", "CompDisplay": "P1"}
    out = apply_pad_name_from_well_master(df, pad_lookup=lookup)
    assert out.loc[0, "Pad Name"] == "P1"


def test_apply_pad_name_unmatched_row_unchanged():
    df = pd.DataFrame({"Well Name": ["YE23 Foo - TC"], "Pad Name": ["should-stay"]})
    lookup = {"Other": "P1"}
    out = apply_pad_name_from_well_master(df, pad_lookup=lookup)
    assert out.loc[0, "Pad Name"] == "should-stay"


def test_apply_pad_name_wm_null_pad():
    df = pd.DataFrame({"Well Name": ["A"], "Pad Name": ["old"]})
    lookup = {"A": None}
    out = apply_pad_name_from_well_master(df, pad_lookup=lookup)
    assert out.loc[0, "Pad Name"] is None or pd.isna(out.loc[0, "Pad Name"])
