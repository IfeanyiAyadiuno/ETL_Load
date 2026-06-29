"""Type-curve sync mirrors Gas WH into Gathered Gas on PCE_Production."""

from datetime import date

import pandas as pd

from pce_production_schema import (
    PCE_PRODUCTION_TC_EXTRA_COLUMNS,
    production_insert_columns,
)
from sync_typecurves_to_production import _tc_row_to_production_tuple


def _col_index(name: str) -> int:
    cols = production_insert_columns(
        include_uwi=False,
        extra_columns=PCE_PRODUCTION_TC_EXTRA_COLUMNS,
    )
    return cols.index(name)


def _base_row(**overrides) -> pd.Series:
    row = {
        "Well Name": "07-01-085-26W6M - TC",
        "ImportDate": date(2026, 6, 17),
        "Gas WH Production (e³m³/d)": 10.0,
        "Condensate WH (m³/d)": 2.0,
        "Gas S2 Production (10³m³)": 8.0,
        "Gas Sales Production (10³m³)": 7.0,
        "Condensate Sales (m³/d)": 1.5,
        "Sales CGR (m³/e³m³)": 0.1,
        "CGR (m³/e³m³)": 0.2,
        "Cum Gas (e³m³)": 1000.0,
        "Cum Condy (m³)": 50.0,
        "Gas WH Cumulative Production (10³m³)": 1000.0,
        "Condensate WH Cumulative Production (m³)": 50.0,
        "Gathered Gas (e³m³/d)": 10.0,
        "Gas Gathered Cumulative (e³m³)": 1000.0,
        "Gathered Condensate (m³/d)": 10.0,
        "Condensate Gathered Cumulative (m³)": 50.0,
        "Layer Producer": "Montney",
        "Pad Name": "PCE-TC-7-01",
        "Formation Producer": "Montney",
        "Fault Block": "FB1",
        "Remarks": None,
        "Lateral Length": 2500.0,
        "Orientation": "H",
        "On Production Year": 2024,
    }
    row.update(overrides)
    return pd.Series(row)


def test_tc_row_populates_gathered_from_tc_columns():
    tup = _tc_row_to_production_tuple(_base_row())
    assert tup is not None
    assert tup[_col_index("Gathered Gas (e³m³/d)")] == 10.0
    assert tup[_col_index("Gas Gathered Cumulative (e³m³)")] == 1000.0
    assert tup[_col_index("Gathered Condensate (m³/d)")] == 10.0
    assert tup[_col_index("Condensate Gathered Cumulative (m³)")] == 50.0


def test_tc_row_gathered_condensate_falls_back_to_gathered_gas_and_cond_wh_cum():
    """When condensate gathered columns are null, mirror gathered gas / condensate WH cum."""
    row = _base_row(
        **{
            "Gathered Condensate (m³/d)": None,
            "Condensate Gathered Cumulative (m³)": None,
            "Gathered Gas (e³m³/d)": 11.0,
            "Condensate WH Cumulative Production (m³)": 77.0,
        }
    )
    tup = _tc_row_to_production_tuple(row)
    assert tup is not None
    assert tup[_col_index("Gathered Condensate (m³/d)")] == 11.0
    assert tup[_col_index("Condensate Gathered Cumulative (m³)")] == 77.0


def test_tc_row_falls_back_to_wh_when_gathered_columns_null():
    row = _base_row(
        **{
            "Gathered Gas (e³m³/d)": None,
            "Gas Gathered Cumulative (e³m³)": None,
            "Gas WH Production (e³m³/d)": 12.5,
            "Gas WH Cumulative Production (10³m³)": 900.0,
        }
    )
    tup = _tc_row_to_production_tuple(row)
    assert tup is not None
    assert tup[_col_index("Gathered Gas (e³m³/d)")] == 12.5
    assert tup[_col_index("Gas Gathered Cumulative (e³m³)")] == 900.0


def test_ye2_style_wh_from_s2_mirrors_into_gathered():
    """YE2 rows store WH = S2; gathered should match that WH value."""
    row = _base_row(
        **{
            "Well Name": "YE23 McD LM NFB TC-1P",
            "Gas WH Production (e³m³/d)": 15.0,
            "Gathered Gas (e³m³/d)": 15.0,
            "Gas WH Cumulative Production (10³m³)": 500.0,
            "Gas Gathered Cumulative (e³m³)": 500.0,
        }
    )
    tup = _tc_row_to_production_tuple(row)
    assert tup is not None
    assert tup[_col_index("Gas WH Production (10³m³)")] == 15.0
    assert tup[_col_index("Gathered Gas (e³m³/d)")] == 15.0
    assert tup[_col_index("Gas Gathered Cumulative (e³m³)")] == 500.0
