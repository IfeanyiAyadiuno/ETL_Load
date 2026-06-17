"""Schema parity across PCE_Production insert definitions."""

from pce_production_schema import (
    PCE_PRODUCTION_INSERT_COLUMNS,
    PCE_PRODUCTION_TC_EXTRA_COLUMNS,
    production_insert_columns,
)
from production_update import _INSERT_COLS


def test_production_update_insert_cols_match_schema():
    assert list(PCE_PRODUCTION_INSERT_COLUMNS) == _INSERT_COLS


def test_tc_columns_are_gathered_minus_uwi_plus_remarks():
    tc_cols = production_insert_columns(
        include_uwi=False,
        extra_columns=PCE_PRODUCTION_TC_EXTRA_COLUMNS,
    )
    gathered = list(PCE_PRODUCTION_INSERT_COLUMNS)
    gathered_no_uwi = [c for c in gathered if c != "UWI"]
    assert list(tc_cols) == gathered_no_uwi + ["Remarks"]
