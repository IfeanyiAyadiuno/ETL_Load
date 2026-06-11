"""Gathered production pad `` PRD`` suffix from Well Master."""

import pandas as pd
from unittest.mock import MagicMock

from production_update import (
    PRODUCTION_PAD_SUFFIX,
    apply_pad_name_from_well_master,
    production_pad_name_from_wm,
    production_pad_sql_from_wm,
    sync_production_wm_metadata_from_wm_sql,
)


def test_production_pad_name_from_wm_appends_suffix():
    assert production_pad_name_from_wm("15-12") == "15-12 PRD"


def test_production_pad_name_from_wm_idempotent():
    assert production_pad_name_from_wm("15-12 PRD") == "15-12 PRD"


def test_production_pad_name_from_wm_blank():
    assert production_pad_name_from_wm(None) is None
    assert production_pad_name_from_wm("") is None
    assert production_pad_name_from_wm("   ") is None


def test_apply_pad_name_from_well_master_suffix():
    df = pd.DataFrame({"Well Name": ["W-1", "W-2"], "Pad Name": [None, "old"]})
    out = apply_pad_name_from_well_master(df, pad_lookup={"W-1": "15-12", "W-2": "9-3 PRD"})
    assert out.loc[0, "Pad Name"] == "15-12 PRD"
    assert out.loc[1, "Pad Name"] == "9-3 PRD"


def test_sync_production_wm_metadata_pad_sql_uses_prd_suffix():
    cursor = MagicMock()
    sync_production_wm_metadata_from_wm_sql(cursor, update_pad=True, update_enersight=False, update_month=False)

    sql = cursor.execute.call_args[0][0]
    assert " PRD" in sql
    assert production_pad_sql_from_wm("ca.pad") in sql


def test_production_pad_suffix_constant():
    assert PRODUCTION_PAD_SUFFIX == " PRD"
