"""PCE_Production [UWI] populated on insert and synced from WM."""

import pandas as pd
from pathlib import Path
from unittest.mock import MagicMock

from production_update import (
    _INSERT_COLS,
    apply_uwi_from_well_master,
    insert_pce_production,
    sync_production_uwi_from_wm_sql,
)


def test_insert_cols_include_uwi():
    assert "UWI" in _INSERT_COLS
    assert _INSERT_COLS.index("UWI") == _INSERT_COLS.index("Well Name") + 1


def test_apply_uwi_from_well_master_maps_by_well_name():
    df = pd.DataFrame({"Well Name": ["W-1", "W-2"], "Date": ["2024-01-01", "2024-01-01"]})
    out = apply_uwi_from_well_master(
        df,
        uwi_lookup={"W-1": "202/a-028-I/094-B-08/0"},
    )
    assert out.loc[0, "UWI"] == "202/a-028-I/094-B-08/0"
    assert pd.isna(out.loc[1, "UWI"])


def test_insert_pce_production_sql_includes_uwi_column(monkeypatch):
    captured = {}

    class FakeCursor:
        fast_executemany = True

        def executemany(self, sql, rows):
            captured["sql"] = sql
            captured["rows"] = rows

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    monkeypatch.setattr("production_update.get_sql_conn", lambda: FakeConn())
    monkeypatch.setattr("production_update.fetch_well_master_enersight_lookup", lambda: {})

    df = pd.DataFrame(
        {
            "Date": [pd.Timestamp("2024-01-01")],
            "Days Seq": [1],
            "Day Seq UPRT": [1],
            "Well Name": ["W-1"],
            "UWI": ["202/a-028-I/094-B-08/0"],
            "Gas WH Production (10³m³)": [1.0],
            "Condensate WH (m³/d)": [0.0],
            "Gas S2 Production (10³m³)": [0.0],
            "Gas Sales Production (10³m³)": [0.0],
            "Condensate Sales (m³/d)": [0.0],
            "Gathered Gas (e³m³/d)": [0.0],
            "Gathered Condensate (m³/d)": [0.0],
            "Gath. Water Rate (m³/d)": [0.0],
            "Sales CGR (m³/e³m³)": [0.0],
            "CGR (m³/e³m³)": [0.0],
            "WGR (m³/e³m³)": [0.0],
            "ECF": [0.0],
            "Hours On": [24.0],
            "Tubing Pressure (kPa)": [0.0],
            "Casing Pressure (kPa)": [0.0],
            "Choke Size": [0.0],
            "Gas WH Cumulative Production (10³m³)": [0.0],
            "Gas S2 Cumulative Production (10³m³)": [0.0],
            "Gas Sales Cumulative Production (10³m³)": [0.0],
            "Condensate Sales Cumulative Production (m³)": [0.0],
            "Condensate WH Cumulative Production (m³)": [0.0],
            "Gas Gathered Cumulative (e³m³)": [0.0],
            "Condensate Gathered Cumulative (m³)": [0.0],
            "Gath. Water Cumulative (m³)": [0.0],
            "Formation Producer": [None],
            "Layer Producer": [None],
            "Fault Block": [None],
            "Pad Name": ["15-12 PRD"],
            "Lateral Length": [None],
            "Orientation": [None],
            "On Production Year": [2024],
            "Alloc. Water Rate (m³)": [0.0],
            "NGL (m³)": [0.0],
            "Gas WH Avg (10³m³)": [0.0],
            "Gas S2 Avg (10³m³)": [0.0],
            "Gas Gathered Avg (e³m³/d)": [0.0],
            "Condensate Gathered Avg (m³/d)": [0.0],
            "Gath. Water Avg (m³/d)": [0.0],
            "Alloc. Water Avg (m³)": [0.0],
            "Month": ["Gath PRD W-1"],
        }
    )

    insert_pce_production(df)
    assert "[UWI]" in captured["sql"]
    assert captured["rows"][0][_INSERT_COLS.index("UWI")] == "202/a-028-I/094-B-08/0"


def test_prodview_quick_insert_source_includes_uwi():
    from pce_production_schema import build_production_insert_sql

    sql = build_production_insert_sql()
    assert "[UWI]" in sql

    source = (Path(__file__).resolve().parents[1] / "prodview_update_gui.py").read_text(
        encoding="utf-8"
    )
    assert "apply_uwi_from_well_master" in source
    assert "pce_rebuild_pipeline" in source


def test_sync_production_uwi_sql_updates_existing_rows():
    cursor = MagicMock()
    sync_production_uwi_from_wm_sql(cursor)

    sql = cursor.execute.call_args[0][0]
    assert "UPDATE p" in sql
    assert "SET p.[UWI]" in sql
