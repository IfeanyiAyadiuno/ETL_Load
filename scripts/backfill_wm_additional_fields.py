#!/usr/bin/env python3
"""
Bulk backfill PCE_WM additional fields from an Excel workbook.

Excel layout:
  Row 2 (index 1): column headers (cells may wrap one word per line)
  Row 3+ (index 2+): data
  Column A: UWI (matched to PCE_WM.[Value Navigator UWI])

  python scripts/backfill_wm_additional_fields.py path/to/file.xlsx
  python scripts/backfill_wm_additional_fields.py path/to/file.xlsx --dry-run
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from db_connection import get_sql_conn  # noqa: E402
from sales_allocation_updates import _pce_uwi_lookup_variations  # noqa: E402
from well_master_db import ADDITIONAL_FIELD_COLUMNS, WellMasterDB  # noqa: E402

HEADER_ROW_INDEX = 1
DATA_START_INDEX = 2

# Normalized Excel header -> PCE_WM python key
_EXCEL_HEADER_TO_KEY = {
    "uwi": None,
    "bottom hole latitude": "bottom_hole_latitude",
    "bottom hole longitude": "bottom_hole_longitude",
    "bottom hole utm easting (m)": "bottom_hole_utm_easting_m",
    "bottom hole utm northing (m)": "bottom_hole_utm_northing_m",
    "bottom hole utm zone": "bottom_hole_utm_zone",
    "surface hole latitude": "surface_hole_latitude",
    "surface hole longitude": "surface_hole_longitude",
    "surface hole utm easting (m)": "surface_hole_utm_easting_m",
    "surface hole utm northing (m)": "surface_hole_utm_northing_m",
    "surface hole utm northing(m)": "surface_hole_utm_northing_m",
    "surface hole utm zone": "surface_hole_utm_zone",
    "kb elevation (m)": "kb_elevation_m",
    "ground elevation (m)": "ground_elevation_m",
    "ground elevation(m)": "ground_elevation_m",
    "max true vertical depth (m)": "max_true_vertical_depth_m",
    "max true vertical depth(m)": "max_true_vertical_depth_m",
    "total depth (m)": "total_depth_m",
    "spud date": "spud_date",
    "rig release date": "rig_release_date",
    "outside diameter (mm)": "outside_diameter_mm",
    "tubing strength (mpa)": "tubing_strength_mpa",
    "tubing linear weight (kg/m)": "tubing_linear_weight_kg_m",
}


def normalize_excel_header(raw) -> str:
    """Collapse wrapped/newline headers: 'Bottom\\nHole\\nLatitude' -> 'bottom hole latitude'."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return ""
    s = " ".join(str(raw).split())
    s = re.sub(r"\s*\(\s*", " (", s)
    s = re.sub(r"\s*\)\s*", ")", s)
    return s.strip().lower()


def _build_wm_uwi_index(cursor) -> dict:
    cursor.execute(
        """
        SELECT [Well Name], [Value Navigator UWI]
        FROM PCE_WM
        WHERE [Value Navigator UWI] IS NOT NULL
          AND ([Exception] IS NULL OR [Exception] = '' OR [Exception] = 'N')
        """
    )
    index: dict = {}
    for well_name, uwi in cursor.fetchall():
        if not uwi:
            continue
        uwi_s = str(uwi).strip()
        for variant in _pce_uwi_lookup_variations(uwi_s):
            index[variant] = str(well_name).strip()
    return index


def _row_to_fields(row: pd.Series, col_map: dict) -> dict:
    out = {}
    for col_idx, key in col_map.items():
        if key is None:
            continue
        val = row.iloc[col_idx] if col_idx < len(row) else None
        if val is None or (isinstance(val, float) and pd.isna(val)):
            out[key] = ""
        else:
            out[key] = str(val).strip()
    return out


def load_excel(path: Path) -> tuple[pd.DataFrame, dict]:
    raw = pd.read_excel(path, header=None, engine="openpyxl")
    if raw.shape[0] <= HEADER_ROW_INDEX:
        raise ValueError("Excel file has no header row at row 2.")

    headers = [normalize_excel_header(raw.iloc[HEADER_ROW_INDEX, c]) for c in range(raw.shape[1])]
    col_map = {}
    uwi_col = None
    for idx, h in enumerate(headers):
        if not h:
            continue
        if h == "uwi":
            uwi_col = idx
            col_map[idx] = None
            continue
        key = _EXCEL_HEADER_TO_KEY.get(h)
        if key:
            col_map[idx] = key

    if uwi_col is None:
        raise ValueError("Excel must have a UWI column in row 2.")

    data = raw.iloc[DATA_START_INDEX:].reset_index(drop=True)
    return data, {**col_map, uwi_col: "__uwi__"}


def run_backfill(path: Path, *, dry_run: bool = False) -> dict:
    data, col_map = load_excel(path)
    uwi_col = next(k for k, v in col_map.items() if v == "__uwi__")
    field_cols = {k: v for k, v in col_map.items() if v and v != "__uwi__"}

    conn = get_sql_conn()
    try:
        cur = conn.cursor()
        uwi_index = _build_wm_uwi_index(cur)

        matched = 0
        updated = 0
        unmatched = []
        errors = []

        for _, row in data.iterrows():
            uwi_raw = row.iloc[uwi_col] if uwi_col < len(row) else None
            if uwi_raw is None or (isinstance(uwi_raw, float) and pd.isna(uwi_raw)):
                continue
            uwi_str = str(uwi_raw).strip()
            if not uwi_str:
                continue

            well_name = None
            for variant in _pce_uwi_lookup_variations(uwi_str):
                if variant in uwi_index:
                    well_name = uwi_index[variant]
                    break
            if not well_name:
                unmatched.append(uwi_str)
                continue

            matched += 1
            fields = _row_to_fields(row, field_cols)
            if dry_run:
                updated += 1
                continue

            ok, err = WellMasterDB.save_additional_fields(well_name, fields)
            if ok:
                updated += 1
            else:
                errors.append(f"{well_name} ({uwi_str}): {err}")

        if not dry_run:
            conn.commit()

        return {
            "rows_read": len(data),
            "matched": matched,
            "updated": updated,
            "unmatched": unmatched,
            "errors": errors,
        }
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill PCE_WM additional fields from Excel")
    parser.add_argument("excel_path", type=Path, help="Path to Excel workbook")
    parser.add_argument("--dry-run", action="store_true", help="Match only; do not UPDATE")
    args = parser.parse_args()

    if not args.excel_path.is_file():
        print(f"File not found: {args.excel_path}")
        return 1

    summary = run_backfill(args.excel_path, dry_run=args.dry_run)
    print(f"Rows read: {summary['rows_read']}")
    print(f"Matched to PCE_WM: {summary['matched']}")
    print(f"{'Would update' if args.dry_run else 'Updated'}: {summary['updated']}")
    if summary["unmatched"]:
        print(f"Unmatched UWIs ({len(summary['unmatched'])}):")
        for u in summary["unmatched"][:20]:
            print(f"  {u}")
        if len(summary["unmatched"]) > 20:
            print(f"  ... and {len(summary['unmatched']) - 20} more")
    if summary["errors"]:
        print("Errors:")
        for e in summary["errors"][:20]:
            print(f"  {e}")
    return 0 if not summary["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
