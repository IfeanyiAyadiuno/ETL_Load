#!/usr/bin/env python3
"""
Spread monthly NGL Excel volumes to daily Ratio (_R) and Fraction (_F) on PCE_Production.

Prerequisites:
  1. Run scripts/add_pce_ngl_columns.sql in SSMS (UWI + 10 NGL columns + UWI backfill).

Usage:
  python scripts/ngl_daily_compare.py --excel "path/to/ngl.xlsx" --dry-run
  python scripts/ngl_daily_compare.py --excel "path/to/ngl.xlsx"
"""

import argparse
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from ngl_daily_compare import run_ngl_daily_compare  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compute NGL Ratio/Fraction daily columns from monthly Excel."
    )
    parser.add_argument("--excel", required=True, help="Path to monthly NGL Excel file")
    parser.add_argument("--sheet", default=0, help="Sheet name or index (default: first)")
    parser.add_argument(
        "--uwi-column",
        default="UWI",
        help="UWI column name in Excel header row (default: UWI)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute and log only; do not UPDATE SQL",
    )
    parser.add_argument(
        "--no-clear",
        action="store_true",
        help="Do not NULL out trial NGL columns before UPDATE",
    )
    parser.add_argument(
        "--unmatched-csv",
        default=None,
        help="Dry-run only: write unmatched Excel UWIs to this CSV path",
    )
    args = parser.parse_args()

    excel_path = Path(args.excel)
    if not excel_path.is_file():
        print(f"Excel file not found: {excel_path}")
        return 1

    sheet = int(args.sheet) if str(args.sheet).isdigit() else args.sheet

    try:
        summary = run_ngl_daily_compare(
            str(excel_path),
            sheet_name=sheet,
            uwi_column=args.uwi_column,
            dry_run=args.dry_run,
            clear_first=not args.no_clear,
            unmatched_csv=args.unmatched_csv,
            log=print,
        )
    except Exception as exc:
        print(f"Error: {exc}")
        return 1

    print("")
    print("Summary:")
    print(f"  Excel monthly rows:     {summary.excel_rows}")
    print(f"  Production rows (UWI):  {summary.prod_rows}")
    print(f"  Rows w/o UWI (hint):    {summary.prod_rows_without_uwi_hint}")
    print(f"  Excel UWIs matched:     {summary.excel_uwis_matched} of {summary.excel_uwis}")
    print(f"  Excel UWIs unmatched:   {len(summary.unmatched_excel_uwis)}")
    print(f"  Rows with Excel match:  {summary.rows_with_excel_match}")
    print(f"  Rows updated:           {summary.rows_updated}")
    if args.dry_run:
        print("  (dry run — no SQL changes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
