#!/usr/bin/env python3
"""
Standalone CLI — not imported or launched by the Qt GUI.

List Accumap UWIs that do not map to PCE_WM for a calendar month. Uses the same
logic as Public Sales (sheet Sales Gas - to PRW, month filter, UWI cleanup,
PCE_WM matching in sales_allocation_updates).

From the repository root:
  python scripts/accumap_unmatched_uwis.py --month "Aug 2025" --accumap "I:/path/Accumap.xlsx"
  python scripts/accumap_unmatched_uwis.py -m "Aug 2025" -a "I:/path/Accumap.xlsx" -o unmatched.csv

From elsewhere (script adds repo root to sys.path):
  python /path/to/ETL_Load/scripts/accumap_unmatched_uwis.py -m "Aug 2025" -a "..."
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_rp = str(_REPO_ROOT)
if _rp not in sys.path:
    sys.path.insert(0, _rp)

from db_connection import get_sql_conn
from sales_allocation_updates import (
    fetch_pce_uwi_to_well_name,
    map_accumap_uwi_to_well_sales,
    read_accumap_sales_by_uwi,
)


def main() -> int:
    p = argparse.ArgumentParser(
        description="CLI: list Accumap UWIs with no PCE_WM match (not part of the GUI app)."
    )
    p.add_argument(
        "-m",
        "--month",
        required=True,
        help='Calendar month (e.g. "Aug 2025") — must match Accumap row dates.',
    )
    p.add_argument(
        "-a",
        "--accumap",
        required=True,
        help="Path to Public Data Accumap Excel file.",
    )
    p.add_argument(
        "-o",
        "--output",
        help="Optional CSV path (columns: uwi, prd_monthly_gas_e3m3).",
    )
    args = p.parse_args()

    try:
        month_dt = datetime.strptime(args.month.strip(), "%b %Y")
    except ValueError:
        print('Invalid --month; use "MMM YYYY" (e.g. Aug 2025).', file=sys.stderr)
        return 2

    conn = get_sql_conn()
    try:
        cursor = conn.cursor()
        pce_uwi_dict = fetch_pce_uwi_to_well_name(cursor)
    finally:
        conn.close()

    try:
        accumap_by_uwi = read_accumap_sales_by_uwi(args.accumap, month_dt)
    except Exception as e:
        print(f"Failed to read Accumap: {e}", file=sys.stderr)
        return 1

    _well_sales, unmatched = map_accumap_uwi_to_well_sales(accumap_by_uwi, pce_uwi_dict)
    unmatched_sorted = sorted(set(unmatched), key=lambda s: (len(s), s))

    print(
        f"Month {args.month}: {len(accumap_by_uwi)} UWIs in Accumap, "
        f"{len(unmatched_sorted)} unmatched to PCE_WM."
    )
    for uwi in unmatched_sorted:
        gas = accumap_by_uwi.get(uwi, 0.0)
        print(f"{uwi}\t{gas}")

    if args.output:
        with open(args.output, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["uwi", "prd_monthly_gas_e3m3"])
            for uwi in unmatched_sorted:
                w.writerow([uwi, accumap_by_uwi.get(uwi, "")])
        print(f"Wrote {args.output}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
