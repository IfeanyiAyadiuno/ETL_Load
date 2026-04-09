"""
Accumap UWI audit vs PCE_WM: prints matched and unmatched rows to stdout (terminal).

Uses the same Accumap sheet / filters / UWI rules as Public Sales (sales_allocation_updates).
Accumap file path defaults to **Accumap Template** in settings.ini (same as the GUI).

Run without opening the main window:

  python production_update_gui.py --accumap-unmatched --month "Aug 2025"

Override the Excel path:

  python production_update_gui.py --accumap-unmatched -m "Aug 2025" -a "I:/other/Accumap.xlsx"

Or use the thin script (optional -a; otherwise settings.ini):

  python scripts/accumap_unmatched_uwis.py -m "Aug 2025"
"""

from __future__ import annotations

import argparse
import configparser
import csv
import os
import sys
from datetime import datetime

from app_paths import get_settings_path
from db_connection import get_sql_conn
from sales_allocation_updates import (
    fetch_pce_uwi_to_well_name,
    map_accumap_uwi_to_well_sales,
    read_accumap_sales_by_uwi,
)


def accumap_path_from_settings() -> str:
    """PATHS/accumap_template from settings.ini next to the app (or repo)."""
    ini = get_settings_path()
    if not os.path.isfile(ini):
        return ""
    cfg = configparser.ConfigParser()
    cfg.read(ini)
    return cfg.get("PATHS", "accumap_template", fallback="").strip()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description=(
            "Print Accumap UWIs matched to PCE_WM wells and unmatched UWIs "
            "(terminal output; uses settings.ini Accumap path unless -a is set)."
        ),
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
        default=None,
        help="Accumap Excel path (default: Accumap Template from settings.ini).",
    )
    p.add_argument(
        "-o",
        "--output",
        help=(
            "Optional CSV path: columns match_status, accumap_uwi, pce_well_name, "
            "prd_monthly_gas_e3m3 (matched|unmatched)."
        ),
    )
    args = p.parse_args(argv)

    try:
        month_dt = datetime.strptime(args.month.strip(), "%b %Y")
    except ValueError:
        print('Invalid --month; use "MMM YYYY" (e.g. Aug 2025).', file=sys.stderr)
        return 2

    accumap = (args.accumap or "").strip() or accumap_path_from_settings()
    if not accumap:
        print(
            "No Accumap path: set Accumap Template in Settings (settings.ini) or pass --accumap.",
            file=sys.stderr,
        )
        return 2
    if not os.path.isfile(accumap):
        print(f"Accumap file not found: {accumap}", file=sys.stderr)
        return 2

    conn = get_sql_conn()
    try:
        cursor = conn.cursor()
        pce_uwi_dict = fetch_pce_uwi_to_well_name(cursor)
    finally:
        conn.close()

    try:
        accumap_by_uwi = read_accumap_sales_by_uwi(accumap, month_dt)
    except Exception as e:
        print(f"Failed to read Accumap: {e}", file=sys.stderr)
        return 1

    _well_sales, unmatched, matched_rows = map_accumap_uwi_to_well_sales(
        accumap_by_uwi, pce_uwi_dict
    )
    unmatched_sorted = sorted(set(unmatched), key=lambda s: (len(s), s))
    matched_sorted = sorted(matched_rows, key=lambda t: (t[1], t[0]))
    n_matched_uwi = len(matched_sorted)
    n_unmatched = len(unmatched_sorted)
    n_distinct_wells = len({t[1] for t in matched_sorted})

    print(f"Accumap file: {accumap}")
    print(
        f"Month {args.month}: {len(accumap_by_uwi)} UWIs in Accumap; "
        f"{n_matched_uwi} matched to {n_distinct_wells} PCE_WM well(s); "
        f"{n_unmatched} unmatched."
    )
    print()
    print("--- Matched (Accumap UWI -> PCE_WM [Well Name]) ---")
    print("accumap_uwi\tpce_well_name\tprd_monthly_gas_e3m3")
    for uwi, well, gas in matched_sorted:
        print(f"{uwi}\t{well}\t{gas}")

    print()
    print("--- Unmatched (no PCE_WM [Value Navigator UWI] match) ---")
    print("accumap_uwi\tprd_monthly_gas_e3m3")
    for uwi in unmatched_sorted:
        gas = accumap_by_uwi.get(uwi, 0.0)
        print(f"{uwi}\t{gas}")

    if args.output:
        with open(args.output, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(
                ["match_status", "accumap_uwi", "pce_well_name", "prd_monthly_gas_e3m3"]
            )
            for uwi, well, gas in matched_sorted:
                w.writerow(["matched", uwi, well, gas])
            for uwi in unmatched_sorted:
                w.writerow(
                    ["unmatched", uwi, "", accumap_by_uwi.get(uwi, "")],
                )
        print(
            f"Wrote {n_matched_uwi + n_unmatched} rows to {args.output}",
            file=sys.stderr,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
