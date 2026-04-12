#!/usr/bin/env python3
"""
Peek PCE_Production for a well identified by PCE_WM GasIDREC and PressuresIDREC.

Not attached to the GUI. Uses db_connection (.env / Trusted Connection).

Usage:
  python gas_idrec_production_peek.py <GasIDREC> <PressuresIDREC>
  python gas_idrec_production_peek.py <GasIDREC> --pressures-null
  python gas_idrec_production_peek.py <GasIDREC> <PressuresIDREC> --limit 100

If multiple wells match, the script lists them and exits (no guess).
"""

from __future__ import annotations

import argparse
import sys

import pandas as pd

from db_connection import SQL_DATABASE, SQL_SERVER, get_sql_conn


def _norm_id(v: str | None) -> str:
    if v is None:
        return ""
    return str(v).strip()


def fetch_wm_rows(cursor, gas_idrec: str, pressures_idrec: str | None, pressures_null: bool):
    gas = _norm_id(gas_idrec)
    if pressures_null:
        cursor.execute(
            """
            SELECT
                [Well Name],
                [Composite Name],
                [Exception],
                GasIDREC,
                PressuresIDREC
            FROM PCE_WM
            WHERE LTRIM(RTRIM(CAST(GasIDREC AS NVARCHAR(200)))) = ?
              AND PressuresIDREC IS NULL
            """,
            gas,
        )
    else:
        pres = _norm_id(pressures_idrec or "")
        cursor.execute(
            """
            SELECT
                [Well Name],
                [Composite Name],
                [Exception],
                GasIDREC,
                PressuresIDREC
            FROM PCE_WM
            WHERE LTRIM(RTRIM(CAST(GasIDREC AS NVARCHAR(200)))) = ?
              AND LTRIM(RTRIM(CAST(PressuresIDREC AS NVARCHAR(200)))) = ?
            """,
            gas,
            pres,
        )
    return cursor.fetchall()


def production_well_name(well_name, composite_name) -> str:
    """Match production_update.apply_well_names: composite when non-empty, else well name."""
    if composite_name is not None and str(composite_name).strip():
        return str(composite_name).strip()
    return str(well_name).strip()


def main() -> int:
    p = argparse.ArgumentParser(
        description="Print first N rows from PCE_Production for a GasIDREC / PressuresIDREC pair."
    )
    p.add_argument("gas_idrec", help="GasIDREC value as stored in PCE_WM (string form is fine)")
    p.add_argument(
        "pressures_idrec",
        nargs="?",
        default=None,
        help="PressuresIDREC (omit with --pressures-null if that column is NULL in PCE_WM)",
    )
    p.add_argument(
        "--pressures-null",
        action="store_true",
        help="Match rows where PressuresIDREC IS NULL",
    )
    p.add_argument("--limit", type=int, default=50, help="Max rows to return (default 50)")
    args = p.parse_args()

    if args.pressures_null and args.pressures_idrec:
        print("Use either --pressures-null or a PressuresIDREC value, not both.", file=sys.stderr)
        return 2

    if not args.pressures_null and args.pressures_idrec is None:
        print(
            "Provide PressuresIDREC as the second argument, or use --pressures-null.",
            file=sys.stderr,
        )
        return 2

    try:
        conn = get_sql_conn()
    except Exception as e:
        print(str(e), file=sys.stderr)
        return 1

    try:
        cursor = conn.cursor()
        rows = fetch_wm_rows(
            cursor,
            args.gas_idrec,
            args.pressures_idrec,
            args.pressures_null,
        )
    finally:
        conn.close()

    if not rows:
        print(
            "No PCE_WM row matched this GasIDREC"
            + (" (PressuresIDREC IS NULL)." if args.pressures_null else f" / PressuresIDREC.")
        )
        return 1

    if len(rows) > 1:
        print(f"Multiple PCE_WM rows matched ({len(rows)}). Resolve duplicates in master data.\n")
        for i, r in enumerate(rows, 1):
            wn, comp, exc, g, pr = r
            print(f"  [{i}] Well Name={wn!r} Composite={comp!r} Exception={exc!r}")
            print(f"       GasIDREC={g!r} PressuresIDREC={pr!r}")
        return 1

    well_name, composite_name, exception, gas_db, pres_db = rows[0]
    prod_name = production_well_name(well_name, composite_name)

    print(f"Server: {SQL_SERVER}  Database: {SQL_DATABASE}")
    print(f"PCE_WM: Well Name={well_name!r}  Composite Name={composite_name!r}")
    print(f"        GasIDREC={gas_db!r}  PressuresIDREC={pres_db!r}  Exception={exception!r}")
    print(f"PCE_Production filter: [Well Name] = {prod_name!r}")
    print(f"TOP {args.limit} rows, ordered by [Date]\n")

    query = f"""
        SELECT TOP ({int(args.limit)})
            *
        FROM PCE_Production
        WHERE [Well Name] = ?
        ORDER BY [Date]
    """

    try:
        conn = get_sql_conn()
        df = pd.read_sql(query, conn, params=[prod_name])
    finally:
        conn.close()

    if df.empty:
        # Same fallback as test_well_lookup: try raw Well Name if composite missed
        if prod_name != str(well_name).strip():
            conn = get_sql_conn()
            try:
                df = pd.read_sql(query, conn, params=[str(well_name).strip()])
            finally:
                conn.close()
            if not df.empty:
                print(f"(Using PCE_WM [Well Name] instead of composite: {well_name!r})\n")

    if df.empty:
        print("No rows in PCE_Production for that well key.")
        return 1

    # Wide table; avoid truncation of middle columns
    with pd.option_context("display.max_columns", None, "display.width", 240, "display.max_colwidth", 32):
        print(df.to_string(index=False))
    print(f"\nRows shown: {len(df)}  Columns: {len(df.columns)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
