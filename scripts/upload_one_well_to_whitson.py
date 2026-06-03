#!/usr/bin/env python3
"""
Upload one well's production from PCE_Production to Whitson+.

  python scripts/upload_one_well_to_whitson.py --well-name "2-01-85-26W6M - T3 - PnP - S"
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# Whitson+ credentials
CLIENT = "pacificcanbriam"
CLIENT_ID = "Nburbd6T4XuAWa5322kyoMOexyTytUJg"
CLIENT_SECRET = "z0jE85oNk1sXPnf2iNMXaf_vj5vrr1-sCf6MGBRi57faXpYKRfHT60gAtY0E9DoY"
PROJECT_ID = 20

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))
from db_connection import get_sql_conn  # noqa: E402
import whitson_connect  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--well-name", required=True)
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--project-id", type=int, default=PROJECT_ID)
    parser.add_argument("--replace", action="store_true")
    args = parser.parse_args()

    well_name = args.well_name
    project_id = args.project_id
    append_only = not args.replace

    whitson = whitson_connect.WhitsonConnection(CLIENT, CLIENT_ID, CLIENT_SECRET)
    whitson.access_token = whitson.get_access_token_smart()

    sql = """
        SELECT [Date], [Gathered Gas (e³m³/d)], [Condensate WH (m³/d)],
               [Alloc. Water Rate (m³)], [Tubing Pressure (kPa)],
               [Casing Pressure (kPa)], [Choke Size]
        FROM PCE_Production
        WHERE [Well Name] = ?
    """
    params = [well_name]
    if args.start:
        sql += " AND [Date] >= ?"
        params.append(args.start)
    if args.end:
        sql += " AND [Date] <= ?"
        params.append(args.end)
    sql += " ORDER BY [Date]"

    conn = get_sql_conn()
    try:
        df = pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()

    if df.empty:
        print(f"No PCE_Production rows for {well_name!r}")
        return 1

    def nn(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        return max(0.0, float(v))

    production_payload = []
    for _, row in df.iterrows():
        d = row["Date"]
        date = (
            d.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            if isinstance(d, datetime)
            else pd.to_datetime(d).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        )
        production_payload.append(
            {
                "date": date,
                "qo_sc": nn(row["Condensate WH (m³/d)"]),
                "qg_sc": nn(row["Gathered Gas (e³m³/d)"]),
                "qw_sc": nn(row["Alloc. Water Rate (m³)"]),
                "p_wf_measured": None,
                "p_tubing": nn(row["Tubing Pressure (kPa)"]),
                "p_casing": nn(row["Casing Pressure (kPa)"]),
                "qg_gas_lift": None,
                "liquid_level": None,
                "choke_size": nn(row["Choke Size"]),
                "line_pressure": None,
            }
        )

    well_id = whitson.find_well_id_by_name(project_id, well_name)
    if not well_id:
        whitson.create_well(payload={"project_id": project_id, "name": well_name})
        well_id = whitson.find_well_id_by_name(project_id, well_name)
    if not well_id:
        print(f"Could not find or create Whitson well {well_name!r}")
        return 1

    resp = whitson.upload_production_to_well(
        well_id, production_payload, append_only=append_only
    )
    if resp.status_code < 200 or resp.status_code >= 300:
        print(f"Upload failed ({resp.status_code}): {resp.text[:1000]}")
        return 1

    print(f"Uploaded {len(production_payload)} point(s) to well id={well_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
