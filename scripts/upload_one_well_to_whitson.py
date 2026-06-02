#!/usr/bin/env python3
"""
Upload one well's daily production from PCE_Production to Whitson+.

Requires whitson_connect.py in the repo root.

Example:
  python scripts/upload_one_well_to_whitson.py --well-name "B2-01-85-26W6M - T3 - PnP - S"
  python scripts/upload_one_well_to_whitson.py --well-name "..." --start 2024-01-01 --end 2024-12-31
  python scripts/upload_one_well_to_whitson.py --well-name "..." --replace
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional

import pandas as pd

CLIENT = "pacificcanbriam"
CLIENT_ID = "Nburbd6T4XuAWa5322kyoMOexyTytUJg"
CLIENT_SECRET = "z0jE85oNk1sXPnf2iNMXaf_vj5vrr1-sCf6MGBRi57faXpYKRfHT60gAtY0E9DoY"
PROJECT_ID = 20

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from db_connection import get_sql_conn  # noqa: E402
import whitson_connect  # noqa: E402


def _safe_nonneg(val: Any) -> Optional[float]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        x = float(val)
    except (TypeError, ValueError):
        return None
    return max(0.0, x)


def _iso_date(d) -> Optional[str]:
    if d is None or (isinstance(d, float) and pd.isna(d)):
        return None
    if isinstance(d, datetime):
        dt = d
    else:
        dt = pd.to_datetime(d).to_pydatetime()
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def fetch_production(
    conn,
    well_name: str,
    start: Optional[str],
    end: Optional[str],
) -> pd.DataFrame:
    sql = """
        SELECT
            p.[Date],
            p.[Gas WH Production (10³m³)]     AS gas_wh,
            p.[Condensate WH (m³/d)]          AS cond_wh,
            p.[Alloc. Water Rate (m³)]        AS water,
            p.[Tubing Pressure (kPa)]         AS p_tubing,
            p.[Casing Pressure (kPa)]         AS p_casing,
            p.[Choke Size]                    AS choke
        FROM PCE_Production AS p
        WHERE p.[Well Name] = ?
          AND p.[Well Name] NOT LIKE '% - TC'
          AND p.[Well Name] NOT LIKE 'YE2%%'
    """
    params: List[Any] = [well_name]
    if start:
        sql += " AND p.[Date] >= ?"
        params.append(start)
    if end:
        sql += " AND p.[Date] <= ?"
        params.append(end)
    sql += " ORDER BY p.[Date]"

    cur = conn.cursor()
    cur.execute(sql, params)
    cols = [c[0] for c in cur.description]
    rows = cur.fetchall()
    if not rows:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame.from_records(rows, columns=cols)


def rows_to_whitson_payload(df: pd.DataFrame) -> List[dict]:
    out: List[dict] = []
    for _, r in df.iterrows():
        out.append(
            {
                "date": _iso_date(r["Date"]),
                "qo_sc": _safe_nonneg(r["cond_wh"]),
                "qg_sc": _safe_nonneg(r["gas_wh"]),
                "qw_sc": _safe_nonneg(r["water"]),
                "p_wf_measured": None,
                "p_tubing": _safe_nonneg(r["p_tubing"]),
                "p_casing": _safe_nonneg(r["p_casing"]),
                "qg_gas_lift": None,
                "liquid_level": None,
                "choke_size": _safe_nonneg(r["choke"]),
                "line_pressure": None,
            }
        )
    return out


def ensure_whitson_well(
    whitson: whitson_connect.WhitsonConnection,
    project_id: int,
    well_name: str,
) -> int:
    well_id = whitson.find_well_id_by_name(project_id, well_name)
    if well_id:
        print(f"Whitson well already exists (id={well_id}): {well_name!r}")
        return well_id

    resp = whitson.create_well(
        payload={"project_id": project_id, "name": well_name}
    )
    if resp.status_code < 200 or resp.status_code >= 300:
        raise RuntimeError(
            f"create_well failed ({resp.status_code}): {resp.text[:500]}"
        )

    try:
        body = resp.json()
        if isinstance(body, dict) and body.get("id"):
            well_id = int(body["id"])
            print(f"Created Whitson well (id={well_id}): {well_name!r}")
            return well_id
    except (TypeError, ValueError):
        pass

    well_id = whitson.find_well_id_by_name(project_id, well_name)
    if not well_id:
        raise RuntimeError(f"Well created but id not found for {well_name!r}")
    print(f"Created Whitson well (id={well_id}): {well_name!r}")
    return well_id


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Upload one PCE_Production well to Whitson+"
    )
    parser.add_argument(
        "--well-name",
        required=True,
        help="Exact PCE_Production.[Well Name] value",
    )
    parser.add_argument("--start", help="Start date YYYY-MM-DD (optional)")
    parser.add_argument("--end", help="End date YYYY-MM-DD (optional)")
    parser.add_argument(
        "--project-id",
        type=int,
        default=PROJECT_ID,
        help=f"Whitson project id (default: {PROJECT_ID})",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Replace matching dates (append_only=False). Default is append-only.",
    )
    args = parser.parse_args()

    well_name = args.well_name
    append_only = not args.replace

    conn = get_sql_conn()
    try:
        df = fetch_production(conn, well_name, args.start, args.end)
        if df.empty:
            print(f"No PCE_Production rows for {well_name!r}")
            return 1

        print(f"Loaded {len(df)} production row(s) for {well_name!r}")
        production_payload = rows_to_whitson_payload(df)

        whitson = whitson_connect.WhitsonConnection(CLIENT, CLIENT_ID, CLIENT_SECRET)
        whitson.access_token = whitson.get_access_token_smart()
        if not whitson.access_token:
            print("Auth failed: no access token")
            return 1

        well_id = ensure_whitson_well(whitson, args.project_id, well_name)

        resp = whitson.upload_production_to_well(
            well_id,
            production_payload,
            append_only=append_only,
        )
        if resp.status_code < 200 or resp.status_code >= 300:
            print(f"Upload failed ({resp.status_code}): {resp.text[:1000]}")
            return 1

        print(
            f"Uploaded {len(production_payload)} point(s) to Whitson well id={well_id} "
            f"(append_only={append_only})"
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
