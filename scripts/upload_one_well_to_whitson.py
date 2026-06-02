#!/usr/bin/env python3
"""
Upload one well's daily production from PCE_Production to Whitson+.

Requires whitson_connect.py in the repo root.

Example:
  python scripts/upload_one_well_to_whitson.py --well-name "B2-01-85-26W6M"
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional

import pandas as pd

# Whitson+ API credentials (edit here if they change)
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


def _search_key_from_well_name(well_name: str) -> str:
    """Use the DLS segment (e.g. 85-26W6) for a loose DB search."""
    import re

    m = re.search(r"(\d{1,2}-\d{2,3}-\d{2}W6M?)", well_name, re.I)
    if m:
        return m.group(1).replace("085", "85").replace("026", "26")
    token = well_name.split("-")[0].strip()
    return token if len(token) >= 2 else well_name[:20]


def suggest_similar_well_names(conn, well_name: str, limit: int = 15) -> List[str]:
    key = _search_key_from_well_name(well_name)
    like = f"%{key}%"
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT TOP (?) [Well Name]
        FROM PCE_Production
        WHERE [Well Name] LIKE ?
          AND [Well Name] NOT LIKE '% - TC'
          AND [Well Name] NOT LIKE 'YE2%%'
        ORDER BY [Well Name]
        """,
        limit,
        like,
    )
    return [row[0] for row in cur.fetchall() if row[0]]


def diagnose_well(conn, well_name: str) -> None:
    """Print where production data exists (or does not) for troubleshooting."""
    from db_connection import sql_target_label

    cur = conn.cursor()
    cur.execute("SELECT @@SERVERNAME, DB_NAME()")
    server, db = cur.fetchone()
    print(f"Connected: configured={sql_target_label()!r}  live={server}.{db}")

    checks = [
        (
            "PCE_Production exact [Well Name]",
            """
            SELECT COUNT(*), MIN([Date]), MAX([Date])
            FROM PCE_Production
            WHERE [Well Name] = ?
            """,
            [well_name],
        ),
        (
            "PCE_Production trimmed [Well Name]",
            """
            SELECT COUNT(*), MIN([Date]), MAX([Date])
            FROM PCE_Production
            WHERE LTRIM(RTRIM([Well Name])) = ?
            """,
            [well_name.strip()],
        ),
        (
            "PCE_Production [Enersight Well Name]",
            """
            SELECT COUNT(*), MIN([Date]), MAX([Date])
            FROM PCE_Production
            WHERE [Enersight Well Name] = ?
            """,
            [well_name],
        ),
        (
            "PCE_CDA exact [Well Name] (pre-WM mapping)",
            """
            SELECT COUNT(*), MIN(ProdDate), MAX(ProdDate)
            FROM PCE_CDA
            WHERE [Well Name] = ?
            """,
            [well_name],
        ),
        (
            "PCE_WM [Composite Name]",
            """
            SELECT COUNT(*)
            FROM PCE_WM
            WHERE [Composite Name] = ?
            """,
            [well_name],
        ),
        (
            "PCE_WM [Well Name]",
            """
            SELECT COUNT(*)
            FROM PCE_WM
            WHERE [Well Name] = ?
            """,
            [well_name],
        ),
    ]

    print(f"\nDiagnostics for well name {well_name!r} (len={len(well_name)}):")
    for label, sql, params in checks:
        cur.execute(sql, params)
        row = cur.fetchone()
        if row is None:
            print(f"  {label}: (no result)")
            continue
        if len(row) == 1:
            print(f"  {label}: {row[0]} row(s)")
        else:
            cnt, dmin, dmax = row
            print(f"  {label}: {cnt} row(s), dates {dmin} .. {dmax}")

    cur.execute(
        """
        SELECT TOP 5 [Well Name], LEN([Well Name]) AS n
        FROM PCE_Production
        WHERE [Well Name] LIKE ?
        ORDER BY [Well Name]
        """,
        f"%{_search_key_from_well_name(well_name)}%",
    )
    near = cur.fetchall()
    if near:
        print("\n  Nearby PCE_Production [Well Name] values:")
        for name, n in near:
            print(f"    {name!r} (len={n})")
    else:
        print("\n  No PCE_Production rows matching a loose DLS search.")

    cur.execute(
        """
        SELECT TOP 3
            wm.[Well Name],
            wm.[Composite Name],
            wm.[Enersight Well Name],
            wm.[Exception]
        FROM PCE_WM AS wm
        WHERE wm.[Composite Name] LIKE ?
           OR wm.[Well Name] LIKE ?
           OR wm.[Enersight Well Name] = ?
        """,
        f"%{_search_key_from_well_name(well_name)}%",
        f"%{_search_key_from_well_name(well_name)}%",
        well_name,
    )
    wm_rows = cur.fetchall()
    if wm_rows:
        print("\n  PCE_WM matches:")
        for r in wm_rows:
            print(f"    Well={r[0]!r}  Composite={r[1]!r}  Enersight={r[2]!r}  Exception={r[3]!r}")

    print(
        "\nIf PCE_CDA has rows but PCE_Production does not, run a production rebuild "
        "(ProdView Update or production_update.py)."
    )
    print(
        "If SSMS shows rows but this script does not, compare @@SERVERNAME above "
        "to the server you queried in SSMS (settings.ini overrides .env)."
    )


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
    """Map PCE_Production rows to Whitson production_data points (ARIES-style)."""
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

    payload = {"project_id": project_id, "name": well_name}
    resp = whitson.create_well(payload=payload)
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
        "--check",
        action="store_true",
        help="Diagnose where production exists (no Whitson upload)",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Replace matching dates (append_only=False). Default is append-only.",
    )
    args = parser.parse_args()

    project_id = args.project_id

    well_name = args.well_name.strip()
    append_only = not args.replace

    conn = get_sql_conn()
    try:
        if args.check:
            diagnose_well(conn, well_name)
            return 0

        df = fetch_production(conn, well_name, args.start, args.end)
        if df.empty:
            print(f"No PCE_Production rows for {well_name!r} (exact match).")
            suggestions = suggest_similar_well_names(conn, well_name)
            if suggestions:
                print("Similar [Well Name] values in PCE_Production:")
                for name in suggestions:
                    print(f"  {name}")
            else:
                print("No similar names found — check spelling or run in SSMS:")
                print(
                    "  SELECT DISTINCT TOP 20 [Well Name] FROM PCE_Production "
                    "WHERE [Well Name] LIKE '%26W6%' ORDER BY 1"
                )
            return 1

        print(f"Loaded {len(df)} production row(s) for {well_name!r}")
        production_payload = rows_to_whitson_payload(df)

        whitson = whitson_connect.WhitsonConnection(CLIENT, CLIENT_ID, CLIENT_SECRET)
        whitson.access_token = whitson.get_access_token_smart()

        well_id = ensure_whitson_well(whitson, project_id, well_name)

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
