"""
Push PCE_Production daily rates to Whitson+ for all wells (imperial conversion via INI).
"""

from __future__ import annotations

import importlib.util
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

import whitson_connect
from db_connection import get_sql_conn
from prodview_date_bounds import prodview_effective_end_date
from whitson_imperial_units import (
    WhitsonImperialFactors,
    build_payload_point,
    load_whitson_imperial_factors,
)
from whitson_well_attributes import (
    WellMetadata,
    build_whitson_well_create_payload,
    fetch_well_metadata_for_whitson,
    sync_whitson_well_attributes,
)

_REPO_ROOT = Path(__file__).resolve().parent


def _load_script_credentials() -> Tuple[str, str, str, int]:
    """CLIENT, CLIENT_ID, CLIENT_SECRET, PROJECT_ID from scripts/whitson_upload.py."""
    script = _REPO_ROOT / "scripts" / "whitson_upload.py"
    spec = importlib.util.spec_from_file_location("whitson_upload", script)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load Whitson credentials from {script}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.CLIENT, mod.CLIENT_ID, mod.CLIENT_SECRET, mod.PROJECT_ID


def get_default_project_id() -> int:
    """Default Whitson+ project_id from scripts/whitson_upload.py."""
    _, _, _, project_id = _load_script_credentials()
    return int(project_id)


def make_whitson_connection() -> whitson_connect.WhitsonConnection:
    client, client_id, client_secret, _ = _load_script_credentials()
    whitson = whitson_connect.WhitsonConnection(client, client_id, client_secret)
    whitson.access_token = whitson.get_access_token_smart()
    return whitson


def effective_end_date(end_date: date, apply_prodview_cap: bool) -> date:
    if not apply_prodview_cap:
        return end_date
    cap = prodview_effective_end_date()
    return end_date if end_date <= cap else cap


def query_production_date_bounds(conn) -> Tuple[date, date]:
    """Min and max calendar dates in PCE_Production."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            MIN(CAST([Date] AS DATE)),
            MAX(CAST([Date] AS DATE))
        FROM dbo.PCE_Production
        """
    )
    row = cur.fetchone()
    if not row or row[0] is None or row[1] is None:
        raise ValueError("PCE_Production has no production dates.")
    min_d = row[0] if isinstance(row[0], date) else row[0].date()
    max_d = row[1] if isinstance(row[1], date) else row[1].date()
    return min_d, max_d


def resolve_upload_date_range(
    conn,
    *,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Tuple[date, date]:
    """Use full PCE_Production span when dates omitted."""
    min_d, max_d = query_production_date_bounds(conn)
    return (
        start_date if start_date is not None else min_d,
        end_date if end_date is not None else max_d,
    )


_WELL_EXCLUSION = """
    AND p.[Well Name] NOT LIKE N'% - TC'
    AND p.[Well Name] NOT LIKE N'YE2%'
"""

_LIST_WELLS_SQL = f"""
SELECT DISTINCT
    RTRIM(CAST(p.[Well Name] AS NVARCHAR(4000))) AS WellName,
    NULLIF(
        LTRIM(RTRIM(CAST(ca.[Value Navigator UWI] AS NVARCHAR(4000)))),
        N''
    ) AS Uwi
FROM dbo.PCE_Production AS p
OUTER APPLY (
    SELECT TOP 1 wm.[Value Navigator UWI]
    FROM dbo.PCE_WM AS wm
    WHERE (
              wm.[Well Name] = p.[Well Name]
           OR (
                  NULLIF(RTRIM(CAST(wm.[Composite Name] AS NVARCHAR(4000))), N'') IS NOT NULL
              AND wm.[Composite Name] = p.[Well Name]
              )
          )
      AND (wm.[Exception] IS NULL OR wm.[Exception] = N'' OR wm.[Exception] = N'N')
) AS ca
WHERE CAST(p.[Date] AS DATE) >= ?
  AND CAST(p.[Date] AS DATE) <= ?
{_WELL_EXCLUSION}
ORDER BY WellName
"""

_FETCH_PROD_SQL = """
SELECT
      [Date]
    , [Gathered Gas (e³m³/d)]
    , [Condensate WH (m³/d)]
    , [Gath. Water Rate (m³/d)]
    , [Tubing Pressure (kPa)]
    , [Casing Pressure (kPa)]
    , [Choke Size]
FROM dbo.PCE_Production
WHERE RTRIM(CAST([Well Name] AS NVARCHAR(4000))) = ?
  AND CAST([Date] AS DATE) >= ?
  AND CAST([Date] AS DATE) <= ?
ORDER BY [Date]
"""


def list_production_wells(
    conn,
    *,
    start: date,
    end: date,
) -> List[Tuple[str, Optional[str]]]:
    """Distinct well names in range with optional WM UWI."""
    cur = conn.cursor()
    cur.execute(_LIST_WELLS_SQL, start, end)
    return [(row[0], row[1]) for row in cur.fetchall()]


def fetch_production_for_well(
    conn,
    well_name: str,
    start: date,
    end: date,
) -> pd.DataFrame:
    return pd.read_sql(_FETCH_PROD_SQL, conn, params=[well_name, start, end])


def _date_to_iso(d: Any) -> str:
    if isinstance(d, datetime):
        return d.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return pd.to_datetime(d).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def build_whitson_payload(
    df: pd.DataFrame,
    factors: WhitsonImperialFactors,
) -> List[dict]:
    payload: List[dict] = []
    for _, row in df.iterrows():
        payload.append(
            build_payload_point(
                row,
                factors,
                date_iso=_date_to_iso(row["Date"]),
            )
        )
    return payload


def ensure_whitson_well(
    whitson: whitson_connect.WhitsonConnection,
    project_id: int,
    name: str,
    uwi_api: Optional[str],
    *,
    metadata: Optional[WellMetadata] = None,
    log_cb: Optional[Callable[[str], None]] = None,
) -> Optional[int]:
    def log(msg: str) -> None:
        if log_cb:
            log_cb(msg)

    well_id = whitson.find_well_id_by_name(project_id, name)
    if well_id:
        return well_id

    if uwi_api:
        try:
            matches = whitson.get_wells(project_id, uwi_api=uwi_api)
            well_id = whitson.get_well_id_by_uwi_api(matches, uwi_api)
            if well_id:
                log(f"  Found Whitson well id={well_id} by UWI {uwi_api!r}")
                return well_id
        except RuntimeError as exc:
            log(f"  UWI lookup failed: {exc}")

    wm = metadata or WellMetadata()
    create_payload = build_whitson_well_create_payload(
        wm,
        project_id=project_id,
        name=name,
        uwi_api=uwi_api,
    )
    whitson.create_well(payload=create_payload)
    well_id = whitson.find_well_id_by_name(project_id, name)
    if well_id:
        log(f"  Created Whitson well id={well_id} name={name!r}")
    return well_id


def push_well(
    whitson: whitson_connect.WhitsonConnection,
    conn,
    *,
    well_name: str,
    uwi_api: Optional[str],
    start: date,
    end: date,
    factors: WhitsonImperialFactors,
    project_id: int,
    append_only: bool = True,
    log_cb: Optional[Callable[[str], None]] = None,
) -> Tuple[str, int, Optional[str], bool]:
    """
    Push one well. Returns (status, point_count, error_message, attributes_synced).
    status: ok | skipped | failed
    """
    def log(msg: str) -> None:
        if log_cb:
            log_cb(msg)

    if not uwi_api:
        log(f"SKIP {well_name!r}: no Value Navigator UWI in PCE_WM")
        return "skipped", 0, "no UWI", False

    df = fetch_production_for_well(conn, well_name, start, end)
    if df.empty:
        log(f"SKIP {well_name!r}: no rows in date range")
        return "skipped", 0, "no production rows", False

    metadata = fetch_well_metadata_for_whitson(conn, well_name)
    if metadata.uwi_api and not uwi_api:
        uwi_api = metadata.uwi_api

    payload = build_whitson_payload(df, factors)
    well_id = ensure_whitson_well(
        whitson,
        project_id,
        well_name,
        uwi_api,
        metadata=metadata,
        log_cb=log_cb,
    )
    if not well_id:
        log(f"FAIL {well_name!r}: could not find or create Whitson well")
        return "failed", 0, "well not found or created", False

    attrs_synced = sync_whitson_well_attributes(
        whitson, well_id, metadata, log_cb=log_cb
    )

    resp = whitson.upload_production_to_well(
        well_id, payload, append_only=append_only
    )
    if resp.status_code < 200 or resp.status_code >= 300:
        err = (resp.text or "")[:500]
        log(f"FAIL {well_name!r}: HTTP {resp.status_code} {err}")
        return "failed", len(payload), err, attrs_synced

    log(
        f"OK {well_name!r} (UWI {uwi_api!r}) -> id={well_id}, "
        f"{len(payload)} point(s), HTTP {resp.status_code}"
    )
    return "ok", len(payload), None, attrs_synced


def push_all_wells(
    *,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    append_only: bool = True,
    apply_prodview_cap: bool = True,
    factors: Optional[WhitsonImperialFactors] = None,
    project_id: Optional[int] = None,
    log_cb: Optional[Callable[[str], None]] = None,
    progress_cb: Optional[Callable[[int, int], None]] = None,
    cancel_cb: Optional[Callable[[], bool]] = None,
) -> Dict[str, Any]:
    """
    Push all distinct production wells in [start_date, end_date].

    When dates are omitted, uses min/max dates in PCE_Production (end may be
    capped to prodview_effective_end_date when apply_prodview_cap is True).
    append_only=True appends points; new wells are created then loaded.
    """
    def log(msg: str) -> None:
        if log_cb:
            log_cb(msg)

    def cancelled() -> bool:
        return bool(cancel_cb and cancel_cb())

    _, _, _, default_project = _load_script_credentials()
    project_id = project_id if project_id is not None else default_project
    factors = factors or load_whitson_imperial_factors()

    conn = get_sql_conn()
    try:
        start_date, end_date = resolve_upload_date_range(
            conn, start_date=start_date, end_date=end_date
        )

        end = effective_end_date(end_date, apply_prodview_cap)
        if end < start_date:
            raise ValueError(
                f"Effective end date {end} is before start date {start_date}."
            )
        if apply_prodview_cap and end != end_date:
            log(
                f"End date capped to Prodview effective date: {end} "
                f"(table max {end_date})"
            )

        summary: Dict[str, Any] = {
            "ok": 0,
            "skipped": 0,
            "failed": 0,
            "attributes_synced": 0,
            "attributes_failed": 0,
            "errors": [],
            "project_id": project_id,
            "start": start_date.isoformat(),
            "end": end.isoformat(),
        }
        wells = list_production_wells(conn, start=start_date, end=end)
        total = len(wells)
        log(f"Whitson+ project_id: {project_id}")
        log(f"Wells to process: {total} ({start_date} to {end})")

        whitson = make_whitson_connection()
        if cancelled():
            log("Cancelled before upload.")
            return summary

        for i, (well_name, uwi) in enumerate(wells):
            if cancelled():
                log("Upload cancelled.")
                break
            try:
                status, _n, err, attrs_ok = push_well(
                    whitson,
                    conn,
                    well_name=well_name,
                    uwi_api=uwi,
                    start=start_date,
                    end=end,
                    factors=factors,
                    project_id=project_id,
                    append_only=append_only,
                    log_cb=log_cb,
                )
                summary[status] = summary.get(status, 0) + 1
                if status == "ok":
                    if attrs_ok:
                        summary["attributes_synced"] += 1
                    else:
                        summary["attributes_failed"] += 1
                if status == "failed" and err:
                    summary["errors"].append({"well": well_name, "error": err})
            except Exception as exc:
                summary["failed"] = summary["failed"] + 1
                msg = str(exc)
                summary["errors"].append({"well": well_name, "error": msg})
                log(f"FAIL {well_name!r}: {msg}")

            if progress_cb:
                progress_cb(i + 1, total)

        log(
            f"Done: {summary['ok']} ok, {summary['skipped']} skipped, "
            f"{summary['failed']} failed; "
            f"attributes synced {summary['attributes_synced']}, "
            f"attribute sync issues {summary['attributes_failed']}"
        )
        return summary
    finally:
        conn.close()
