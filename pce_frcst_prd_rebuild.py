"""
Full replace of dbo.PCE_FRCST_PRD: copy PCE_Monthly_Forecasts business columns,
then append gathered production daily rows (WM-enriched) into the same column shape.

Gathered rows populate ``[UWI]`` from WM **Composite Name** when set; otherwise **Value
Navigator UWI** (forecasts unchanged).

Gathered rows use production where ``CAST([Date] AS date) <= prodview_effective_end_date()``
(same ``today - PRODVIEW_DATA_LAG_DAYS`` rule as Prodview / quick update).

See scripts/create_pce_frcst_prd.sql for DDL.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional

import log_format as lf
from db_connection import get_sql_conn
from prodview_date_bounds import PRODVIEW_DATA_LAG_DAYS, prodview_effective_end_date
from production_update import gathered_prd_month_sql_from_enersight

_LOG = print

_INSERT_FORECAST = """
INSERT INTO dbo.PCE_FRCST_PRD (
    [Date], [UWI], [CDGR_Mcf_d], [CD_Cond_bbl_d], [CD_Water_bbl_d],
    [Month], [Pad], [Fault_Block], [Enersight Well Name]
)
SELECT
      mf.[Date]
    , mf.[UWI]
    , mf.[CDGR_Mcf_d]
    , mf.[CD_Cond_bbl_d]
    , mf.[CD_Water_bbl_d]
    , mf.[Month]
    , mf.[Pad]
    , mf.[Fault_Block]
    , mf.[Enersight Well Name]
FROM dbo.PCE_Monthly_Forecasts AS mf
"""

_INSERT_GATHERED = """
INSERT INTO dbo.PCE_FRCST_PRD (
    [Date], [UWI], [CDGR_Mcf_d], [CD_Cond_bbl_d], [CD_Water_bbl_d],
    [Month], [Pad], [Fault_Block], [Enersight Well Name]
)
SELECT
      p.[Date]
    , COALESCE(
          NULLIF(LTRIM(RTRIM(CAST(ca.[Composite Name] AS NVARCHAR(4000)))), N''),
          ca.[Value Navigator UWI]
      )
    , p.[Gathered Gas (e³m³/d)]
    , p.[Gathered Condensate (m³/d)]
    , p.[Alloc. Water Rate (m³)]
    , {gathered_month}
    , ca.[Pad Name]
    , ca.[Fault Block]
    , ca.[Enersight Well Name]
FROM dbo.PCE_Production AS p
CROSS APPLY (
    SELECT TOP 1
          wm.[Value Navigator UWI]
        , wm.[Composite Name]
        , wm.[Pad Name]
        , wm.[Fault Block]
        , wm.[Enersight Well Name]
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
WHERE p.[Well Name] NOT LIKE N'% - TC'
  AND p.[Well Name] NOT LIKE N'YE2%'
  AND NULLIF(RTRIM(CAST(ca.[Value Navigator UWI] AS NVARCHAR(4000))), N'') IS NOT NULL
  AND CAST(p.[Date] AS DATE) <= ?
""".format(
    gathered_month=gathered_prd_month_sql_from_enersight("ca.[Enersight Well Name]"),
)


def _table_exists(cursor) -> bool:
    cursor.execute("SELECT OBJECT_ID(N'dbo.PCE_FRCST_PRD', N'U')")
    row = cursor.fetchone()
    return row is not None and row[0] is not None


def rebuild_pce_frcst_prd(
    *,
    log: Optional[Callable[[str], None]] = None,
    conn=None,
) -> Dict[str, Any]:
    """
    DELETE all rows from PCE_FRCST_PRD, refill from forecasts, append production rows.

    If ``conn`` is provided, uses that connection and still commits on success
    (and rolls back on failure). The connection is never closed here unless
    this function opened it. If ``conn`` is omitted, opens a connection, commits,
    and closes.
    """
    log_fn = log or _LOG
    own_conn = conn is None
    if own_conn:
        conn = get_sql_conn()

    out: Dict[str, Any] = {
        "skipped": False,
        "forecast_rows": None,
        "gathered_rows": None,
    }

    try:
        cur = conn.cursor()
        if not _table_exists(cur):
            log_fn(
                lf.warn(
                    "dbo.PCE_FRCST_PRD missing; run scripts/create_pce_frcst_prd.sql — "
                    "skipping combined forecast/production rebuild."
                )
            )
            out["skipped"] = True
            out["reason"] = "PCE_FRCST_PRD does not exist"
            return out

        eff_end = prodview_effective_end_date()
        log_fn(
            lf.step(
                "Rebuilding dbo.PCE_FRCST_PRD (forecasts + gathered production, "
                f"production through {eff_end.isoformat()} — today minus {PRODVIEW_DATA_LAG_DAYS} day(s))…"
            )
        )
        cur.execute("DELETE FROM dbo.PCE_FRCST_PRD")
        cur.execute(_INSERT_FORECAST)
        out["forecast_rows"] = cur.rowcount
        cur.execute(_INSERT_GATHERED, (eff_end,))
        out["gathered_rows"] = cur.rowcount

        conn.commit()

        log_fn(
            lf.success(
                f"PCE_FRCST_PRD: {lf.num(out['forecast_rows'] or 0)} forecast row(s), "
                f"{lf.num(out['gathered_rows'] or 0)} gathered row(s)."
            )
        )
        return out
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        if own_conn and conn is not None:
            conn.close()
