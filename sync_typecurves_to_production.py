"""
Materialize dbo.PCE_TC rows into dbo.PCE_Production (one row per TC row at ImportDate).

Mapping is explicit Python-side only (no reporting views). Call after type-curve import
commits and after Prodview / full rebuild paths refresh PCE_Production.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Callable, List, Optional, Tuple

import numpy as np
import pandas as pd

import log_format as lf
from db_connection import get_sql_conn
from type_curves_import import _tc_pad_name_from_excel

_INSERT_SQL = """
INSERT INTO PCE_Production (
    [Date], [Days Seq], [Day Seq UPRT], [Well Name],
    [Gas WH Production (10³m³)], [Condensate WH (m³/d)],
    [Gas S2 Production (10³m³)], [Gas Sales Production (10³m³)],
    [Condensate Sales (m³/d)], [Gathered Gas (e³m³/d)],
    [Gathered Condensate (m³/d)], [Sales CGR (m³/e³m³)],
    [CGR (m³/e³m³)], [WGR (m³/e³m³)], [ECF],
    [Hours On], [Tubing Pressure (kPa)], [Casing Pressure (kPa)],
    [Choke Size], [Gas WH Cumulative Production (10³m³)],
    [Gas S2 Cumulative Production (10³m³)],
    [Gas Sales Cumulative Production (10³m³)],
    [Condensate Sales Cumulative Production (m³)],
    [Condensate WH Cumulative Production (m³)],
    [Gas Gathered Cumulative (e³m³)],
    [Condensate Gathered Cumulative (m³)],
    [Formation Producer], [Layer Producer], [Fault Block],
    [Pad Name], [Lateral Length], [Orientation],
    [On Production Year], [Alloc. Water Rate (m³)], [NGL (m³)],
    [Gas WH Avg (10³m³)], [Gas S2 Avg (10³m³)],
    [Gas Gathered Avg (e³m³/d)], [Condensate Gathered Avg (m³/d)],
    [Remarks]
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_TC_SELECT = """
SELECT
    [Well Name], [ImportDate],
    [Gas S2 Production (10³m³)], [Gas Sales Production (10³m³)],
    [Condensate Sales (m³/d)], [Sales CGR (m³/e³m³)],
    [Gas WH Production (e³m³/d)], [Condensate WH (m³/d)],
    [Cum Gas (e³m³)], [Cum Condy (m³)],
    [Layer Producer], [Pad Name], [SourceFileName],
    [Formation Producer], [Fault Block], [Remarks],
    [Lateral Length], [On Production Year], [Orientation],
    [Gas S2 Production mcf/d], [Gas S2 Cum Production mmcf],
    [Condensate Sales bbl/d], [Condensate Sales Cum mbbl]
FROM dbo.PCE_TC
"""


def _as_date(v) -> Optional[date]:
    if v is None or (isinstance(v, float) and np.isnan(v)) or pd.isna(v):
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    ts = pd.to_datetime(v, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.date()


def _num(v) -> Optional[float]:
    if v is None or (isinstance(v, float) and np.isnan(v)) or pd.isna(v):
        return None
    try:
        x = float(v)
        if np.isinf(x) or np.isnan(x):
            return None
        return x
    except (TypeError, ValueError):
        return None


def _int_or_none(v) -> Optional[int]:
    f = _num(v)
    if f is None:
        return None
    return int(round(f))


def _tc_row_to_production_tuple(row: pd.Series) -> Optional[Tuple]:
    wn = row.get("Well Name")
    if wn is None or str(wn).strip() == "":
        return None
    d = _as_date(row.get("ImportDate"))
    if d is None:
        return None

    gas_wh = _num(row.get("Gas WH Production (e³m³/d)"))
    cond_wh = _num(row.get("Condensate WH (m³/d)"))
    gas_s2 = _num(row.get("Gas S2 Production (10³m³)"))
    gas_sales = _num(row.get("Gas Sales Production (10³m³)"))
    cond_sales = _num(row.get("Condensate Sales (m³/d)"))
    sales_cgr = _num(row.get("Sales CGR (m³/e³m³)"))
    cum_gas = _num(row.get("Cum Gas (e³m³)"))
    cum_condy = _num(row.get("Cum Condy (m³)"))
    layer = row.get("Layer Producer")
    pad = row.get("Pad Name")
    formation = row.get("Formation Producer")
    fault = row.get("Fault Block")
    lateral = _num(row.get("Lateral Length"))
    orient = row.get("Orientation")
    on_year = _int_or_none(row.get("On Production Year"))
    remarks_raw = row.get("Remarks")

    layer_s = None if layer is None or pd.isna(layer) else str(layer).strip() or None
    pad_s = None if pad is None or pd.isna(pad) else str(pad).strip() or None
    if pad_s is not None:
        pad_s = _tc_pad_name_from_excel(pad_s)
    formation_s = None if formation is None or pd.isna(formation) else str(formation).strip() or None
    fault_s = None if fault is None or pd.isna(fault) else str(fault).strip() or None
    orient_s = None if orient is None or pd.isna(orient) else str(orient).strip() or None
    remarks_s = (
        None
        if remarks_raw is None or pd.isna(remarks_raw)
        else str(remarks_raw).strip() or None
    )

    return (
        d,
        1,
        1,
        str(wn).strip(),
        gas_wh,
        cond_wh,
        gas_s2,
        gas_sales,
        cond_sales,
        None,
        None,
        sales_cgr,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        cum_gas,
        None,
        cum_condy,
        cum_condy,
        None,
        None,
        formation_s,
        layer_s,
        fault_s,
        pad_s,
        lateral,
        orient_s,
        on_year,
        None,
        None,
        None,
        None,
        None,
        None,
        remarks_s,
    )


def sync_tc_to_production(
    log_callback: Optional[Callable[[str], None]] = None,
    conn=None,
) -> dict:
    """
    Re-materialize all PCE_TC rows into PCE_Production at each row's ImportDate.

    Deletes production rows whose (Well Name, Date) pair exists on PCE_TC, then inserts fresh.
    """
    def log(msg: str) -> None:
        if log_callback:
            log_callback(msg)

    close_conn = False
    if conn is None:
        conn = get_sql_conn()
        close_conn = True
    try:
        df = pd.read_sql(_TC_SELECT, conn)
        if df.empty:
            log(lf.detail("sync_tc_to_production: PCE_TC empty; nothing to materialize."))
            return {"ok": True, "rows_deleted": 0, "rows_inserted": 0}

        cursor = conn.cursor()
        # Backfill dbo.PCE_TC.[Pad Name] when legacy rows lack the PCE-TC- prefix (same rules as import).
        pad_fixes: List[Tuple] = []
        for _, row in df.iterrows():
            pad = row.get("Pad Name")
            wn = row.get("Well Name")
            imp = row.get("ImportDate")
            if wn is None or str(wn).strip() == "":
                continue
            pad_s = None if pad is None or pd.isna(pad) else str(pad).strip() or None
            if pad_s is None:
                continue
            new_pad = _tc_pad_name_from_excel(pad_s)
            if new_pad and new_pad != pad_s:
                pad_fixes.append((new_pad, str(wn).strip(), imp))
        if pad_fixes:
            cursor.executemany(
                """
                UPDATE dbo.PCE_TC
                SET [Pad Name] = ?
                WHERE [Well Name] = ? AND [ImportDate] = ?
                """,
                pad_fixes,
            )
            conn.commit()
            df = pd.read_sql(_TC_SELECT, conn)
        cursor.fast_executemany = True
        cursor.execute(
            """
            DELETE p
            FROM dbo.PCE_Production p
            WHERE EXISTS (
                SELECT 1 FROM dbo.PCE_TC t
                WHERE t.[Well Name] = p.[Well Name]
                  AND t.[ImportDate] = p.[Date]
            )
            """
        )
        deleted = cursor.rowcount or 0
        conn.commit()

        rows: List[Tuple] = []
        for _, row in df.iterrows():
            tup = _tc_row_to_production_tuple(row)
            if tup:
                rows.append(tup)

        if not rows:
            log(lf.warn("sync_tc_to_production: no valid TC rows to insert."))
            return {"ok": True, "rows_deleted": deleted, "rows_inserted": 0}

        cursor.executemany(_INSERT_SQL, rows)
        conn.commit()
        log(
            lf.detail(
                f"sync_tc_to_production: removed {lf.num(deleted)} old row(s), "
                f"inserted {lf.num(len(rows))}."
            )
        )
        return {"ok": True, "rows_deleted": deleted, "rows_inserted": len(rows)}
    finally:
        if close_conn:
            conn.close()
