import time
import re
from datetime import date
from typing import Callable, Dict, List, Optional, Tuple

from prodview_date_bounds import PRODVIEW_DATA_LAG_DAYS, prodview_effective_end_date

import pandas as pd
import numpy as np
from datetime import datetime
import warnings

import log_format as lf
from pce_production_schema import (
    PCE_PRODUCTION_INSERT_COLUMNS,
    SQL_INSERT_BATCH_SIZE,
    build_production_insert_sql,
    executemany_with_row_fallback,
)
from db_connection import get_sql_conn, SQL_DATABASE, SQL_SERVER

warnings.filterwarnings('ignore', category=FutureWarning)

# Prefix for gathered-production [Month] tags on PCE_Production / PCE_FRCST_PRD.
GATH_PRD_MONTH_PREFIX = "Gath PRD"

# Suffix appended to WM pad names on gathered PCE_Production / PCE_FRCST_PRD rows.
PRODUCTION_PAD_SUFFIX = " PRD"


def production_pad_name_from_wm(pad_name):
    """Return WM pad with `` PRD`` suffix for gathered production (idempotent)."""
    if pad_name is None or (isinstance(pad_name, float) and pd.isna(pad_name)):
        return None
    s = str(pad_name).strip()
    if not s:
        return None
    if s.endswith(PRODUCTION_PAD_SUFFIX):
        return s
    return f"{s}{PRODUCTION_PAD_SUFFIX}"


def production_pad_sql_from_wm(column_expr: str) -> str:
    """SQL expression: blank WM pad -> NULL, else pad + `` PRD`` (no double suffix)."""
    trimmed = f"NULLIF(LTRIM(RTRIM(CAST({column_expr} AS NVARCHAR(4000)))), N'')"
    suffix = PRODUCTION_PAD_SUFFIX.replace("'", "''")
    return (
        f"CASE WHEN {trimmed} IS NULL THEN NULL "
        f"WHEN RIGHT({trimmed}, {len(PRODUCTION_PAD_SUFFIX)}) = N'{suffix}' THEN {trimmed} "
        f"ELSE {trimmed} + N'{suffix}' END"
    )


def gathered_frcst_prd_pad_sql() -> str:
    """
    SQL expression for gathered ``PCE_FRCST_PRD.[Pad]``.

    Prefers ``PCE_Production.[Pad Name]``, falls back to WM pad, always applies
    `` PRD`` suffix (idempotent if already suffixed).
    """
    pad_src = """COALESCE(
          NULLIF(LTRIM(RTRIM(CAST(p.[Pad Name] AS NVARCHAR(4000)))), N''),
          NULLIF(LTRIM(RTRIM(CAST(ca.[Pad Name] AS NVARCHAR(4000)))), N'')
      )"""
    return production_pad_sql_from_wm(pad_src)


class _RebuildProgress:
    """Map full-rebuild sub-steps into 0–99% for the Prodview dialog."""

    _RANGES = {
        "snowflake": (0, 25),
        "cda_sales": (25, 65),
        "prep": (65, 75),
        "insert": (75, 95),
        "finalize": (95, 99),
    }

    def __init__(self, callback: Optional[Callable[[int], None]] = None):
        self._callback = callback

    def emit(self, percent: float) -> None:
        if self._callback is not None:
            self._callback(min(99, max(0, int(round(percent)))))

    def phase_done(self, phase: str) -> None:
        _, end = self._RANGES[phase]
        self.emit(end)

    def phase_part(self, phase: str, done: int, total: int) -> None:
        start, end = self._RANGES[phase]
        if total <= 0:
            self.emit(end)
            return
        self.emit(start + (done / total) * (end - start))

    def snowflake_substep(self, sub_pct: int) -> None:
        """Map refresh_pce_cda_from_snowflake milestones (15, 30, 45) into snowflake phase."""
        start, end = self._RANGES["snowflake"]
        self.emit(start + (min(max(sub_pct, 0), 45) / 45.0) * (end - start))

# Backward-compatible alias (prefix only; per-well labels use gathered_prd_month_label()).
PCE_PRODUCTION_MONTH_LABEL = GATH_PRD_MONTH_PREFIX


def gathered_prd_month_label(enersight_well_name) -> str:
    """
    Build gathered-production [Month] label: ``Gath PRD L-16`` from Enersight well name
    with the word ``well`` removed (case-insensitive).
    """
    if enersight_well_name is None or (isinstance(enersight_well_name, float) and pd.isna(enersight_well_name)):
        return GATH_PRD_MONTH_PREFIX
    s = str(enersight_well_name).strip()
    if not s:
        return GATH_PRD_MONTH_PREFIX
    core = re.sub(r"\bwell\b", "", s, flags=re.IGNORECASE)
    core = " ".join(core.split())
    if not core:
        return GATH_PRD_MONTH_PREFIX
    return f"{GATH_PRD_MONTH_PREFIX} {core}"


def gathered_prd_month_sql_from_enersight(enersight_sql: str) -> str:
    """T-SQL expression for [Month] from an Enersight Well Name column/SQL fragment."""
    en = (
        f"NULLIF(LTRIM(RTRIM(CAST(({enersight_sql}) AS NVARCHAR(4000)))), N'')"
    )
    core = (
        f"LTRIM(RTRIM(REPLACE(REPLACE(REPLACE("
        f"{en}, N' Well', N''), N' well', N''), N' WELL', N'')))"
    )
    return f"""CASE
        WHEN {en} IS NULL THEN N'{GATH_PRD_MONTH_PREFIX}'
        WHEN {core} = N'' THEN N'{GATH_PRD_MONTH_PREFIX}'
        ELSE N'{GATH_PRD_MONTH_PREFIX} ' + {core}
    END"""


def _load_wm_dataframe(conn=None):
    """Single PCE_WM read for well-name / metadata lookups."""
    query = """
    SELECT [Well Name], [Composite Name], [Value Navigator UWI],
           [Pad Name], [Enersight Well Name]
    FROM PCE_WM
    WHERE [Well Name] IS NOT NULL
      AND ([Exception] IS NULL OR [Exception] = '' OR [Exception] = 'N')
    """
    if conn is None:
        with get_sql_conn() as own_conn:
            return pd.read_sql(query, own_conn)
    return pd.read_sql(query, conn)


def fetch_well_master_lookups(conn=None):
    """
    Return composite/fallback rename maps plus UWI, pad, and Enersight lookups
    from one PCE_WM query.
    """
    wm = _load_wm_dataframe(conn)
    wm = wm.copy()
    wm["Well Name"] = wm["Well Name"].astype(str).str.strip()

    valid_composite = (
        wm["Composite Name"].notna()
        & wm["Composite Name"].astype(str).str.strip().ne("")
    )
    composite_stripped = wm["Composite Name"].astype(str).str.strip()
    composite_map = dict(
        zip(
            wm.loc[valid_composite, "Well Name"],
            composite_stripped[valid_composite],
        )
    )
    fallback_map = dict(zip(wm["Well Name"], wm["Well Name"]))

    uwi_lookup = {}
    pad_lookup = {}
    enersight_lookup = {}
    for _, row in wm.iterrows():
        wn = str(row["Well Name"]).strip() if pd.notna(row["Well Name"]) else ""
        comp = row["Composite Name"]
        comp_s = str(comp).strip() if comp is not None and str(comp).strip() else None

        uwi = row["Value Navigator UWI"]
        if uwi is not None and not pd.isna(uwi) and str(uwi).strip():
            uwi_val = str(uwi).strip()
            if wn:
                uwi_lookup[wn] = uwi_val
            if comp_s:
                uwi_lookup[comp_s] = uwi_val

        pad = row["Pad Name"]
        if pd.isna(pad) or (isinstance(pad, str) and not str(pad).strip()):
            pad_val = None
        else:
            pad_val = str(pad).strip()
        if wn:
            pad_lookup[wn] = pad_val
        if comp_s:
            pad_lookup[comp_s] = pad_val

        en = row["Enersight Well Name"]
        if pd.isna(en) or (isinstance(en, str) and not str(en).strip()):
            en_val = None
        else:
            en_val = str(en).strip()
        if wn:
            enersight_lookup[wn] = en_val
        if comp_s:
            enersight_lookup[comp_s] = en_val

    return {
        "composite_map": composite_map,
        "fallback_map": fallback_map,
        "uwi_lookup": uwi_lookup,
        "pad_lookup": pad_lookup,
        "enersight_lookup": enersight_lookup,
    }


def fetch_well_master_enersight_lookup(conn=None):
    """WM ``[Well Name]`` / ``[Composite Name]`` -> ``[Enersight Well Name]``."""
    if conn is not None:
        return fetch_well_master_lookups(conn)["enersight_lookup"]
    return fetch_well_master_lookups()["enersight_lookup"]


def apply_gathered_prd_month_labels(df, enersight_lookup=None):
    """Set ``[Month]`` on gathered production rows from WM Enersight names."""
    if df is None or df.empty or "Well Name" not in df.columns:
        return df
    lookup = (
        enersight_lookup
        if enersight_lookup is not None
        else fetch_well_master_enersight_lookup()
    )
    out = df.copy()
    keys = out["Well Name"].astype(str).str.strip()
    label_map = {
        wn: gathered_prd_month_label(lookup.get(wn))
        for wn in keys.unique()
    }
    out["Month"] = keys.map(label_map)
    return out


def _allocation_factor_months_overlapping(
    cursor,
    range_start: date,
    range_end: date,
) -> List:
    """MonthStartDate values in Allocation_Factors whose calendar month overlaps [range_start, range_end]."""
    from sales_allocation_updates import calendar_month_bounds

    cursor.execute(
        "SELECT DISTINCT MonthStartDate FROM Allocation_Factors ORDER BY MonthStartDate"
    )
    out = []
    for (month_start,) in cursor.fetchall():
        first, last, _ = calendar_month_bounds(month_start)
        if first <= range_end and last >= range_start:
            out.append(month_start)
    return out


def _refresh_cda_sales_from_allocation_factors(
    log=print,
    cancel_event=None,
    update_production=True,
    progress: Optional[_RebuildProgress] = None,
    date_window: Optional[Tuple[date, date]] = None,
    conn=None,
):
    """
    Repaint Gas S2, gas sales, condensate sales, and Sales CGR on PCE_CDA using
    Allocation_Factors (same logic as PA + Public Sales ratio passes).

    When ``date_window`` is set, only months overlapping that inclusive range are
    processed (routine update). Otherwise all AF months run (full rebuild).

    Returns False if cancelled mid-loop; True otherwise (including when AF is empty).
    """
    from sales_allocation_updates import (
        apply_full_sales_ratios_bulk,
        apply_valnav_allocation_bulk,
    )

    def aborted():
        return cancel_event is not None and cancel_event.is_set()

    own_conn = conn is None
    if own_conn:
        conn = get_sql_conn()
    try:
        cursor = conn.cursor()
        if date_window is not None:
            months = _allocation_factor_months_overlapping(
                cursor, date_window[0], date_window[1]
            )
            n = len(months)
            if not months:
                log(
                    lf.warn(
                        "No Allocation_Factors rows overlap the rolling window; "
                        "PCE_CDA Gas S2 / sales columns are unchanged."
                    )
                )
                return True
        else:
            cursor.execute(
                "SELECT COUNT(DISTINCT MonthStartDate) FROM Allocation_Factors"
            )
            n = int(cursor.fetchone()[0] or 0)
            if n == 0:
                log(
                    lf.warn(
                        "No Allocation_Factors rows; PCE_CDA Gas S2 / sales columns are unchanged."
                    )
                )
                return True

        scope = (
            f"{date_window[0]} through {date_window[1]}"
            if date_window is not None
            else "all months"
        )
        log(
            lf.step(
                f"Refreshing PCE_CDA from Allocation_Factors ({lf.num(n)} month(s), "
                f"{scope}: Gas S2, gas sales, condensate sales, Sales CGR)…"
            )
        )

        if aborted():
            log(lf.warn("Cancelled during PCE_CDA sales refresh."))
            return False

        apply_valnav_allocation_bulk(
            conn,
            log=log,
            update_production=update_production,
            date_window=date_window,
        )
        if progress is not None:
            progress.phase_part("cda_sales", max(1, n // 2), max(1, n))

        if aborted():
            log(lf.warn("Cancelled during PCE_CDA sales refresh."))
            return False

        apply_full_sales_ratios_bulk(
            conn,
            log=log,
            update_production=update_production,
            date_window=date_window,
        )
        if progress is not None:
            progress.phase_done("cda_sales")
        return True
    finally:
        if own_conn and conn is not None and hasattr(conn, "close"):
            conn.close()


def _refresh_ngl_from_allocation_factors(
    log=print,
    cancel_event=None,
    conn=None,
    date_window: Optional[Tuple[date, date]] = None,
):
    """
    Repaint NGL ratio columns on PCE_Production from Allocation_Factors monthly volumes.

    When ``date_window`` is set, only months overlapping that inclusive range are
    processed (routine update). Otherwise all AF NGL months run (full rebuild).

    Returns False if cancelled mid-loop; True otherwise (including when no NGL months exist).
    """
    from ngl_monthly_update import run_ngl_bulk_from_allocation_factors

    def aborted():
        return cancel_event is not None and cancel_event.is_set()

    own_conn = conn is None
    if own_conn:
        conn = get_sql_conn()
    try:
        cursor = conn.cursor()
        if date_window is not None:
            from sales_allocation_updates import calendar_month_bounds

            cursor.execute(
                """
                SELECT DISTINCT MonthStartDate
                FROM Allocation_Factors
                WHERE [NGL_C2] IS NOT NULL
                   OR [NGL_C3] IS NOT NULL
                   OR [NGL_C4] IS NOT NULL
                   OR [NGL_C5] IS NOT NULL
                   OR [PA_NGLs] IS NOT NULL
                ORDER BY MonthStartDate
                """
            )
            range_start, range_end = date_window
            n = 0
            for (month_start,) in cursor.fetchall():
                first, last, _ = calendar_month_bounds(month_start)
                if first <= range_end and last >= range_start:
                    n += 1
            if n == 0:
                log(
                    lf.warn(
                        "No Allocation_Factors NGL rows overlap the rolling window; "
                        "PCE_Production NGL ratios unchanged."
                    )
                )
                return True
        else:
            cursor.execute(
                """
                SELECT COUNT(DISTINCT MonthStartDate)
                FROM Allocation_Factors
                WHERE [NGL_C2] IS NOT NULL
                   OR [NGL_C3] IS NOT NULL
                   OR [NGL_C4] IS NOT NULL
                   OR [NGL_C5] IS NOT NULL
                   OR [PA_NGLs] IS NOT NULL
                """
            )
            n = int(cursor.fetchone()[0] or 0)

        if n == 0:
            log(lf.warn("No Allocation_Factors NGL rows; PCE_Production NGL ratios unchanged."))
            return True

        scope = (
            f"{date_window[0]} through {date_window[1]}"
            if date_window is not None
            else "all months"
        )
        log(
            lf.step(
                f"Refreshing PCE_Production NGL ratios from Allocation_Factors "
                f"({lf.num(n)} month(s), {scope}, batched)…"
            )
        )

        if aborted():
            log(lf.warn("Cancelled during NGL ratio refresh."))
            return False

        summary = run_ngl_bulk_from_allocation_factors(
            conn,
            log=log,
            cancel_event=cancel_event,
            date_window=date_window,
        )
        if summary.skipped and (summary.skip_reason or "").startswith("Cancelled"):
            return False
        return True
    finally:
        if own_conn and conn is not None and hasattr(conn, "close"):
            conn.close()


def clear_pce_production(conn=None):
    """Clear all rows from PCE_Production (TRUNCATE when permitted, else DELETE)."""
    own_conn = conn is None
    if own_conn:
        conn = get_sql_conn()
    try:
        cursor = conn.cursor()
        print(lf.step("Clearing PCE_Production…"))
        deleted = 0
        try:
            cursor.execute("TRUNCATE TABLE dbo.PCE_Production")
            conn.commit()
            print(lf.success("Cleared PCE_Production (truncate)"))
            return deleted
        except Exception:
            pass
        cursor.execute("SELECT COUNT(*) FROM PCE_Production")
        existing = cursor.fetchone()[0] or 0
        print(
            lf.detail(
                f"Truncate unavailable; deleting {lf.num(existing)} row(s) "
                "(this may take several minutes)…"
            )
        )
        cursor.execute("DELETE FROM PCE_Production")
        deleted = cursor.rowcount or 0
        try:
            cursor.execute("DBCC CHECKIDENT('PCE_Production', RESEED, 0)")
        except Exception:
            pass
        conn.commit()
        print(
            lf.success(
                f"Cleared PCE_Production: {lf.num(deleted)} records deleted "
                "(identity reseeded where applicable)"
            )
        )
        return deleted
    finally:
        if own_conn and conn is not None and hasattr(conn, "close"):
            conn.close()


_CDA_SELECT_SQL = """
    SELECT 
        [Well Name] as [Source_Well_Name],
        ProdDate as [Date],
        [GasWH_Production] as [Gas WH Production (10³m³)],
        [Condensate_WH_Production] as [Condensate WH (m³/d)],
        [Gas - S2 Production] as [Gas S2 Production (10³m³)],
        [Gas - Sales Production] as [Gas Sales Production (10³m³)],
        [Condensate - Sales Production] as [Condensate Sales (m³/d)],
        [Gathered_Gas_Production] as [Gathered Gas (e³m³/d)],
        [Gathered_Condensate_Production] as [Gathered Condensate (m³/d)],
        [Gathered_Water_Production] as [Gath. Water Rate (m³/d)],
        [Sales CGR Ratio] as [Sales CGR (m³/e³m³)],
        [CGR_Ratio] as [CGR (m³/e³m³)],
        [WGR_Ratio] as [WGR (m³/e³m³)],
        [ECF_Ratio] as [ECF],
        [OnProdHours] as [Hours On],
        [TubingPressure] as [Tubing Pressure (kPa)],
        [CasingPressure] as [Casing Pressure (kPa)],
        [ChokeSize] as [Choke Size],
        [AllocatedWater_Rate] as [Alloc. Water Rate (m³)],
        [NGL_Production] as [NGL (m³)],
        [Formation Producer],
        [Layer Producer],
        [Fault Block],
        [Pad Name],
        [Lateral Length],
        [Orient] as [Orientation]
    FROM PCE_CDA
"""


def _cda_select_sql(start_date=None, end_cap=None) -> Tuple[str, List]:
    """Build CDA SELECT with optional ProdDate bounds pushed to SQL."""
    clauses: List[str] = []
    params: List = []
    if start_date is not None:
        clauses.append("ProdDate >= ?")
        params.append(start_date)
    if end_cap is not None:
        clauses.append("ProdDate <= ?")
        params.append(end_cap)
    sql = _CDA_SELECT_SQL.strip()
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    return sql, params


def _sort_cda_dataframe(df):
    """Match legacy SQL ORDER BY (YE2, TC, then well/date)."""
    if df.empty:
        return df
    out = df.copy()
    src = out["Source_Well_Name"].astype(str)
    out["_sort_tier"] = np.where(
        src.str.startswith("YE2"),
        1,
        np.where(src.str.endswith(" - TC"), 2, 0),
    )
    out = out.sort_values(["_sort_tier", "Source_Well_Name", "Date"]).drop(
        columns=["_sort_tier"]
    )
    return out.reset_index(drop=True)


_CUMULATIVE_PAIRS: Tuple[Tuple[str, str], ...] = (
    ("Gas WH Production (10³m³)", "Gas WH Cumulative Production (10³m³)"),
    ("Gas S2 Production (10³m³)", "Gas S2 Cumulative Production (10³m³)"),
    ("Gas Sales Production (10³m³)", "Gas Sales Cumulative Production (10³m³)"),
    ("Condensate Sales (m³/d)", "Condensate Sales Cumulative Production (m³)"),
    ("Condensate WH (m³/d)", "Condensate WH Cumulative Production (m³)"),
    ("Gathered Gas (e³m³/d)", "Gas Gathered Cumulative (e³m³)"),
    ("Gathered Condensate (m³/d)", "Condensate Gathered Cumulative (m³)"),
    ("Gath. Water Rate (m³/d)", "Gath. Water Cumulative (m³)"),
)


def month_start_on_or_before(d: date) -> date:
    """First calendar day of the month containing *d*."""
    return d.replace(day=1)


def map_cda_well_names_to_production(
    cda_well_names,
    composite_map,
    fallback_map,
) -> List[str]:
    """
    Map PCE_CDA ``[Well Name]`` values to PCE_Production ``[Well Name]`` (composite / WM).

    Preserves order; drops blanks; de-duplicates.
    """
    seen = set()
    out: List[str] = []
    for raw in cda_well_names or []:
        s = str(raw).strip()
        if not s:
            continue
        mapped = composite_map.get(s) or fallback_map.get(s) or s
        key = str(mapped).strip()
        if key and key not in seen:
            seen.add(key)
            out.append(key)
    return out


def query_wells_with_cda_in_range(cursor, date_start, date_end):
    """Distinct PCE_CDA well names with rows in [date_start, date_end]."""
    cursor.execute(
        """
        SELECT DISTINCT [Well Name]
        FROM PCE_CDA
        WHERE ProdDate BETWEEN ? AND ?
        """,
        date_start,
        date_end,
    )
    return [row[0] for row in cursor.fetchall()]


def fetch_cda_data(
    well_names=None,
    start_date=None,
    end_cap=None,
    conn=None,
    log=None,
):
    """
    Fetch daily production rows from PCE_CDA for rebuild.

    ``well_names``: optional list of CDA ``[Well Name]`` values; when set, only
    those wells are loaded. ``start_date`` / ``end_cap`` limit ProdDate inclusively.
    When omitted, loads all CDA (or full history per well). Sorting is done in pandas.
    """
    log_fn = log or print
    own_conn = conn is None
    if own_conn:
        conn = get_sql_conn()

    try:
        if well_names is not None and len(well_names) == 0:
            df = pd.DataFrame()
        elif well_names is None:
            count_sql = "SELECT COUNT(*) FROM PCE_CDA"
            count_params: List = []
            count_clauses: List[str] = []
            if start_date is not None:
                count_clauses.append("ProdDate >= ?")
                count_params.append(start_date)
            if end_cap is not None:
                count_clauses.append("ProdDate <= ?")
                count_params.append(end_cap)
            if count_clauses:
                count_sql += " WHERE " + " AND ".join(count_clauses)
            count_cur = conn.cursor()
            count_cur.execute(count_sql, count_params or None)
            cda_rows = count_cur.fetchone()[0] or 0
            bounds = ""
            if start_date is not None or end_cap is not None:
                bounds = f" ({start_date or '…'} through {end_cap or '…'})"
            log_fn(
                lf.step(
                    f"Loading {lf.num(cda_rows)} PCE_CDA row(s) from SQL Server{bounds} "
                    "(this may take several minutes)…"
                )
            )
            query, params = _cda_select_sql(start_date, end_cap)
            df = pd.read_sql(query, conn, params=params or None)
        else:
            frames = []
            chunk_size = 500
            for i in range(0, len(well_names), chunk_size):
                chunk = well_names[i : i + chunk_size]
                placeholders = ",".join("?" for _ in chunk)
                clauses = [f"[Well Name] IN ({placeholders})"]
                params = list(chunk)
                if start_date is not None:
                    clauses.append("ProdDate >= ?")
                    params.append(start_date)
                if end_cap is not None:
                    clauses.append("ProdDate <= ?")
                    params.append(end_cap)
                query = f"{_CDA_SELECT_SQL} WHERE {' AND '.join(clauses)}"
                frames.append(pd.read_sql(query, conn, params=params))
            df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

        if not df.empty:
            df = _sort_cda_dataframe(df)

        if well_names is None:
            log_fn(lf.detail(f"Loaded {lf.num(len(df))} rows from PCE_CDA"))
        else:
            span = ""
            if start_date is not None or end_cap is not None:
                span = f" ({start_date or '…'} through {end_cap or '…'})"
            log_fn(
                lf.detail(
                    f"Loaded {lf.num(len(df))} CDA rows for "
                    f"{lf.num(len(well_names))} well(s){span}"
                )
            )
        return df
    finally:
        if own_conn and conn is not None:
            conn.close()


def fetch_production_patch_seeds(
    cursor,
    well_names,
    before_date: date,
) -> Tuple[Dict[str, int], Dict[str, int], Dict[str, Dict[str, float]]]:
    """
    Anchors from the last PCE_Production row strictly before *before_date* per well.

    Used when routine update replaces only a rolling date window (not full history).
    ``well_names`` must be PCE_Production ``[Well Name]`` values (after WM mapping),
    not raw PCE_CDA source names.
    """
    days_seq: Dict[str, int] = {}
    day_seq_uprt: Dict[str, int] = {}
    cum_seeds: Dict[str, Dict[str, float]] = {}
    if not well_names:
        return days_seq, day_seq_uprt, cum_seeds

    cum_cols = [target for _, target in _CUMULATIVE_PAIRS]
    select_cols = ", ".join(f"p.[{c}]" for c in cum_cols)
    batch_size = 200
    for i in range(0, len(well_names), batch_size):
        chunk = well_names[i : i + batch_size]
        ph = ",".join("?" * len(chunk))
        cursor.execute(
            f"""
            SELECT p.[Well Name], p.[Days Seq], p.[Day Seq UPRT], {select_cols}
            FROM dbo.PCE_Production AS p
            INNER JOIN (
                SELECT [Well Name], MAX([Date]) AS md
                FROM dbo.PCE_Production
                WHERE [Date] < ? AND [Well Name] IN ({ph})
                GROUP BY [Well Name]
            ) AS x
                ON p.[Well Name] = x.[Well Name] AND p.[Date] = x.md
            """,
            (before_date, *chunk),
        )
        for row in cursor.fetchall():
            wn = row[0]
            if wn is None:
                continue
            wn_s = str(wn).strip()
            days_seq[wn_s] = int(row[1] or 0)
            day_seq_uprt[wn_s] = int(row[2] or 0)
            cum_seeds[wn_s] = {}
            for j, col in enumerate(cum_cols, start=3):
                val = row[j]
                cum_seeds[wn_s][col] = float(val) if val is not None else 0.0
    return days_seq, day_seq_uprt, cum_seeds


def fetch_on_production_year_by_well(cursor, well_names) -> Dict[str, int]:
    """Year of first PCE_Production row per well (for window-only rebuilds)."""
    out: Dict[str, int] = {}
    if not well_names:
        return out
    batch_size = 200
    for i in range(0, len(well_names), batch_size):
        chunk = well_names[i : i + batch_size]
        ph = ",".join("?" * len(chunk))
        cursor.execute(
            f"""
            SELECT [Well Name], MIN([Date])
            FROM dbo.PCE_Production
            WHERE [Well Name] IN ({ph})
            GROUP BY [Well Name]
            """,
            chunk,
        )
        for wn, first_dt in cursor.fetchall():
            if wn is None or first_dt is None:
                continue
            ts = pd.to_datetime(first_dt, errors="coerce")
            if pd.isna(ts):
                continue
            out[str(wn).strip()] = int(ts.year)
    return out


def fetch_well_mapping(conn=None):
    """Fetch well name mappings from PCE_WM (Composite Name and Well Name)"""
    lookups = fetch_well_master_lookups(conn)
    composite_map = lookups["composite_map"]
    fallback_map = lookups["fallback_map"]
    print(lf.detail(f"Loaded {lf.num(len(fallback_map))} well name mappings"))
    return composite_map, fallback_map


def fetch_well_master_uwi_lookup(conn=None):
    """Trimmed WM ``[Well Name]`` / ``[Composite Name]`` -> ``[Value Navigator UWI]``."""
    return fetch_well_master_lookups(conn)["uwi_lookup"]


def apply_uwi_from_well_master(df, uwi_lookup=None):
    """Set ``UWI`` from WM where ``[Well Name]`` matches WM well or composite."""
    if df is None or df.empty or "Well Name" not in df.columns:
        return df
    if "UWI" not in df.columns:
        out = df.copy()
        out["UWI"] = None
        df = out
    lookup = uwi_lookup if uwi_lookup is not None else fetch_well_master_uwi_lookup()
    if not lookup:
        return df
    out = df.copy()
    out["UWI"] = out["UWI"].astype(object)
    keys = out["Well Name"].astype(str).str.strip()
    mask = keys.isin(lookup)
    if mask.any():
        out.loc[mask, "UWI"] = keys[mask].map(lookup).values
    return out


def fetch_well_master_pad_lookup(conn=None):
    """Trimmed WM ``[Well Name]`` / ``[Composite Name]`` -> ``[Pad Name]``."""
    return fetch_well_master_lookups(conn)["pad_lookup"]


def apply_pad_name_from_well_master(df, pad_lookup=None):
    """Overwrite ``Pad Name`` from WM where ``[Well Name]`` matches WM well or composite."""
    if df is None or df.empty or "Well Name" not in df.columns:
        return df
    if "Pad Name" not in df.columns:
        df = df.copy()
        df["Pad Name"] = np.nan
    lookup = pad_lookup if pad_lookup is not None else fetch_well_master_pad_lookup()
    if not lookup:
        return df
    out = df.copy()
    keys = out["Well Name"].astype(str).str.strip()
    mask = keys.isin(lookup)
    if mask.any():
        out.loc[mask, "Pad Name"] = (
            keys[mask].map(lookup).map(production_pad_name_from_wm).values
        )
    return out


_WM_APPLY_JOIN = """
CROSS APPLY (
    SELECT TOP 1
          wm.[Pad Name] AS pad
        , wm.[Enersight Well Name] AS en
    FROM PCE_WM AS wm
    WHERE (
            wm.[Well Name] = p.[Well Name]
         OR (
                NULLIF(RTRIM(CAST(wm.[Composite Name] AS NVARCHAR(4000))), N'') IS NOT NULL
            AND wm.[Composite Name] = p.[Well Name]
            )
        )
      AND (wm.[Exception] IS NULL OR wm.[Exception] = N'' OR wm.[Exception] = N'N')
) AS ca
"""


def sync_production_wm_metadata_from_wm_sql(
    cursor,
    date_start=None,
    date_end=None,
    *,
    update_pad=True,
    update_enersight=True,
    update_month=True,
    pad_date_start=None,
    pad_date_end=None,
):
    """
    Set ``PCE_Production`` WM-backed metadata in one table scan.

    Optional ``date_start``/``date_end`` restrict which rows are updated (legacy pad-only
    pass). When ``pad_date_start``/``pad_date_end`` are set, pad is updated only on rows
    in that inclusive range (others keep existing pad); enersight/month apply to all rows
    matched by the WHERE clause.
    """
    if not any((update_pad, update_enersight, update_month)):
        return

    date_filter = ""
    params: List = []
    if date_start is not None and date_end is not None:
        date_filter = " AND p.[Date] BETWEEN ? AND ?"
        params = [date_start, date_end]

    month_expr = gathered_prd_month_sql_from_enersight("ca.en")
    enersight_val = "NULLIF(LTRIM(RTRIM(CAST(ca.en AS NVARCHAR(4000)))), N'')"

    set_parts = []
    if update_pad:
        pad_sql = production_pad_sql_from_wm("ca.pad")
        if pad_date_start is not None and pad_date_end is not None:
            set_parts.append(
                f"p.[Pad Name] = CASE WHEN p.[Date] BETWEEN ? AND ? "
                f"THEN {pad_sql} ELSE p.[Pad Name] END"
            )
            params = [pad_date_start, pad_date_end] + params
        else:
            set_parts.append(f"p.[Pad Name] = {pad_sql}")
    if update_enersight:
        set_parts.append(
            f"p.[Enersight Well Name] = COALESCE({enersight_val}, p.[Enersight Well Name])"
        )
    if update_month:
        set_parts.append(f"p.[Month] = {month_expr}")

    sql = f"""
UPDATE p
SET {', '.join(set_parts)}
FROM PCE_Production AS p
{_WM_APPLY_JOIN}
WHERE p.[Well Name] NOT LIKE N'%% - TC'
  AND p.[Well Name] NOT LIKE N'YE2%%'
{date_filter}
"""
    if params:
        cursor.execute(sql, params)
    else:
        cursor.execute(sql)


def sync_production_wm_metadata_combined_sql(
    cursor,
    *,
    pad_date_window: Optional[Tuple[date, date]] = None,
):
    """
    One-pass WM metadata sync: enersight + month on all gathered rows; pad only inside
    ``pad_date_window`` when provided (post-rebuild full rebuild path).
    """
    sync_production_wm_metadata_from_wm_sql(
        cursor,
        update_pad=pad_date_window is not None,
        update_enersight=True,
        update_month=True,
        pad_date_start=pad_date_window[0] if pad_date_window else None,
        pad_date_end=pad_date_window[1] if pad_date_window else None,
    )


def sync_production_pad_names_from_wm_sql(cursor, date_start=None, date_end=None):
    """Set ``PCE_Production.[Pad Name]`` from ``PCE_WM`` (Well or Composite name match)."""
    sync_production_wm_metadata_from_wm_sql(
        cursor,
        date_start,
        date_end,
        update_pad=True,
        update_enersight=False,
        update_month=False,
    )


def sync_production_enersight_well_names_from_wm_sql(cursor, date_start=None, date_end=None):
    """
    Set ``PCE_Production.[Enersight Well Name]`` from ``PCE_WM`` (Well or Composite name match).
    Same join rules as ``sync_production_pad_names_from_wm_sql``; only rows with non-blank WM
    Enersight are updated. Run after production rebuilds that re-insert without this column.
    """
    sync_production_wm_metadata_from_wm_sql(
        cursor,
        date_start,
        date_end,
        update_pad=False,
        update_enersight=True,
        update_month=False,
    )


def sync_production_gathered_month_labels_from_wm_sql(cursor, date_start=None, date_end=None):
    """Set ``PCE_Production.[Month]`` to ``Gath PRD {Enersight}`` from ``PCE_WM``."""
    sync_production_wm_metadata_from_wm_sql(
        cursor,
        date_start,
        date_end,
        update_pad=False,
        update_enersight=False,
        update_month=True,
    )


def sync_production_uwi_from_wm_sql(cursor, date_start=None, date_end=None):
    """
    Set ``PCE_Production.[UWI]`` from ``PCE_WM.[Value Navigator UWI]``.

    Matches ``scripts/add_pce_ngl_columns.sql`` Part 2 (composite / well name, non-exception WM).
    """
    date_filter = ""
    params: list = []
    if date_start is not None and date_end is not None:
        date_filter = " AND p.[Date] BETWEEN ? AND ?"
        params = [date_start, date_end]

    sql = f"""
UPDATE p
SET p.[UWI] = LTRIM(RTRIM(CAST(ca.[Value Navigator UWI] AS NVARCHAR(4000))))
FROM PCE_Production AS p
CROSS APPLY (
    SELECT TOP 1 wm.[Value Navigator UWI]
    FROM PCE_WM AS wm
    WHERE (
              wm.[Well Name] = p.[Well Name]
           OR (
                  NULLIF(RTRIM(CAST(wm.[Composite Name] AS NVARCHAR(4000))), N'') IS NOT NULL
              AND wm.[Composite Name] = p.[Well Name]
              )
          )
      AND (wm.[Exception] IS NULL OR wm.[Exception] = N'' OR wm.[Exception] = N'N')
      AND NULLIF(LTRIM(RTRIM(CAST(wm.[Value Navigator UWI] AS NVARCHAR(4000)))), N'') IS NOT NULL
) AS ca
WHERE p.[Well Name] NOT LIKE N'% - TC'
  AND p.[Well Name] NOT LIKE N'YE2%'
{date_filter}
"""
    if params:
        cursor.execute(sql, params)
    else:
        cursor.execute(sql)


def sync_allocation_factors_uwi_from_wm_sql(cursor):
    """
    Set ``Allocation_Factors.[UWI]`` from ``PCE_WM.[Value Navigator UWI]``.

    Well-name keyed (no composite join). Runs on full/quick rebuild so WM edits
    propagate without re-running PA monthly load.
    """
    cursor.execute(
        """
UPDATE a
SET a.[UWI] = LTRIM(RTRIM(CAST(w.[Value Navigator UWI] AS NVARCHAR(4000))))
FROM Allocation_Factors AS a
INNER JOIN PCE_WM AS w
    ON a.[Well Name] = w.[Well Name]
WHERE (w.[Exception] IS NULL OR w.[Exception] = N'' OR w.[Exception] = N'N')
  AND NULLIF(LTRIM(RTRIM(CAST(w.[Value Navigator UWI] AS NVARCHAR(4000)))), N'') IS NOT NULL
"""
    )


def sync_wm_uwi_to_downstream_sql(cursor, date_start=None, date_end=None):
    """Sync WM UWI onto PCE_Production and Allocation_Factors."""
    sync_production_uwi_from_wm_sql(cursor, date_start, date_end)
    sync_allocation_factors_uwi_from_wm_sql(cursor)


def apply_well_names(df, composite_map, fallback_map):
    """
    Apply well name mapping: use Composite Name if available, otherwise use Well Name.
    Vectorized via .map() instead of per-row .apply().
    """
    original_count = len(df)
    stripped = df['Source_Well_Name'].astype(str).str.strip()

    mapped = stripped.map(composite_map)
    still_missing = mapped.isna()
    mapped[still_missing] = stripped[still_missing].map(fallback_map)

    df['Well Name'] = mapped

    unmapped_count = df['Well Name'].isna().sum()
    if unmapped_count > 0:
        print(lf.warn(f"{lf.num(unmapped_count)} rows ({unmapped_count/original_count*100:.1f}%) have no well name mapping"))
        unmapped_sources = stripped[df['Well Name'].isna()].unique()
        if len(unmapped_sources) > 0:
            print(lf.detail(f"Unmapped source wells: {', '.join(list(unmapped_sources)[:10])}"))
            if len(unmapped_sources) > 10:
                print(lf.detail(f"... and {lf.num(len(unmapped_sources) - 10)} more"))
        df = df.dropna(subset=['Well Name'])
        print(lf.detail(f"Remaining rows: {lf.num(len(df))}"))

    df = df.drop(columns=['Source_Well_Name'])
    print(lf.success("Well name mapping complete"))
    return df

def filter_to_first_production(df):
    """
    For each well, keep only rows from the first non-zero production onward.
    Vectorized: replaces per-well loops + per-row .loc writes with boolean masks.
    """
    original_count = len(df)
    df = df.sort_values(['Well Name', 'Date']).reset_index(drop=True)

    gas_wh = df['Gas WH Production (10³m³)'].fillna(0)
    gathered = df['Gathered Gas (e³m³/d)'].fillna(0)

    # VBA logic: if Gas WH <= 2, use Gathered Gas instead
    replace_mask = (gas_wh <= 2)
    effective = gas_wh.copy()
    effective[replace_mask] = gathered[replace_mask]

    # Apply replacement to the actual column as well
    df.loc[replace_mask, 'Gas WH Production (10³m³)'] = gathered[replace_mask]

    # Find first production date per well (first date where effective > 0)
    has_production = effective > 0
    first_prod_dates = (
        df.loc[has_production]
        .groupby('Well Name')['Date']
        .min()
    )

    if first_prod_dates.empty:
        print(lf.warn("No wells with production data found!"))
        return pd.DataFrame()

    df['_first_prod'] = df['Well Name'].map(first_prod_dates)
    keep_mask = df['_first_prod'].notna() & (df['Date'] >= df['_first_prod'])
    df_filtered = df.loc[keep_mask].drop(columns=['_first_prod']).reset_index(drop=True)

    wells_with_data = first_prod_dates.shape[0]
    rows_removed = original_count - len(df_filtered)
    print(lf.detail(
        f"Filtered to first production: {lf.num(wells_with_data)} wells, "
        f"{lf.num(len(df_filtered))} rows ({lf.num(rows_removed)} removed)"
    ))
    return df_filtered

def calculate_sequences(
    df,
    *,
    days_seq_seed: Optional[Dict[str, int]] = None,
    day_seq_uprt_seed: Optional[Dict[str, int]] = None,
):
    """
    Calculate Days Seq and Day Seq UPRT for each well.
    Vectorized via groupby().cumcount() and cumsum().

    Optional seeds continue numbering from existing PCE_Production before a patch window.
    """
    days_seq_seed = days_seq_seed or {}
    day_seq_uprt_seed = day_seq_uprt_seed or {}
    df = df.sort_values(["Well Name", "Date"]).reset_index(drop=True)

    if days_seq_seed:
        wn_key = df["Well Name"].astype(str).str.strip()
        seq_off = wn_key.map(days_seq_seed).fillna(0).astype(int)
    else:
        seq_off = 0
    df["Days Seq"] = df.groupby("Well Name").cumcount() + 1 + seq_off

    gas_positive = (
        pd.to_numeric(df["Gas WH Production (10³m³)"], errors="coerce")
        .fillna(0)
        .gt(0)
        .astype(int)
    )
    if day_seq_uprt_seed:
        wn_key = df["Well Name"].astype(str).str.strip()
        uprt_off = wn_key.map(day_seq_uprt_seed).fillna(0).astype(int)
    else:
        uprt_off = 0
    df["Day Seq UPRT"] = (
        gas_positive.groupby(df["Well Name"]).cumsum() + uprt_off
    ).clip(lower=1)

    total_wells = df["Well Name"].nunique()
    print(lf.detail(f"Sequences calculated for {lf.num(total_wells)} wells"))
    return df


def calculate_cumulatives(df, *, cum_seeds: Optional[Dict[str, Dict[str, float]]] = None):
    """
    Calculate cumulative totals for each well.

    For each well, cumulative columns are simple running totals
    over time that NEVER reset within a well and always reflect
    the sum of the daily values up to and including that date.

    ``cum_seeds`` adds per-well starting totals (last row before a patch window).
    """
    cum_seeds = cum_seeds or {}
    df = df.sort_values(["Well Name", "Date"]).reset_index(drop=True)

    for source_col, target_col in _CUMULATIVE_PAIRS:
        values = pd.to_numeric(df[source_col], errors="coerce").fillna(0.0)
        running = values.groupby(df["Well Name"]).cumsum()
        if cum_seeds:
            wn_key = df["Well Name"].astype(str).str.strip()
            seed = wn_key.map(
                lambda w: cum_seeds.get(w, {}).get(target_col, 0.0)
            ).fillna(0.0)
            df[target_col] = running + seed
        else:
            df[target_col] = running

    return df

def calculate_monthly_averages(df):
    """
    Calculate per-well averages over each **calendar month** (not a trailing 30‑day window).
    Rows are grouped with ``pandas Period('M')`` on ``Date`` (~ ``YEAR``/``MONTH`` semantics).
    Vectorized via groupby().transform('mean').
    """
    print(lf.step("Calculating monthly averages..."))
    df['_YM'] = pd.to_datetime(df['Date']).dt.to_period('M')

    monthly_avgs = [
        ('Gas WH Production (10³m³)', 'Gas WH Avg (10³m³)'),
        ('Gas S2 Production (10³m³)', 'Gas S2 Avg (10³m³)'),
        ('Gathered Gas (e³m³/d)', 'Gas Gathered Avg (e³m³/d)'),
        ('Gathered Condensate (m³/d)', 'Condensate Gathered Avg (m³/d)'),
        ('Gath. Water Rate (m³/d)', 'Gath. Water Avg (m³/d)'),
        ('Alloc. Water Rate (m³)', 'Alloc. Water Avg (m³)'),
    ]

    group_keys = [df['Well Name'], df['_YM']]
    for source_col, avg_col in monthly_avgs:
        numeric = pd.to_numeric(df[source_col], errors='coerce').fillna(0)
        df[avg_col] = numeric.groupby(group_keys).transform('mean')

    df = df.drop(columns=['_YM'])
    print(lf.detail(f"Monthly averages calculated for {lf.num(df['Well Name'].nunique())} wells (vectorized)"))
    return df

def add_on_production_year(df, year_by_well: Optional[Dict[str, int]] = None):
    """
    Add On Production Year column (year of first production date per well).

    When ``year_by_well`` is supplied (from existing PCE_Production), use it;
    otherwise fall back to the minimum date in *df* per well.
    """
    year_by_well = dict(year_by_well or {})
    df = df.copy()
    chunk_first = pd.to_datetime(df.groupby("Well Name")["Date"].min()).dt.year
    for wn, yr in chunk_first.items():
        key = str(wn).strip()
        if key not in year_by_well:
            year_by_well[key] = int(yr)
    df["On Production Year"] = df["Well Name"].astype(str).str.strip().map(year_by_well)
    return df


_MONTHLY_AVG_COLUMNS: Tuple[str, ...] = (
    "Gas WH Avg (10³m³)",
    "Gas S2 Avg (10³m³)",
    "Gas Gathered Avg (e³m³/d)",
    "Condensate Gathered Avg (m³/d)",
    "Gath. Water Avg (m³/d)",
    "Alloc. Water Avg (m³)",
)

PRODUCTION_SEQUENCE_RECALC_COLUMNS: Tuple[str, ...] = (
    "Days Seq",
    "Day Seq UPRT",
    *(target for _, target in _CUMULATIVE_PAIRS),
    *_MONTHLY_AVG_COLUMNS,
    "On Production Year",
)

_PRODUCTION_SEQUENCE_SOURCE_COLUMNS: Tuple[str, ...] = (
    "Date",
    "Well Name",
    "Gas WH Production (10³m³)",
    "Gas S2 Production (10³m³)",
    "Gas Sales Production (10³m³)",
    "Condensate Sales (m³/d)",
    "Condensate WH (m³/d)",
    "Gathered Gas (e³m³/d)",
    "Gathered Condensate (m³/d)",
    "Gath. Water Rate (m³/d)",
    "Alloc. Water Rate (m³)",
)

_PRODUCTION_SEQUENCE_SELECT_SQL = (
    "SELECT "
    + ", ".join(f"[{c}]" for c in _PRODUCTION_SEQUENCE_SOURCE_COLUMNS)
    + " FROM dbo.PCE_Production"
)

_SEQ_STAGING_TABLE = "#PCE_Production_Seq_Staging"


def _dataframe_for_sequence_source(df: pd.DataFrame) -> pd.DataFrame:
    """Narrow to columns required for from-scratch sequence / cumulative calcs."""
    out = df[list(_PRODUCTION_SEQUENCE_SOURCE_COLUMNS)].copy()
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.date
    return out


def _production_sequence_select_sql(*, exclude_well_names: Optional[List[str]] = None) -> Tuple[str, List]:
    """Build SELECT for sequence rebuild; optionally skip rolling-window wells."""
    sql = _PRODUCTION_SEQUENCE_SELECT_SQL
    params: List = []
    if exclude_well_names:
        names = sorted({str(w).strip() for w in exclude_well_names if str(w).strip()})
        if names:
            placeholders = ",".join("?" for _ in names)
            sql += f" WHERE [Well Name] NOT IN ({placeholders})"
            params.extend(names)
    sql += " ORDER BY [Well Name], [Date]"
    return sql, params


def build_production_sequence_update_sql(table_name: str = "dbo.PCE_Production") -> str:
    """UPDATE seq/cum/avg columns keyed on (Well Name, Date)."""
    sets = ", ".join(f"[{c}] = ?" for c in PRODUCTION_SEQUENCE_RECALC_COLUMNS)
    return f"""
UPDATE {table_name}
SET {sets}
WHERE [Well Name] = ? AND [Date] = ?
""".strip()


def apply_production_sequences_from_scratch(
    df: pd.DataFrame,
    *,
    for_persist: bool = False,
    log=None,
) -> pd.DataFrame:
    """
    Filter to each well's first production, then calculate Days Seq, Day Seq UPRT,
    cumulatives, monthly averages, and On Production Year with no carry-forward seeds.

    Shared by full rebuild (before insert) and routine full-table sequence rebuild.
    """
    if df.empty:
        return df
    out = filter_to_first_production(df.copy())
    if out.empty:
        return out
    out = calculate_sequences(out)
    out = calculate_cumulatives(out)
    out = calculate_monthly_averages(out)
    out = add_on_production_year(out)
    if for_persist:
        out["Days Seq"] = pd.to_numeric(out["Days Seq"], errors="coerce").fillna(0).astype(int)
        out["Day Seq UPRT"] = (
            pd.to_numeric(out["Day Seq UPRT"], errors="coerce").fillna(0).astype(int)
        )
        out["On Production Year"] = pd.to_numeric(out["On Production Year"], errors="coerce")
    return out


def stamp_production_sequence_placeholders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Placeholder seq/cum/avg values for routine window insert.

    Routine update recalculates these columns for every well in a full-table pass
    after the window rows are written.
    """
    out = df.copy()
    out["Days Seq"] = 0
    out["Day Seq UPRT"] = 1
    for _, target in _CUMULATIVE_PAIRS:
        out[target] = 0.0
    for col in _MONTHLY_AVG_COLUMNS:
        out[col] = np.nan
    out["On Production Year"] = np.nan
    return out


def _seq_staging_column_defs() -> List[str]:
    int_cols = {"Days Seq", "Day Seq UPRT", "On Production Year"}
    defs = ["[Well Name] NVARCHAR(4000) NOT NULL", "[Date] DATE NOT NULL"]
    for col in PRODUCTION_SEQUENCE_RECALC_COLUMNS:
        sql_type = "INT NULL" if col in int_cols else "FLOAT NULL"
        defs.append(f"[{col}] {sql_type}")
    return defs


def _create_seq_staging_table(cursor) -> None:
    cursor.execute(
        "IF OBJECT_ID('tempdb..#PCE_Production_Seq_Staging') IS NOT NULL "
        "DROP TABLE #PCE_Production_Seq_Staging"
    )
    cursor.execute(
        f"CREATE TABLE {_SEQ_STAGING_TABLE} ({', '.join(_seq_staging_column_defs())})"
    )


def _seq_staging_insert_sql() -> str:
    cols = ["Well Name", "Date", *PRODUCTION_SEQUENCE_RECALC_COLUMNS]
    col_list = ", ".join(f"[{c}]" for c in cols)
    placeholders = ", ".join("?" for _ in cols)
    return f"INSERT INTO {_SEQ_STAGING_TABLE} ({col_list}) VALUES ({placeholders})"


def _seq_bulk_update_from_staging_sql() -> str:
    set_clause = ", ".join(
        f"p.[{col}] = s.[{col}]" for col in PRODUCTION_SEQUENCE_RECALC_COLUMNS
    )
    return f"""
UPDATE p
SET {set_clause}
FROM dbo.PCE_Production AS p
INNER JOIN {_SEQ_STAGING_TABLE} AS s
    ON p.[Well Name] = s.[Well Name]
   AND CAST(p.[Date] AS DATE) = s.[Date]
""".strip()


def _sequence_staging_insert_rows(df: pd.DataFrame) -> List[Tuple]:
    cols = ["Well Name", "Date", *PRODUCTION_SEQUENCE_RECALC_COLUMNS]
    sub = df[cols].astype(object)
    sub[sub.isna()] = None
    return list(sub.itertuples(index=False, name=None))


def _apply_sequence_updates_via_staging(
    conn,
    df: pd.DataFrame,
    *,
    log=None,
    progress: Optional[Callable[[int], None]] = None,
) -> int:
    """Bulk INSERT into temp staging, then one UPDATE … JOIN (fast path vs row-wise UPDATE)."""
    from pce_production_schema import batch_executemany

    log_fn = log or print
    if df.empty:
        return 0

    rows = _sequence_staging_insert_rows(df)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    _create_seq_staging_table(cursor)
    insert_sql = _seq_staging_insert_sql()
    batch_executemany(
        cursor,
        insert_sql,
        rows,
        log=log_fn,
        label="Sequence staging load",
        progress=progress,
        progress_lo=0,
        progress_hi=85,
    )
    log_fn(lf.detail("Applying staged sequence values to PCE_Production (single UPDATE … JOIN)…"))
    cursor.execute(_seq_bulk_update_from_staging_sql())
    updated = cursor.rowcount
    if updated < 0:
        cursor.execute("SELECT @@ROWCOUNT")
        row = cursor.fetchone()
        updated = int(row[0]) if row else len(rows)
    if progress:
        progress(100)
    return int(updated)


def fetch_pce_production_for_sequence_rebuild(
    conn,
    *,
    exclude_well_names: Optional[List[str]] = None,
    log=None,
):
    """Load PCE_Production rows needed to recalculate sequences."""
    log_fn = log or print
    count_sql = "SELECT COUNT(*) FROM dbo.PCE_Production"
    count_params: List = []
    excluded_names: List[str] = []
    if exclude_well_names:
        excluded_names = sorted(
            {str(w).strip() for w in exclude_well_names if str(w).strip()}
        )
        if excluded_names:
            placeholders = ",".join("?" for _ in excluded_names)
            count_sql += f" WHERE [Well Name] NOT IN ({placeholders})"
            count_params.extend(excluded_names)
    count_cur = conn.cursor()
    count_cur.execute(count_sql, count_params or None)
    row_count = count_cur.fetchone()[0] or 0
    scope = (
        f"excluding {lf.num(len(excluded_names))} rolling-window well(s)"
        if excluded_names
        else "all wells"
    )
    load_msg = (
        f"Loading {lf.num(row_count)} PCE_Production row(s) for sequence rebuild ({scope})"
    )
    query, params = _production_sequence_select_sql(exclude_well_names=exclude_well_names)
    with lf.activity_log(log_fn, load_msg):
        df = pd.read_sql(query, conn, params=params or None)
    if not df.empty:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
    log_fn(lf.detail(f"Loaded {lf.num(len(df))} production row(s)"))
    return df


def _prepare_production_sequence_updates(df: pd.DataFrame) -> pd.DataFrame:
    """Recalculate seq/cum/avgs from production rates (no carry-forward seeds)."""
    return apply_production_sequences_from_scratch(df, for_persist=True)


def _production_sequence_update_rows(df: pd.DataFrame) -> List[Tuple]:
    """Build UPDATE parameter tuples: recalc columns, then Well Name and Date."""
    return _sequence_staging_insert_rows(df)


def _combine_sequence_update_frames(frames: List[pd.DataFrame]) -> pd.DataFrame:
    parts = [f for f in frames if f is not None and not f.empty]
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


def rebuild_all_production_sequences_from_scratch(
    *,
    conn=None,
    log=None,
    cancel_event=None,
    progress: Optional[Callable[[int], None]] = None,
    window_cda_for_seq: Optional[pd.DataFrame] = None,
    window_well_names: Optional[List[str]] = None,
) -> dict:
    """
    Recalculate Days Seq, Day Seq UPRT, cumulatives, monthly averages, and
    On Production Year for **every** well in PCE_Production from scratch.

    Routine update passes ``window_cda_for_seq`` (full CDA history already in
    memory for rolling-window wells) so those wells skip a production-table
    read; all other wells load from PCE_Production. Updates use a temp staging
    table and a single UPDATE … JOIN.

    Full rebuild calculates sequences during the CDA → production build and does
    not call this function.
    """
    log_fn = log or print

    def aborted():
        return cancel_event is not None and cancel_event.is_set()

    own_conn = conn is None
    if own_conn:
        conn = get_sql_conn()

    try:
        if aborted():
            return {"ok": False, "cancelled": True, "rows_updated": 0, "wells": 0}

        activity_msg = (
            "Recalculating Days Seq, Day Seq UPRT, cumulatives, and monthly "
            "averages for all wells in PCE_Production (from scratch)"
        )
        log_fn(lf.step(activity_msg))
        seq_frames: List[pd.DataFrame] = []
        window_names = sorted(
            {str(w).strip() for w in (window_well_names or []) if str(w).strip()}
        )

        if window_cda_for_seq is not None and not window_cda_for_seq.empty:
            log_fn(
                lf.detail(
                    f"Rolling-window wells ({lf.num(len(window_names))}): "
                    "sequence calcs from in-memory CDA (skip production read)"
                )
            )
            with lf.activity_log(log_fn, "Calculating sequences for rolling-window wells"):
                window_source = _dataframe_for_sequence_source(window_cda_for_seq)
                window_seq = _prepare_production_sequence_updates(window_source)
            if not window_seq.empty:
                seq_frames.append(window_seq)

        other_df = fetch_pce_production_for_sequence_rebuild(
            conn,
            exclude_well_names=window_names or None,
            log=log_fn,
        )
        if not other_df.empty:
            with lf.activity_log(log_fn, "Calculating sequences for remaining wells"):
                other_seq = _prepare_production_sequence_updates(other_df)
            if not other_seq.empty:
                seq_frames.append(other_seq)
        elif not window_names and other_df.empty:
            log_fn(lf.detail("PCE_Production empty; nothing to recalculate."))
            return {"ok": True, "rows_updated": 0, "wells": 0}

        combined = _combine_sequence_update_frames(seq_frames)

        if combined.empty:
            log_fn(lf.warn("No production rows after first-production filter."))
            return {"ok": True, "rows_updated": 0, "wells": 0}

        wells = int(combined["Well Name"].nunique())
        with lf.activity_log(log_fn, "Writing sequence updates to PCE_Production"):
            updated = _apply_sequence_updates_via_staging(
                conn,
                combined,
                log=log_fn,
                progress=progress,
            )
            conn.commit()
        log_fn(
            lf.success(
                f"Sequence rebuild complete — {lf.num(updated)} row(s) updated, "
                f"{lf.num(wells)} well(s)"
            )
        )
        return {"ok": True, "rows_updated": updated, "wells": wells}
    finally:
        if own_conn and conn is not None:
            conn.close()


_INSERT_COLS = list(PCE_PRODUCTION_INSERT_COLUMNS)


def insert_pce_production(
    df,
    *,
    progress: Optional[_RebuildProgress] = None,
    uwi_lookup=None,
    enersight_lookup=None,
    conn=None,
):
    """
    Insert dataframe into PCE_Production table.
    Vectorized NaN->None conversion + executemany batches.
    """
    if df.empty:
        print(lf.detail("No rows to insert"))
        return 0

    insert_sql = build_production_insert_sql()

    # Ensure int columns are cast properly before NaN->None conversion
    df = df.copy()
    if "UWI" not in df.columns:
        df = apply_uwi_from_well_master(df, uwi_lookup=uwi_lookup)
    df = apply_gathered_prd_month_labels(df, enersight_lookup=enersight_lookup)
    df['Days Seq'] = pd.to_numeric(df['Days Seq'], errors='coerce').fillna(0).astype(int)
    df['Day Seq UPRT'] = pd.to_numeric(df['Day Seq UPRT'], errors='coerce').fillna(0).astype(int)
    df['On Production Year'] = pd.to_numeric(df['On Production Year'], errors='coerce')

    sub = df[_INSERT_COLS].astype(object)
    sub[sub.isna()] = None
    rows_to_insert = list(sub.itertuples(index=False, name=None))

    batch_size = 20000
    commit_every_rows = 100000
    total_inserted = 0
    duplicate_skipped = 0

    own_conn = conn is None
    if own_conn:
        conn = get_sql_conn()
    try:
        cursor = conn.cursor()
        cursor.fast_executemany = True

        total_rows = len(rows_to_insert)

        for i in range(0, total_rows, batch_size):
            batch = rows_to_insert[i:i + batch_size]
            try:
                cursor.executemany(insert_sql, batch)
                total_inserted += len(batch)
            except Exception as batch_e:
                print(lf.warn(f"Batch at row {i} failed ({batch_e}); row-by-row fallback."))
                for j, row in enumerate(batch):
                    try:
                        cursor.execute(insert_sql, row)
                        total_inserted += 1
                    except Exception as row_e:
                        if "Violation of UNIQUE KEY" in str(row_e):
                            duplicate_skipped += 1
                        else:
                            print(lf.error(f"Error inserting row {i+j}: {row_e}"))

            rows_done = i + len(batch)
            if rows_done % commit_every_rows == 0 or rows_done == total_rows:
                conn.commit()

            if progress is not None:
                progress.phase_part("insert", rows_done, total_rows)
            if rows_done % batch_size == 0 or rows_done == total_rows:
                pct = int(100 * rows_done / total_rows) if total_rows else 100
                print(
                    lf.detail(
                        f"PCE_Production insert progress: "
                        f"{lf.num(rows_done)}/{lf.num(total_rows)} ({pct}%)"
                    )
                )
    finally:
        if own_conn and conn is not None and hasattr(conn, "close"):
            conn.close()

    if progress is not None:
        progress.phase_done("insert")
    print(lf.success(f"Inserted {lf.num(total_inserted)} rows into PCE_Production"))
    if duplicate_skipped > 0:
        print(lf.warn(f"Skipped {lf.num(duplicate_skipped)} duplicate rows"))

    return total_inserted

def main(cancel_event=None, progress_callback=None, data_lag_days=None, log_callback=None):
    """
    Rebuild PCE_Production from PCE_CDA.

    First repaints Gas S2, gas sales, condensate sales, and Sales CGR on PCE_CDA
    from Allocation_Factors (when AF rows exist), then clears and repopulates
    PCE_Production from CDA.

    Optional cancel_event (threading.Event): checked between major steps for
    best-effort cooperative cancel.
    """
    t0 = time.time()
    log = log_callback or print

    def aborted():
        return cancel_event is not None and cancel_event.is_set()

    def _duration():
        return time.time() - t0

    base_meta = {"mode": "full_rebuild", "duration_seconds": _duration()}

    timer = lf.StepTimer(log_fn=log)
    progress = _RebuildProgress(progress_callback)
    progress.emit(0)

    log(lf.header("PCE_Production population", Started=lf.timestamp()))
    if aborted():
        log(lf.warn("Cancelled before start."))
        return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

    end_cap = prodview_effective_end_date(data_lag_days)
    lag_days = PRODVIEW_DATA_LAG_DAYS if data_lag_days is None else int(data_lag_days)
    from prodview_update_gui import (
        query_pce_cda_max_date,
        query_pce_cda_min_date,
        refresh_full_rebuild_cda,
    )

    cda_min_before = query_pce_cda_min_date()
    cda_max_before = query_pce_cda_max_date()
    log(lf.detail(f"PCE_CDA date span before Snowflake: {cda_min_before or '—'} → {cda_max_before or '—'}"))
    log(lf.detail(f"Automatic end date (today − {lag_days} day(s)): {end_cap}"))

    window_start = window_end = end_cap
    rows_inserted = 0
    df = None

    with get_sql_conn() as conn:
        sf_start, sf_end, _cda_rows = refresh_full_rebuild_cda(
            log_callback=log,
            progress_callback=progress.snowflake_substep,
            data_lag_days=data_lag_days,
            conn=conn,
        )
        window_start, window_end = sf_start, sf_end
        progress.phase_done("snowflake")
        cda_max_after = query_pce_cda_max_date()
        log(lf.detail(f"PCE_CDA max date after Snowflake refresh: {cda_max_after or '—'}"))
        timer.mark("Snowflake → PCE_CDA full lifespan refresh")

        if aborted():
            log(lf.warn("Cancelled after Snowflake CDA refresh."))
            return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

        if not _refresh_cda_sales_from_allocation_factors(
            log=log,
            cancel_event=cancel_event,
            update_production=False,
            progress=progress,
            conn=conn,
        ):
            return {**base_meta, "cancelled": True, "duration_seconds": _duration()}
        timer.mark("PCE_CDA sales refresh from Allocation_Factors")

        if aborted():
            log(lf.warn("Cancelled after PCE_CDA sales refresh."))
            return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

        if sf_end > end_cap:
            cur = conn.cursor()
            cur.execute("DELETE FROM PCE_CDA WHERE ProdDate > ?", (end_cap,))
            n_trim = cur.rowcount or 0
            conn.commit()
            if n_trim:
                log(lf.detail(f"Trimmed {lf.num(n_trim)} PCE_CDA row(s) after {end_cap} (automatic end)"))
            else:
                log(lf.detail(f"No future PCE_CDA rows after {end_cap}"))
        else:
            log(lf.detail(f"Skipping CDA future trim (Snowflake already capped at {sf_end})"))
        timer.mark("Trim future CDA rows")

        if aborted():
            log(lf.warn("Cancelled after trimming future CDA."))
            return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

        log(lf.step("Rebuilding PCE_Production from PCE_CDA…"))
        progress.emit(65)
        progress.emit(66)

        clear_pce_production(conn=conn)
        timer.mark("Clear PCE_Production")
        if aborted():
            log(lf.warn("Cancelled after clearing PCE_Production."))
            return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

        progress.emit(67)

        log(lf.step("Loading well mappings from PCE_WM…"))
        wm_lookups = fetch_well_master_lookups(conn)
        composite_map = wm_lookups["composite_map"]
        fallback_map = wm_lookups["fallback_map"]
        log(lf.detail(f"Loaded {lf.num(len(fallback_map))} well name mappings"))
        timer.mark("Load well mappings")
        if aborted():
            log(lf.warn("Cancelled after loading well mappings."))
            return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

        progress.emit(68)

        df = fetch_cda_data(end_cap=end_cap, log=log, conn=conn)
        timer.mark("Load PCE_CDA into pandas")
        progress.emit(70)

        if df.empty:
            log(lf.warn("No data to process. Exiting."))
            return {
                **base_meta,
                "skipped": True,
                "reason": "No rows in PCE_CDA",
                "duration_seconds": _duration(),
            }

        log(lf.step("Applying well name, pad, and UWI mappings…"))
        df = apply_well_names(df, composite_map, fallback_map)
        df = apply_pad_name_from_well_master(df, wm_lookups["pad_lookup"])
        df = apply_uwi_from_well_master(df, wm_lookups["uwi_lookup"])
        timer.mark("Well name / pad / UWI mapping")
        progress.emit(71)

        if df.empty:
            log(lf.warn("No data after well name mapping. Exiting."))
            return {
                **base_meta,
                "skipped": True,
                "reason": "No data after well name mapping",
                "duration_seconds": _duration(),
            }

        if aborted():
            log(lf.warn("Cancelled before sequence calculations."))
            return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

        log(
            lf.step(
                f"Calculating sequences, cumulatives, and monthly averages "
                f"({lf.num(len(df))} row(s))…"
            )
        )
        calc_msg = (
            f"Calculating sequences and cumulatives for {lf.num(len(df))} row(s)"
        )
        with lf.activity_log(log, calc_msg):
            df = apply_production_sequences_from_scratch(df, for_persist=True)
        progress.emit(74)
        timer.mark("Sequences, cumulatives, monthly avgs")
        progress.phase_done("prep")

        if aborted():
            log(lf.warn("Cancelled before inserting into PCE_Production."))
            return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

        log(lf.step(f"Inserting {lf.num(len(df))} row(s) into PCE_Production…"))
        rows_inserted = insert_pce_production(
            df,
            progress=progress,
            uwi_lookup=wm_lookups["uwi_lookup"],
            enersight_lookup=wm_lookups["enersight_lookup"],
            conn=conn,
        )
        timer.mark("Insert PCE_Production")

        from pce_rebuild_pipeline import run_post_production_rebuild_steps

        if not run_post_production_rebuild_steps(
            log,
            conn=conn,
            date_window=(window_start, window_end),
            cancel_event=cancel_event,
        ):
            return {**base_meta, "cancelled": True, "duration_seconds": _duration()}
        timer.mark("Post-production rebuild steps")

    progress.phase_done("finalize")

    wells_processed = len(df["Well Name"].unique())
    total_records = len(df)

    # Step 12: Final summary
    cda_max_final = query_pce_cda_max_date()
    log(lf.summary("Complete", {
        "Completed": lf.timestamp(),
        "Automatic end date": end_cap,
        "PCE_CDA max date": cda_max_final or "—",
        "Wells processed": wells_processed,
        "Total records": total_records,
        "Records inserted": rows_inserted,
        "Destination": f"{SQL_SERVER}.{SQL_DATABASE}.PCE_Production",
    }))

    return {
        **base_meta,
        "wells_processed": wells_processed,
        "total_records": total_records,
        "records_inserted": rows_inserted,
        "duration_seconds": _duration(),
    }


if __name__ == "__main__":
    out = main()
    if out:
        print(lf.detail(f"Exit summary: {out}"))