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
    out["Month"] = keys.map(lambda k: gathered_prd_month_label(lookup.get(k)))
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
):
    """
    Repaint Gas S2, gas sales, condensate sales, and Sales CGR on PCE_CDA using
    Allocation_Factors (same logic as PA + Public Sales ratio passes).

    When ``date_window`` is set, only months overlapping that inclusive range are
    processed (routine update). Otherwise all AF months run (full rebuild).

    Returns False if cancelled mid-loop; True otherwise (including when AF is empty).
    """
    from sales_allocation_updates import (
        apply_full_sales_ratios_for_month,
        apply_valnav_allocation_to_cda_and_production,
    )

    def aborted():
        return cancel_event is not None and cancel_event.is_set()

    with get_sql_conn() as conn:
        cursor = conn.cursor()
        if date_window is not None:
            months = _allocation_factor_months_overlapping(
                cursor, date_window[0], date_window[1]
            )
        else:
            cursor.execute(
                "SELECT DISTINCT MonthStartDate FROM Allocation_Factors ORDER BY MonthStartDate"
            )
            months = [row[0] for row in cursor.fetchall()]

    if not months:
        if date_window is not None:
            log(
                lf.warn(
                    "No Allocation_Factors rows overlap the rolling window; "
                    "PCE_CDA Gas S2 / sales columns are unchanged."
                )
            )
        else:
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
            f"Refreshing PCE_CDA from Allocation_Factors ({lf.num(len(months))} month(s), "
            f"{scope}: Gas S2, gas sales, condensate sales, Sales CGR)…"
        )
    )

    n = len(months)
    with get_sql_conn() as conn:
        for i, month_start in enumerate(months):
            if aborted():
                log(lf.warn("Cancelled during PCE_CDA sales refresh."))
                return False
            apply_valnav_allocation_to_cda_and_production(
                conn, month_start, log=log, update_production=update_production
            )
            apply_full_sales_ratios_for_month(
                conn, month_start, log=log, update_production=update_production
            )
            if progress is not None:
                progress.phase_part("cda_sales", i + 1, n)
            if n <= 24 or (i + 1) % 12 == 0 or (i + 1) == n:
                log(lf.detail(f"CDA sales refresh progress: {i + 1}/{n} months"))

    if progress is not None:
        progress.phase_done("cda_sales")
    return True


def _refresh_ngl_from_allocation_factors(
    log=print,
    cancel_event=None,
):
    """
    Repaint NGL ratio columns on PCE_Production from Allocation_Factors monthly volumes.

    Runs every distinct month in AF that has any NGL volume. Returns False if cancelled
    mid-loop; True otherwise (including when no NGL months exist).
    """
    from ngl_monthly_update import run_ngl_monthly_from_allocation_factors

    def aborted():
        return cancel_event is not None and cancel_event.is_set()

    with get_sql_conn() as conn:
        cursor = conn.cursor()
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
        months = [row[0] for row in cursor.fetchall()]

    if not months:
        log(lf.warn("No Allocation_Factors NGL rows; PCE_Production NGL ratios unchanged."))
        return True

    log(
        lf.step(
            f"Refreshing PCE_Production NGL ratios from Allocation_Factors "
            f"({lf.num(len(months))} months)…"
        )
    )

    n = len(months)
    with get_sql_conn() as conn:
        for i, month_start in enumerate(months):
            if aborted():
                log(lf.warn("Cancelled during NGL ratio refresh."))
                return False
            run_ngl_monthly_from_allocation_factors(conn, month_start, log=log)
            if n <= 24 or (i + 1) % 12 == 0 or (i + 1) == n:
                log(lf.detail(f"NGL refresh progress: {i + 1}/{n} months"))

    return True


def clear_pce_production():
    """Clear all data from PCE_Production table"""
    with get_sql_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM PCE_Production")
        existing = cursor.fetchone()[0] or 0
        print(
            lf.step(
                f"Clearing PCE_Production ({lf.num(existing)} row(s); "
                "this may take several minutes)…"
            )
        )
        # Delete all rows
        cursor.execute("DELETE FROM PCE_Production")
        deleted = cursor.rowcount
        # Reset identity/ID column so new rows start from 1 again (if table has an IDENTITY)
        try:
            cursor.execute("DBCC CHECKIDENT('PCE_Production', RESEED, 0)")
        except Exception:
            # If the table has no IDENTITY or permissions are limited, ignore the error
            pass
        conn.commit()
        print(lf.success(f"Cleared PCE_Production: {lf.num(deleted)} records deleted (identity reseeded where applicable)"))
        return deleted


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
            count_cur = conn.cursor()
            count_cur.execute("SELECT COUNT(*) FROM PCE_CDA")
            cda_rows = count_cur.fetchone()[0] or 0
            log_fn(
                lf.step(
                    f"Loading {lf.num(cda_rows)} PCE_CDA row(s) from SQL Server "
                    "(full table read; this may take several minutes)…"
                )
            )
            df = pd.read_sql(_CDA_SELECT_SQL, conn)
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

        if well_names is None and start_date is not None and not df.empty:
            start_series = pd.to_datetime(df["Date"], errors="coerce").dt.date
            before = len(df)
            df = df.loc[start_series >= start_date].copy()
            dropped = before - len(df)
            if dropped:
                log_fn(lf.detail(f"Excluded {lf.num(dropped)} CDA row(s) before {start_date}"))

        if well_names is None and end_cap is not None and not df.empty:
            cap_series = pd.to_datetime(df["Date"], errors="coerce").dt.date
            before = len(df)
            df = df.loc[cap_series <= end_cap].copy()
            dropped = before - len(df)
            if dropped:
                log_fn(lf.detail(f"Excluded {lf.num(dropped)} CDA row(s) after {end_cap}"))

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
):
    """
    Set ``PCE_Production`` WM-backed metadata in one table scan.

    Optional ``date_start``/``date_end`` restrict which rows are updated (same
    semantics as the legacy per-field sync helpers).
    """
    if not any((update_pad, update_enersight, update_month)):
        return

    date_filter = ""
    params = []
    if date_start is not None and date_end is not None:
        date_filter = " AND p.[Date] BETWEEN ? AND ?"
        params = [date_start, date_end]

    month_expr = gathered_prd_month_sql_from_enersight("ca.en")
    enersight_val = "NULLIF(LTRIM(RTRIM(CAST(ca.en AS NVARCHAR(4000)))), N'')"

    set_parts = []
    if update_pad:
        set_parts.append(f"p.[Pad Name] = {production_pad_sql_from_wm('ca.pad')}")
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

    seq_off = df["Well Name"].map(lambda w: days_seq_seed.get(str(w).strip(), 0))
    df["Days Seq"] = df.groupby("Well Name").cumcount() + 1 + seq_off

    gas_positive = (
        pd.to_numeric(df["Gas WH Production (10³m³)"], errors="coerce")
        .fillna(0)
        .gt(0)
        .astype(int)
    )
    uprt_off = df["Well Name"].map(lambda w: day_seq_uprt_seed.get(str(w).strip(), 0))
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
        seed = df["Well Name"].map(
            lambda w: cum_seeds.get(str(w).strip(), {}).get(target_col, 0.0)
        )
        df[target_col] = values.groupby(df["Well Name"]).cumsum() + seed

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
    + " FROM dbo.PCE_Production ORDER BY [Well Name], [Date]"
)


def build_production_sequence_update_sql(table_name: str = "dbo.PCE_Production") -> str:
    """UPDATE seq/cum/avg columns keyed on (Well Name, Date)."""
    sets = ", ".join(f"[{c}] = ?" for c in PRODUCTION_SEQUENCE_RECALC_COLUMNS)
    return f"""
UPDATE {table_name}
SET {sets}
WHERE [Well Name] = ? AND [Date] = ?
""".strip()


def fetch_pce_production_for_sequence_rebuild(conn, *, log=None):
    """Load all PCE_Production rows needed to recalculate sequences."""
    log_fn = log or print
    count_cur = conn.cursor()
    count_cur.execute("SELECT COUNT(*) FROM dbo.PCE_Production")
    row_count = count_cur.fetchone()[0] or 0
    log_fn(
        lf.step(
            f"Loading {lf.num(row_count)} PCE_Production row(s) for sequence rebuild…"
        )
    )
    df = pd.read_sql(_PRODUCTION_SEQUENCE_SELECT_SQL, conn)
    if not df.empty:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
    log_fn(lf.detail(f"Loaded {lf.num(len(df))} production row(s)"))
    return df


def _prepare_production_sequence_updates(df: pd.DataFrame) -> pd.DataFrame:
    """Recalculate seq/cum/avgs from production rates (no carry-forward seeds)."""
    if df.empty:
        return df
    df = filter_to_first_production(df.copy())
    if df.empty:
        return df
    df = calculate_sequences(df)
    df = calculate_cumulatives(df)
    df = calculate_monthly_averages(df)
    df = add_on_production_year(df)
    df["Days Seq"] = pd.to_numeric(df["Days Seq"], errors="coerce").fillna(0).astype(int)
    df["Day Seq UPRT"] = pd.to_numeric(df["Day Seq UPRT"], errors="coerce").fillna(0).astype(int)
    df["On Production Year"] = pd.to_numeric(df["On Production Year"], errors="coerce")
    return df


def _production_sequence_update_rows(df: pd.DataFrame) -> List[Tuple]:
    """Build UPDATE parameter tuples: recalc columns, then Well Name and Date."""
    cols = list(PRODUCTION_SEQUENCE_RECALC_COLUMNS) + ["Well Name", "Date"]
    sub = df[cols].astype(object)
    sub[sub.isna()] = None
    return list(sub.itertuples(index=False, name=None))


def rebuild_all_production_sequences_from_scratch(
    *,
    conn=None,
    log=None,
    cancel_event=None,
    progress: Optional[Callable[[int], None]] = None,
) -> dict:
    """
    Recalculate Days Seq, Day Seq UPRT, cumulatives, monthly averages, and
    On Production Year for **every** well in PCE_Production from existing rates.

    Intended to run after production rows are written (routine window insert,
    full rebuild, or type-curve materialization) so sequencing is consistent
    across the full table, not only the patched date span.
    """
    from pce_production_schema import batch_executemany

    log_fn = log or print

    def aborted():
        return cancel_event is not None and cancel_event.is_set()

    own_conn = conn is None
    if own_conn:
        conn = get_sql_conn()

    try:
        if aborted():
            return {"ok": False, "cancelled": True, "rows_updated": 0, "wells": 0}

        log_fn(
            lf.step(
                "Recalculating Days Seq, Day Seq UPRT, cumulatives, and monthly "
                "averages for all wells in PCE_Production (from scratch)…"
            )
        )
        df = fetch_pce_production_for_sequence_rebuild(conn, log=log_fn)
        if df.empty:
            log_fn(lf.detail("PCE_Production empty; nothing to recalculate."))
            return {"ok": True, "rows_updated": 0, "wells": 0}

        df = _prepare_production_sequence_updates(df)
        if df.empty:
            log_fn(lf.warn("No production rows after first-production filter."))
            return {"ok": True, "rows_updated": 0, "wells": 0}

        wells = int(df["Well Name"].nunique())
        update_rows = _production_sequence_update_rows(df)
        update_sql = build_production_sequence_update_sql()
        cursor = conn.cursor()
        cursor.fast_executemany = True
        batch_executemany(
            cursor,
            update_sql,
            update_rows,
            log=log_fn,
            label="PCE_Production sequence update",
            progress=progress,
            progress_lo=0,
            progress_hi=100,
        )
        conn.commit()
        log_fn(
            lf.success(
                f"Sequence rebuild complete — {lf.num(len(update_rows))} row(s), "
                f"{lf.num(wells)} well(s)"
            )
        )
        return {"ok": True, "rows_updated": len(update_rows), "wells": wells}
    finally:
        if own_conn and conn is not None:
            conn.close()


_INSERT_COLS = list(PCE_PRODUCTION_INSERT_COLUMNS)


def insert_pce_production(df, *, progress: Optional[_RebuildProgress] = None):
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
        df = apply_uwi_from_well_master(df)
    enersight_lookup = fetch_well_master_enersight_lookup()
    df = apply_gathered_prd_month_labels(df, enersight_lookup)
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

    with get_sql_conn() as conn:
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

    sf_start, sf_end, _cda_rows = refresh_full_rebuild_cda(
        log_callback=print,
        progress_callback=progress.snowflake_substep,
        data_lag_days=data_lag_days,
    )
    progress.phase_done("snowflake")
    cda_max_after = query_pce_cda_max_date()
    log(lf.detail(f"PCE_CDA max date after Snowflake refresh: {cda_max_after or '—'}"))
    timer.mark("Snowflake → PCE_CDA full lifespan refresh")

    if aborted():
        log(lf.warn("Cancelled after Snowflake CDA refresh."))
        return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

    # Step 1: Ensure CDA sales / S2 columns match Allocation_Factors before copying to Production
    if not _refresh_cda_sales_from_allocation_factors(
        log=print,
        cancel_event=cancel_event,
        update_production=False,
        progress=progress,
    ):
        return {**base_meta, "cancelled": True, "duration_seconds": _duration()}
    timer.mark("PCE_CDA sales refresh from Allocation_Factors")

    if aborted():
        log(lf.warn("Cancelled after PCE_CDA sales refresh."))
        return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

    with get_sql_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM PCE_CDA WHERE ProdDate > ?", (end_cap,))
        n_trim = cur.rowcount or 0
        conn.commit()
    if n_trim:
        log(lf.detail(f"Trimmed {lf.num(n_trim)} PCE_CDA row(s) after {end_cap} (automatic end)"))
    else:
        log(lf.detail(f"No future PCE_CDA rows after {end_cap}"))
    timer.mark("Trim future CDA rows")

    if aborted():
        log(lf.warn("Cancelled after trimming future CDA."))
        return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

    log(lf.step("Rebuilding PCE_Production from PCE_CDA…"))
    progress.emit(65)

    window_start, window_end = sf_start, sf_end
    log(lf.step(f"Trimming future PCE_Production rows after {end_cap}…"))
    with get_sql_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            DELETE FROM PCE_Production
            WHERE [Date] > ?
              AND [Well Name] NOT LIKE '% - TC'
              AND [Well Name] NOT LIKE 'YE2%'
            """,
            (end_cap,),
        )
        n_prod_trim = cur.rowcount or 0
        conn.commit()
    if n_prod_trim:
        log(
            lf.detail(
                f"Trimmed {lf.num(n_prod_trim)} PCE_Production row(s) after {end_cap}"
            )
        )
    else:
        log(lf.detail(f"No future PCE_Production rows after {end_cap}"))
    timer.mark("Trim future PCE_Production rows")

    if aborted():
        log(lf.warn("Cancelled after trimming future production."))
        return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

    progress.emit(66)

    # Step 2: Clear existing data
    clear_pce_production()
    timer.mark("Clear PCE_Production")
    if aborted():
        log(lf.warn("Cancelled after clearing PCE_Production."))
        return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

    progress.emit(67)

    # Step 3: Fetch well name mappings
    log(lf.step("Loading well name mappings from PCE_WM…"))
    composite_map, fallback_map = fetch_well_mapping()
    timer.mark("Load well mappings")
    if aborted():
        log(lf.warn("Cancelled after loading well mappings."))
        return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

    progress.emit(68)

    # Step 4: Fetch CDA data
    df = fetch_cda_data(end_cap=end_cap, log=print)
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

    # Step 5: Apply well name mappings (composite name with fallback to well name)
    log(lf.step("Applying well name, pad, and UWI mappings…"))
    df = apply_well_names(df, composite_map, fallback_map)
    df = apply_pad_name_from_well_master(df)
    df = apply_uwi_from_well_master(df)
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

    # Step 6: Filter to first production date for each well
    before_first_prod = len(df)
    log(lf.step("Filtering to each well's first production date…"))
    df = filter_to_first_production(df)
    log(
        lf.detail(
            f"First-production filter: {lf.num(len(df))} row(s) "
            f"({lf.num(before_first_prod - len(df))} trimmed)"
        )
    )
    timer.mark("Filter to first production")
    progress.emit(72)

    if df.empty:
        log(lf.warn("No data after filtering. Exiting."))
        return {
            **base_meta,
            "skipped": True,
            "reason": "No data after first-production filter",
            "duration_seconds": _duration(),
        }

    if aborted():
        log(lf.warn("Cancelled before sequence calculations."))
        return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

    # Step 7: Calculate sequences with corrected Day Seq UPRT logic
    log(
        lf.step(
            f"Calculating sequences, cumulatives, and monthly averages "
            f"({lf.num(len(df))} row(s))…"
        )
    )
    df = calculate_sequences(df)
    progress.emit(73)

    # Step 8: Calculate cumulatives
    df = calculate_cumulatives(df)
    progress.emit(74)

    # Step 9: Calculate monthly averages
    df = calculate_monthly_averages(df)

    # Step 10: Add On Production Year
    df = add_on_production_year(df)
    timer.mark("Sequences, cumulatives, monthly avgs")
    progress.phase_done("prep")

    if aborted():
        log(lf.warn("Cancelled before inserting into PCE_Production."))
        return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

    # Step 11: Insert into PCE_Production
    log(lf.step(f"Inserting {lf.num(len(df))} row(s) into PCE_Production…"))
    rows_inserted = insert_pce_production(df, progress=progress)
    timer.mark("Insert PCE_Production")

    from pce_rebuild_pipeline import run_post_production_rebuild_steps

    if not run_post_production_rebuild_steps(
        log,
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