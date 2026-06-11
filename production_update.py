import time
import re
from typing import Callable, Optional

import pandas as pd
import numpy as np
from datetime import datetime
import warnings

import log_format as lf
from db_connection import get_sql_conn, SQL_DATABASE, SQL_SERVER
from prodview_date_bounds import prodview_effective_end_date

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


def fetch_well_master_enersight_lookup():
    """WM ``[Well Name]`` / ``[Composite Name]`` -> ``[Enersight Well Name]``."""
    query = """
    SELECT [Well Name], [Composite Name], [Enersight Well Name]
    FROM PCE_WM
    WHERE [Well Name] IS NOT NULL
      AND ([Exception] IS NULL OR [Exception] = '' OR [Exception] = 'N')
    """
    with get_sql_conn() as conn:
        wm = pd.read_sql(query, conn)

    lookup = {}
    for _, row in wm.iterrows():
        en = row["Enersight Well Name"]
        if pd.isna(en) or (isinstance(en, str) and not str(en).strip()):
            en_val = None
        else:
            en_val = str(en).strip()
        wn = str(row["Well Name"]).strip() if pd.notna(row["Well Name"]) else ""
        if wn:
            lookup[wn] = en_val
        comp = row["Composite Name"]
        if comp is not None and str(comp).strip():
            lookup[str(comp).strip()] = en_val
    return lookup


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


def _refresh_cda_sales_from_allocation_factors(
    log=print,
    cancel_event=None,
    update_production=True,
    progress: Optional[_RebuildProgress] = None,
):
    """
    Repaint Gas S2, gas sales, condensate sales, and Sales CGR on PCE_CDA using
    Allocation_Factors (same logic as PA + Public Sales ratio passes).

    Runs every distinct month in Allocation_Factors. Returns False if cancelled
    mid-loop; True otherwise (including when AF is empty).
    """
    from sales_allocation_updates import (
        apply_full_sales_ratios_for_month,
        apply_valnav_allocation_to_cda_and_production,
    )

    def aborted():
        return cancel_event is not None and cancel_event.is_set()

    with get_sql_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT DISTINCT MonthStartDate FROM Allocation_Factors ORDER BY MonthStartDate"
        )
        months = [row[0] for row in cursor.fetchall()]

    if not months:
        log(
            lf.warn(
                "No Allocation_Factors rows; PCE_CDA Gas S2 / sales columns are unchanged."
            )
        )
        return True

    log(
        lf.step(
            f"Refreshing PCE_CDA from Allocation_Factors ({lf.num(len(months))} months: "
            "Gas S2, gas sales, condensate sales, Sales CGR)…"
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


def fetch_cda_data(well_names=None, end_cap=None, conn=None, log=None):
    """
    Fetch daily production rows from PCE_CDA for rebuild.

    ``well_names``: optional list of CDA ``[Well Name]`` values; when set, only
    those wells are loaded (full history per well). When omitted, loads all CDA.
    Sorting is done in pandas (same order as the legacy SQL ORDER BY).
    """
    log_fn = log or print
    own_conn = conn is None
    if own_conn:
        conn = get_sql_conn()

    try:
        if well_names is not None and len(well_names) == 0:
            df = pd.DataFrame()
        elif well_names is None:
            df = pd.read_sql(_CDA_SELECT_SQL, conn)
        else:
            frames = []
            chunk_size = 500
            for i in range(0, len(well_names), chunk_size):
                chunk = well_names[i : i + chunk_size]
                placeholders = ",".join("?" for _ in chunk)
                query = f"{_CDA_SELECT_SQL} WHERE [Well Name] IN ({placeholders})"
                frames.append(pd.read_sql(query, conn, params=chunk))
            df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

        if end_cap is not None and not df.empty:
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
            log_fn(
                lf.detail(
                    f"Loaded {lf.num(len(df))} CDA rows for "
                    f"{lf.num(len(well_names))} well(s) in rolling window"
                )
            )
        return df
    finally:
        if own_conn and conn is not None:
            conn.close()


def fetch_well_mapping():
    """Fetch well name mappings from PCE_WM (Composite Name and Well Name)"""
    query = """
    SELECT 
        [Well Name] as SourceWell,
        [Composite Name],
        [Well Name] as FallbackWell
    FROM PCE_WM
    WHERE [Well Name] IS NOT NULL
      AND ([Exception] IS NULL OR [Exception] = '' OR [Exception] = 'N')
    """
    
    with get_sql_conn() as conn:
        df = pd.read_sql(query, conn)
    
    df['SourceWell'] = df['SourceWell'].astype(str).str.strip()
    df['FallbackWell'] = df['FallbackWell'].astype(str).str.strip()

    valid_composite = (
        df['Composite Name'].notna()
        & df['Composite Name'].astype(str).str.strip().ne('')
    )
    composite_stripped = df['Composite Name'].astype(str).str.strip()
    composite_map = dict(zip(
        df.loc[valid_composite, 'SourceWell'],
        composite_stripped[valid_composite],
    ))
    fallback_map = dict(zip(df['SourceWell'], df['FallbackWell']))
    
    print(lf.detail(f"Loaded {lf.num(len(fallback_map))} well name mappings"))
    
    return composite_map, fallback_map


def fetch_well_master_uwi_lookup():
    """Trimmed WM ``[Well Name]`` / ``[Composite Name]`` -> ``[Value Navigator UWI]``."""
    query = """
    SELECT [Well Name], [Composite Name], [Value Navigator UWI]
    FROM PCE_WM
    WHERE [Well Name] IS NOT NULL
      AND ([Exception] IS NULL OR [Exception] = '' OR [Exception] = 'N')
      AND NULLIF(LTRIM(RTRIM(CAST([Value Navigator UWI] AS NVARCHAR(4000)))), N'') IS NOT NULL
    """
    with get_sql_conn() as conn:
        wm = pd.read_sql(query, conn)

    lookup = {}
    for _, row in wm.iterrows():
        uwi = row["Value Navigator UWI"]
        if pd.isna(uwi) or not str(uwi).strip():
            continue
        uwi_val = str(uwi).strip()
        wn = str(row["Well Name"]).strip() if pd.notna(row["Well Name"]) else ""
        if wn:
            lookup[wn] = uwi_val
        comp = row["Composite Name"]
        if comp is not None and str(comp).strip():
            lookup[str(comp).strip()] = uwi_val
    return lookup


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


def fetch_well_master_pad_lookup():
    """Trimmed WM ``[Well Name]`` / ``[Composite Name]`` -> ``[Pad Name]``."""
    query = """
    SELECT [Well Name], [Composite Name], [Pad Name]
    FROM PCE_WM
    WHERE [Well Name] IS NOT NULL
      AND ([Exception] IS NULL OR [Exception] = '' OR [Exception] = 'N')
    """
    with get_sql_conn() as conn:
        wm = pd.read_sql(query, conn)

    lookup = {}
    for _, row in wm.iterrows():
        wn = str(row["Well Name"]).strip() if pd.notna(row["Well Name"]) else ""
        pad = row["Pad Name"]
        if pd.isna(pad) or (isinstance(pad, str) and not str(pad).strip()):
            pad_val = None
        else:
            pad_val = str(pad).strip()
        if wn:
            lookup[wn] = pad_val
        comp = row["Composite Name"]
        if comp is not None and str(comp).strip():
            lookup[str(comp).strip()] = pad_val
    return lookup


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

def calculate_sequences(df):
    """
    Calculate Days Seq and Day Seq UPRT for each well.
    Vectorized via groupby().cumcount() and cumsum().
    """
    df = df.sort_values(['Well Name', 'Date']).reset_index(drop=True)

    # Days Seq: simple per-well counter starting at 1
    df['Days Seq'] = df.groupby('Well Name').cumcount() + 1

    # Day Seq UPRT: cumulative count of production days (>0), floored at 1
    gas_positive = (
        pd.to_numeric(df['Gas WH Production (10³m³)'], errors='coerce')
        .fillna(0)
        .gt(0)
        .astype(int)
    )
    df['Day Seq UPRT'] = gas_positive.groupby(df['Well Name']).cumsum().clip(lower=1)

    total_wells = df['Well Name'].nunique()
    print(lf.detail(f"Sequences calculated for {lf.num(total_wells)} wells"))
    return df

def calculate_cumulatives(df):
    """
    Calculate cumulative totals for each well.

    For each well, cumulative columns are simple running totals
    over time that NEVER reset within a well and always reflect
    the sum of the daily values up to and including that date.
    """
    # Ensure data is sorted by well and date so running totals are stable
    df = df.sort_values(['Well Name', 'Date']).reset_index(drop=True)

    # List of source columns and their cumulative target columns
    cumulatives = [
        ('Gas WH Production (10³m³)', 'Gas WH Cumulative Production (10³m³)'),
        ('Gas S2 Production (10³m³)', 'Gas S2 Cumulative Production (10³m³)'),
        ('Gas Sales Production (10³m³)', 'Gas Sales Cumulative Production (10³m³)'),
        ('Condensate Sales (m³/d)', 'Condensate Sales Cumulative Production (m³)'),
        ('Condensate WH (m³/d)', 'Condensate WH Cumulative Production (m³)'),
        ('Gathered Gas (e³m³/d)', 'Gas Gathered Cumulative (e³m³)'),
        ('Gathered Condensate (m³/d)', 'Condensate Gathered Cumulative (m³)'),
        ('Gath. Water Rate (m³/d)', 'Gath. Water Cumulative (m³)'),
    ]

    # For each pair, compute per‑well running total that never resets
    for source_col, target_col in cumulatives:
        # Convert to numeric and fill NaNs with zero for clean sums
        values = pd.to_numeric(df[source_col], errors='coerce').fillna(0.0)
        df[target_col] = values.groupby(df['Well Name']).cumsum()

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

def add_on_production_year(df):
    """
    Add On Production Year column (year of first production date per well).
    Vectorized via groupby().transform('min').
    """
    first_dates = pd.to_datetime(
        df.groupby('Well Name')['Date'].transform('min')
    )
    df['On Production Year'] = first_dates.dt.year
    return df

_INSERT_COLS = [
    'Date', 'Days Seq', 'Day Seq UPRT', 'Well Name', 'UWI',
    'Gas WH Production (10³m³)', 'Condensate WH (m³/d)',
    'Gas S2 Production (10³m³)', 'Gas Sales Production (10³m³)',
    'Condensate Sales (m³/d)', 'Gathered Gas (e³m³/d)',
    'Gathered Condensate (m³/d)', 'Gath. Water Rate (m³/d)',
    'Sales CGR (m³/e³m³)',
    'CGR (m³/e³m³)', 'WGR (m³/e³m³)', 'ECF',
    'Hours On', 'Tubing Pressure (kPa)', 'Casing Pressure (kPa)',
    'Choke Size', 'Gas WH Cumulative Production (10³m³)',
    'Gas S2 Cumulative Production (10³m³)',
    'Gas Sales Cumulative Production (10³m³)',
    'Condensate Sales Cumulative Production (m³)',
    'Condensate WH Cumulative Production (m³)',
    'Gas Gathered Cumulative (e³m³)',
    'Condensate Gathered Cumulative (m³)',
    'Gath. Water Cumulative (m³)',
    'Formation Producer', 'Layer Producer', 'Fault Block',
    'Pad Name', 'Lateral Length', 'Orientation',
    'On Production Year', 'Alloc. Water Rate (m³)', 'NGL (m³)',
    'Gas WH Avg (10³m³)', 'Gas S2 Avg (10³m³)',
    'Gas Gathered Avg (e³m³/d)', 'Condensate Gathered Avg (m³/d)',
    'Gath. Water Avg (m³/d)',
    'Alloc. Water Avg (m³)',
    'Month',
]

def insert_pce_production(df, *, progress: Optional[_RebuildProgress] = None):
    """
    Insert dataframe into PCE_Production table.
    Vectorized NaN->None conversion + executemany batches.
    """
    if df.empty:
        print(lf.detail("No rows to insert"))
        return 0

    insert_sql = """
    INSERT INTO PCE_Production (
        [Date], [Days Seq], [Day Seq UPRT], [Well Name], [UWI],
        [Gas WH Production (10³m³)], [Condensate WH (m³/d)],
        [Gas S2 Production (10³m³)], [Gas Sales Production (10³m³)],
        [Condensate Sales (m³/d)], [Gathered Gas (e³m³/d)],
        [Gathered Condensate (m³/d)], [Gath. Water Rate (m³/d)],
        [Sales CGR (m³/e³m³)],
        [CGR (m³/e³m³)], [WGR (m³/e³m³)], [ECF],
        [Hours On], [Tubing Pressure (kPa)], [Casing Pressure (kPa)],
        [Choke Size], [Gas WH Cumulative Production (10³m³)],
        [Gas S2 Cumulative Production (10³m³)],
        [Gas Sales Cumulative Production (10³m³)],
        [Condensate Sales Cumulative Production (m³)],
        [Condensate WH Cumulative Production (m³)],
        [Gas Gathered Cumulative (e³m³)],
        [Condensate Gathered Cumulative (m³)],
        [Gath. Water Cumulative (m³)],
        [Formation Producer], [Layer Producer], [Fault Block],
        [Pad Name], [Lateral Length], [Orientation],
        [On Production Year], [Alloc. Water Rate (m³)], [NGL (m³)],
        [Gas WH Avg (10³m³)], [Gas S2 Avg (10³m³)],
        [Gas Gathered Avg (e³m³/d)], [Condensate Gathered Avg (m³/d)],
        [Gath. Water Avg (m³/d)],
        [Alloc. Water Avg (m³)],
        [Month]
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

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

    batch_size = 5000
    commit_every_rows = 50000
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
            if rows_done % 50000 == 0 or rows_done == total_rows:
                print(lf.detail(f"Insert progress: {lf.num(rows_done)}/{lf.num(total_rows)} rows"))

    if progress is not None:
        progress.phase_done("insert")
    print(lf.success(f"Inserted {lf.num(total_inserted)} rows into PCE_Production"))
    if duplicate_skipped > 0:
        print(lf.warn(f"Skipped {lf.num(duplicate_skipped)} duplicate rows"))

    return total_inserted

def main(cancel_event=None, progress_callback=None):
    """
    Rebuild PCE_Production from PCE_CDA.

    First repaints Gas S2, gas sales, condensate sales, and Sales CGR on PCE_CDA
    from Allocation_Factors (when AF rows exist), then clears and repopulates
    PCE_Production from CDA.

    Optional cancel_event (threading.Event): checked between major steps for
    best-effort cooperative cancel.
    """
    t0 = time.time()

    def aborted():
        return cancel_event is not None and cancel_event.is_set()

    def _duration():
        return time.time() - t0

    base_meta = {"mode": "full_rebuild", "duration_seconds": _duration()}

    timer = lf.StepTimer(log_fn=print)
    progress = _RebuildProgress(progress_callback)
    progress.emit(0)

    print(lf.header("PCE_Production population", Started=lf.timestamp()))
    if aborted():
        print(lf.warn("Cancelled before start."))
        return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

    end_cap = prodview_effective_end_date()
    from prodview_update_gui import (
        query_pce_cda_max_date,
        query_pce_cda_min_date,
        refresh_full_rebuild_cda,
    )

    cda_min_before = query_pce_cda_min_date()
    cda_max_before = query_pce_cda_max_date()
    print(lf.detail(f"PCE_CDA date span before Snowflake: {cda_min_before or '—'} → {cda_max_before or '—'}"))
    print(lf.detail(f"Automatic end date (today − lag): {end_cap}"))

    sf_start, sf_end, _cda_rows = refresh_full_rebuild_cda(
        log_callback=print,
        progress_callback=progress.snowflake_substep,
    )
    progress.phase_done("snowflake")
    cda_max_after = query_pce_cda_max_date()
    print(lf.detail(f"PCE_CDA max date after Snowflake refresh: {cda_max_after or '—'}"))
    timer.mark("Snowflake → PCE_CDA full lifespan refresh")

    if aborted():
        print(lf.warn("Cancelled after Snowflake CDA refresh."))
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
        print(lf.warn("Cancelled after PCE_CDA sales refresh."))
        return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

    with get_sql_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM PCE_CDA WHERE ProdDate > ?", (end_cap,))
        n_trim = cur.rowcount or 0
        conn.commit()
    if n_trim:
        print(lf.detail(f"Trimmed {lf.num(n_trim)} PCE_CDA row(s) after {end_cap} (automatic end)"))
    timer.mark("Trim future CDA rows")

    if aborted():
        print(lf.warn("Cancelled after trimming future CDA."))
        return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

    window_start, window_end = sf_start, sf_end
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
        print(
            lf.detail(
                f"Trimmed {lf.num(n_prod_trim)} PCE_Production row(s) after {end_cap}"
            )
        )

    if aborted():
        print(lf.warn("Cancelled after trimming future production."))
        return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

    progress.emit(66)

    # Step 2: Clear existing data
    clear_pce_production()
    timer.mark("Clear PCE_Production")
    if aborted():
        print(lf.warn("Cancelled after clearing PCE_Production."))
        return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

    # Step 3: Fetch well name mappings
    composite_map, fallback_map = fetch_well_mapping()
    timer.mark("Load well mappings")
    if aborted():
        print(lf.warn("Cancelled after loading well mappings."))
        return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

    progress.emit(68)

    # Step 4: Fetch CDA data
    df = fetch_cda_data(end_cap=end_cap, log=print)
    timer.mark("Load PCE_CDA into pandas")
    progress.emit(70)

    if df.empty:
        print(lf.warn("No data to process. Exiting."))
        return {
            **base_meta,
            "skipped": True,
            "reason": "No rows in PCE_CDA",
            "duration_seconds": _duration(),
        }

    # Step 5: Apply well name mappings (composite name with fallback to well name)
    df = apply_well_names(df, composite_map, fallback_map)
    df = apply_pad_name_from_well_master(df)
    df = apply_uwi_from_well_master(df)
    timer.mark("Well name / pad / UWI mapping")

    if df.empty:
        print(lf.warn("No data after well name mapping. Exiting."))
        return {
            **base_meta,
            "skipped": True,
            "reason": "No data after well name mapping",
            "duration_seconds": _duration(),
        }

    # Step 6: Filter to first production date for each well
    df = filter_to_first_production(df)
    timer.mark("Filter to first production")
    progress.emit(72)

    if df.empty:
        print(lf.warn("No data after filtering. Exiting."))
        return {
            **base_meta,
            "skipped": True,
            "reason": "No data after first-production filter",
            "duration_seconds": _duration(),
        }

    if aborted():
        print(lf.warn("Cancelled before sequence calculations."))
        return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

    # Step 7: Calculate sequences with corrected Day Seq UPRT logic
    df = calculate_sequences(df)

    # Step 8: Calculate cumulatives
    df = calculate_cumulatives(df)

    # Step 9: Calculate monthly averages
    df = calculate_monthly_averages(df)

    # Step 10: Add On Production Year
    df = add_on_production_year(df)
    timer.mark("Sequences, cumulatives, monthly avgs")
    progress.phase_done("prep")

    if aborted():
        print(lf.warn("Cancelled before inserting into PCE_Production."))
        return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

    # Step 11: Insert into PCE_Production
    rows_inserted = insert_pce_production(df, progress=progress)
    timer.mark("Insert PCE_Production")

    from sync_typecurves_to_production import sync_tc_to_production

    print(lf.step("Materializing PCE_TC into PCE_Production..."))
    try:
        sync_tc_to_production(log_callback=print)
    except Exception as e:
        print(lf.warn(f"PCE_TC → PCE_Production sync: {e}"))
    timer.mark("PCE_TC → PCE_Production sync")

    with get_sql_conn() as conn:
        cur = conn.cursor()
        sync_wm_uwi_to_downstream_sql(cur)
        conn.commit()
    timer.mark("WM UWI sync (Production + Allocation_Factors)")

    if not _refresh_ngl_from_allocation_factors(
        log=print,
        cancel_event=cancel_event,
    ):
        return {**base_meta, "cancelled": True, "duration_seconds": _duration()}
    timer.mark("NGL ratio refresh from Allocation_Factors")

    with get_sql_conn() as conn:
        cur = conn.cursor()
        sync_production_wm_metadata_from_wm_sql(
            cur,
            update_pad=False,
            update_enersight=True,
            update_month=True,
        )
        sync_production_wm_metadata_from_wm_sql(
            cur,
            window_start,
            window_end,
            update_pad=True,
            update_enersight=False,
            update_month=False,
        )
        conn.commit()
    timer.mark("WM metadata sync (pad, enersight, month)")

    try:
        from pce_frcst_prd_rebuild import rebuild_pce_frcst_prd

        rebuild_pce_frcst_prd(log=print)
    except Exception as e:
        print(lf.warn(f"PCE_FRCST_PRD rebuild: {e}"))
    timer.mark("PCE_FRCST_PRD rebuild")
    progress.phase_done("finalize")

    wells_processed = len(df["Well Name"].unique())
    total_records = len(df)

    # Step 12: Final summary
    cda_max_final = query_pce_cda_max_date()
    print(lf.summary("Complete", {
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