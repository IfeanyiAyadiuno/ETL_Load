"""
Monthly NGL Ratio (_R) spread from Allocation_Factors to PCE_Production.

Monthly NGL volumes live in Allocation_Factors (bulk Excel load or preserved on PA reload).
ValNav Monthly Update applies daily _R columns for the selected month.
"""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from sales_allocation_updates import (
    calendar_month_bounds,
    fetch_pce_uwi_to_well_name,
    fetch_pce_wm_well_to_production_name,
    resolve_valnav_uwi_to_well_name,
)
from valnav_columns import resolve_valnav_ngl_columns, resolve_valnav_uwi_column

# (monthly NGL key in ratio math, PCE_Production column)
NGL_FIELDS: Tuple[Tuple[str, str], ...] = (
    ("NGL-C2", "NGL-C2_R"),
    ("NGL-C3", "NGL-C3_R"),
    ("NGL-C4", "NGL-C4_R"),
    ("NGL-C5", "NGL-C5_R"),
    ("NGLs", "PA_NGLs_R"),
)

# Allocation_Factors column -> monthly NGL key for ratio math
AF_NGL_TO_MONTHLY: Tuple[Tuple[str, str], ...] = (
    ("NGL_C2", "NGL-C2"),
    ("NGL_C3", "NGL-C3"),
    ("NGL_C4", "NGL-C4"),
    ("NGL_C5", "NGL-C5"),
    ("PA_NGLs", "NGLs"),
)

NGL_STAGING_TABLE = "dbo.PCE_NGL_Daily_Staging"
_STAGING_CHUNK_SIZE = 25_000
_STAGING_PREP_PROGRESS = 50_000
_SQL_HEARTBEAT_SEC = 15
DEFAULT_GAS_HURDLE_MULTIPLIER = 5
DEFAULT_GAS_ROLLING_MONTHS = 3


def _format_elapsed(seconds: float) -> str:
    total = max(0, int(seconds))
    minutes, secs = divmod(total, 60)
    if minutes:
        return f"{minutes}:{secs:02d}"
    return f"{secs}s"


@contextmanager
def _sql_heartbeat(
    log: Optional[Callable[[str], None]],
    label: str,
    *,
    interval_sec: int = _SQL_HEARTBEAT_SEC,
) -> Iterator[None]:
    if not log:
        yield
        return
    stop = threading.Event()
    start = time.monotonic()

    def _tick() -> None:
        while not stop.wait(interval_sec):
            log(f"  {label}… {_format_elapsed(time.monotonic() - start)} elapsed")

    thread = threading.Thread(target=_tick, daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=1.0)
        log(f"  {label} done ({_format_elapsed(time.monotonic() - start)}).")


def _ngl_ratio_columns() -> List[str]:
    return [sql_col for _, sql_col in NGL_FIELDS]


def _ratio_coef_column(excel_key: str) -> str:
    return f"__{excel_key}_ratio_coef"


def _ngl_needs_ratio_forward_fill(value: Any) -> bool:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return True
    if pd.isna(value):
        return True
    return float(value) == 0.0


def read_ngl_monthly_from_allocation_factors(
    conn,
    month_start: datetime,
) -> pd.DataFrame:
    """Build monthly NGL rows from Allocation_Factors for the selected month."""
    month_first, _, _ = calendar_month_bounds(month_start)
    year, month = month_start.year, month_start.month
    sql = """
    SELECT
          [Well Name] AS WellName
        , [NGL_C2]
        , [NGL_C3]
        , [NGL_C4]
        , [NGL_C5]
        , [PA_NGLs]
    FROM Allocation_Factors
    WHERE CAST(MonthStartDate AS DATE) = ?
      AND (
            [NGL_C2] IS NOT NULL
         OR [NGL_C3] IS NOT NULL
         OR [NGL_C4] IS NOT NULL
         OR [NGL_C5] IS NOT NULL
         OR [PA_NGLs] IS NOT NULL
      )
    """
    df = pd.read_sql(sql, conn, params=[month_first])
    if df.empty:
        return pd.DataFrame(
            columns=["WellName", "Year", "Month"] + [k for k, _ in NGL_FIELDS]
        )

    rows: List[dict] = []
    for _, row in df.iterrows():
        well_name = row.get("WellName")
        if well_name is None or (isinstance(well_name, float) and pd.isna(well_name)):
            continue
        wn = str(well_name).strip()
        if not wn:
            continue
        entry: Dict[str, Any] = {
            "WellName": wn,
            "Year": year,
            "Month": month,
        }
        for af_col, monthly_key in AF_NGL_TO_MONTHLY:
            val = row.get(af_col)
            if val is None or (isinstance(val, float) and pd.isna(val)):
                entry[monthly_key] = None
            else:
                entry[monthly_key] = float(val)
        rows.append(entry)

    if not rows:
        return pd.DataFrame(
            columns=["WellName", "Year", "Month"] + [k for k, _ in NGL_FIELDS]
        )
    return pd.DataFrame(rows).drop_duplicates(
        subset=["WellName", "Year", "Month"], keep="last"
    )


def read_ngl_monthly_from_valnav(
    df_valnav: pd.DataFrame,
    *,
    col_uwi: str,
    col_ngl: Dict[str, str],
    year: int,
    month: int,
    pce_uwi_dict: Dict[str, str],
) -> pd.DataFrame:
    """Build monthly NGL rows for PA-matched wells (WellName + Year + Month)."""
    rows: List[dict] = []
    for _, row in df_valnav.iterrows():
        raw_uwi = row.get(col_uwi)
        if raw_uwi is None or (isinstance(raw_uwi, float) and pd.isna(raw_uwi)):
            continue
        uwi_str = str(raw_uwi).strip()
        if not uwi_str or uwi_str.lower() == "nan":
            continue
        well_name = resolve_valnav_uwi_to_well_name(uwi_str, pce_uwi_dict)
        if not well_name:
            continue
        entry: Dict[str, Any] = {
            "WellName": well_name,
            "Year": year,
            "Month": month,
        }
        for excel_key, col_name in col_ngl.items():
            val = row.get(col_name)
            if val is None or (isinstance(val, float) and pd.isna(val)):
                entry[excel_key] = None
            else:
                entry[excel_key] = float(val)
        rows.append(entry)
    if not rows:
        return pd.DataFrame(
            columns=["WellName", "Year", "Month"] + [k for k, _ in NGL_FIELDS]
        )
    out = pd.DataFrame(rows)
    return out.drop_duplicates(subset=["WellName", "Year", "Month"], keep="last")


def add_last_valid_ratio_coefs(
    monthly: pd.DataFrame,
    prod_month_gas: pd.DataFrame,
    *,
    log: Optional[Callable[[str], None]] = None,
) -> pd.DataFrame:
    def _log(msg: str) -> None:
        if log:
            log(msg)

    work = monthly.rename(columns={"Year": "ProdYear", "Month": "ProdMonth"}).copy()
    work = work.merge(prod_month_gas, on=["WellName", "ProdYear", "ProdMonth"], how="left")
    work = work.sort_values(["WellName", "ProdYear", "ProdMonth"])

    for excel_key, _ in NGL_FIELDS:
        coef_col = _ratio_coef_column(excel_key)
        ngl = work[excel_key]
        gas = work["MonthGasSum"].fillna(0)
        valid = ngl.notna() & ngl.gt(0) & gas.gt(0)
        work[coef_col] = np.where(valid, ngl / gas, np.nan)
        work[coef_col] = work.groupby("WellName", sort=False)[coef_col].ffill()
        needs_fill = work[excel_key].map(_ngl_needs_ratio_forward_fill)
        filled = int((needs_fill & work[coef_col].notna()).sum())
        if filled:
            _log(
                f"  {excel_key}: {filled:,} zero/missing monthly row(s) "
                "use last valid ratio."
            )
    return work


def rolling_gathered_gas_avg(
    df: pd.DataFrame,
    *,
    months: int = DEFAULT_GAS_ROLLING_MONTHS,
) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=float)

    work = df.copy()
    work["_orig_idx"] = work.index
    sorted_work = work.sort_values(["WellName", "ProdDate"])
    sorted_work["_period"] = pd.to_datetime(sorted_work["ProdDate"]).dt.to_period("M")

    uwi_month = (
        sorted_work.groupby(["WellName", "_period"], as_index=False)["GatheredGas"]
        .mean()
        .sort_values(["WellName", "_period"])
    )
    uwi_month["RollingGasAvg"] = uwi_month.groupby("WellName", sort=False)[
        "GatheredGas"
    ].transform(lambda s: s.shift(1).rolling(months, min_periods=1).mean())

    sorted_work = sorted_work.merge(
        uwi_month[["WellName", "_period", "RollingGasAvg"]],
        on=["WellName", "_period"],
        how="left",
    )
    result = pd.Series(np.nan, index=df.index, dtype=float)
    result.loc[sorted_work["_orig_idx"]] = sorted_work["RollingGasAvg"].to_numpy()
    return result


def apply_gas_hurdle_to_ratio(
    df: pd.DataFrame,
    col_r: str,
    *,
    hurdle_multiplier: float = DEFAULT_GAS_HURDLE_MULTIPLIER,
    log: Optional[Callable[[str], None]] = None,
) -> Tuple[pd.DataFrame, int]:
    if "RollingGasAvg" not in df.columns:
        raise ValueError("apply_gas_hurdle_to_ratio requires RollingGasAvg column")

    out = df.sort_values(["WellName", "ProdDate"]).copy()
    hurdle = hurdle_multiplier * out["RollingGasAvg"]
    spike = (
        out["GatheredGas"].notna()
        & hurdle.notna()
        & hurdle.gt(0)
        & out["GatheredGas"].gt(hurdle)
    )
    prev_r = out.groupby("WellName", sort=False)[col_r].shift(1)
    replaced = spike & prev_r.notna()
    out.loc[replaced, col_r] = prev_r.loc[replaced]

    replaced_count = int(replaced.sum())
    no_prev_count = int((spike & prev_r.isna()).sum())
    if log and replaced_count:
        log(f"    {col_r}: {replaced_count:,} spike day(s) use previous _R.")
    if log and no_prev_count:
        log(
            f"    {col_r}: {no_prev_count:,} spike day(s) kept raw _R "
            "(no previous day)."
        )
    return out, replaced_count


def load_production_for_ngl_month(
    conn,
    well_names: Sequence[str],
    month_start: date,
    month_end: date,
    *,
    log: Optional[Callable[[str], None]] = None,
) -> pd.DataFrame:
    if not well_names:
        return pd.DataFrame(
            columns=["UwiRaw", "WellName", "ProdDate", "GatheredGas"]
        )

    placeholders = ",".join("?" * len(well_names))
    sql = f"""
    SELECT
          LTRIM(RTRIM(CAST(p.[UWI] AS NVARCHAR(4000)))) AS UwiRaw
        , p.[Well Name] AS WellName
        , CAST(p.[Date] AS DATE) AS ProdDate
        , CAST(p.[Gathered Gas (e³m³/d)] AS FLOAT) AS GatheredGas
    FROM dbo.PCE_Production AS p
    WHERE CAST(p.[Date] AS DATE) >= ?
      AND CAST(p.[Date] AS DATE) <= ?
      AND p.[Well Name] IN ({placeholders})
    """
    params: List[Any] = [month_start, month_end, *well_names]
    if log:
        log(
            f"  Querying PCE_Production for {len(well_names):,} well(s), "
            f"{month_start} … {month_end}..."
        )
    start = time.monotonic()
    prod = pd.read_sql(sql, conn, params=params)
    if log:
        log(
            f"  Loaded {len(prod):,} production row(s) "
            f"({_format_elapsed(time.monotonic() - start)})."
        )
    prod["UwiRaw"] = prod["UwiRaw"].map(
        lambda v: str(v).strip() if pd.notna(v) else v
    )
    return prod


def compute_daily_ngl_ratio_columns(
    prod: pd.DataFrame,
    monthly: pd.DataFrame,
    *,
    log: Optional[Callable[[str], None]] = None,
) -> pd.DataFrame:
    def _log(msg: str) -> None:
        if log:
            log(msg)

    if prod.empty:
        return prod.copy()

    _log(f"  Preparing {len(prod):,} production row(s)...")
    out = prod.copy()
    out["ProdYear"] = pd.to_datetime(out["ProdDate"]).dt.year
    out["ProdMonth"] = pd.to_datetime(out["ProdDate"]).dt.month

    gas_sum = (
        out.groupby(["WellName", "ProdYear", "ProdMonth"], as_index=False)["GatheredGas"]
        .sum()
        .rename(columns={"GatheredGas": "MonthGasSum"})
    )
    out = out.merge(gas_sum, on=["WellName", "ProdYear", "ProdMonth"], how="left")

    prod_month_gas = (
        out.groupby(["WellName", "ProdYear", "ProdMonth"], as_index=False)["MonthGasSum"]
        .first()
    )
    _log("  Building last-valid ratio coefficients for zero/missing ValNav NGL...")
    monthly_key = add_last_valid_ratio_coefs(monthly, prod_month_gas, log=log)
    out = out.merge(monthly_key, on=["WellName", "ProdYear", "ProdMonth"], how="left")

    _log(
        f"  Computing {DEFAULT_GAS_ROLLING_MONTHS}-month rolling gathered-gas "
        "average per well (hurdle baseline)..."
    )
    out = out.sort_values(["WellName", "ProdDate"])
    out["RollingGasAvg"] = rolling_gathered_gas_avg(out)

    total_fields = len(NGL_FIELDS)
    for idx, (excel_key, col_r) in enumerate(NGL_FIELDS, start=1):
        pct = int(round(100 * idx / total_fields))
        _log(f"  Calculating {excel_key} ({idx}/{total_fields}, {pct}%)...")
        coef_col = _ratio_coef_column(excel_key)
        out[col_r] = np.nan
        mask_r = out[coef_col].notna() & out["GatheredGas"].notna()
        out.loc[mask_r, col_r] = out.loc[mask_r, coef_col] * out.loc[mask_r, "GatheredGas"]
        out, _ = apply_gas_hurdle_to_ratio(out, col_r, log=log)
        _log(f"    {col_r}: {int(mask_r.sum()):,} ratio value(s).")

    coef_cols = [_ratio_coef_column(k) for k, _ in NGL_FIELDS]
    out = out.drop(columns=[*coef_cols, "RollingGasAvg"], errors="ignore")
    _log("  NGL ratio calculation complete.")
    return out


@dataclass
class NglMonthlySummary:
    monthly_rows: int = 0
    wells_matched: int = 0
    prod_rows: int = 0
    rows_with_ngl: int = 0
    rows_updated: int = 0
    skipped: bool = False
    skip_reason: str = ""


def _float_or_none(value: Any) -> Optional[float]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if pd.isna(value):
        return None
    return float(value)


def _prod_date_value(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, datetime):
        return value.date()
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"Cannot convert production date: {value!r}")
    return parsed.date()


def build_staging_insert_rows(
    to_write: pd.DataFrame,
    ngl_cols: Sequence[str],
    *,
    log: Optional[Callable[[str], None]] = None,
    progress_every: int = _STAGING_PREP_PROGRESS,
) -> List[tuple]:
    uwis = to_write["UwiRaw"].tolist()
    dates = [_prod_date_value(d) for d in to_write["ProdDate"].tolist()]
    col_vals = [to_write[c].tolist() for c in ngl_cols]
    n_rows = len(to_write)
    n_cols = len(ngl_cols)
    rows: List[tuple] = []
    for i in range(n_rows):
        ngl_tuple = tuple(_float_or_none(col_vals[j][i]) for j in range(n_cols))
        rows.append((uwis[i], dates[i]) + ngl_tuple)
        if log and progress_every > 0 and (i + 1) % progress_every == 0:
            pct = int(round(100 * (i + 1) / n_rows))
            log(f"  Preparing rows: {i + 1:,} / {n_rows:,} ({pct}%)")
    if log and n_rows > 0:
        log(f"  Preparing rows: {n_rows:,} / {n_rows:,} (100%)")
    return rows


def _staging_insert_sql(ngl_cols: Sequence[str]) -> str:
    col_list = ", ".join(f"[{name}]" for name in ("UwiRaw", "ProdDate", *ngl_cols))
    placeholders = ", ".join("?" * (2 + len(ngl_cols)))
    return f"INSERT INTO {NGL_STAGING_TABLE} ({col_list}) VALUES ({placeholders})"


def _bulk_update_from_staging_sql(ngl_cols: Sequence[str]) -> str:
    set_clause = ", ".join(f"p.[{col}] = s.[{col}]" for col in ngl_cols)
    return f"""
        UPDATE p
        SET {set_clause}
        FROM dbo.PCE_Production AS p
        INNER JOIN {NGL_STAGING_TABLE} AS s
            ON LTRIM(RTRIM(CAST(p.[UWI] AS NVARCHAR(4000)))) = s.UwiRaw
           AND CAST(p.[Date] AS DATE) = s.ProdDate
    """


def clear_ngl_ratio_columns_month(
    conn,
    well_names: Sequence[str],
    month_start: date,
    month_end: date,
    *,
    log: Optional[Callable[[str], None]] = None,
) -> None:
    if not well_names:
        return
    placeholders = ",".join("?" * len(well_names))
    sql = f"""
        UPDATE dbo.PCE_Production
        SET
              [NGL-C2_R] = NULL, [NGL-C3_R] = NULL, [NGL-C4_R] = NULL,
              [NGL-C5_R] = NULL, [PA_NGLs_R] = NULL
        WHERE CAST([Date] AS DATE) >= ?
          AND CAST([Date] AS DATE) <= ?
          AND [Well Name] IN ({placeholders})
        """
    if log:
        log(
            f"Clearing NGL ratio columns for {len(well_names):,} well(s), "
            f"{month_start} … {month_end}..."
        )
    cur = conn.cursor()
    with _sql_heartbeat(log, "Clear NGL ratio columns"):
        cur.execute(sql, [month_start, month_end, *well_names])


def apply_ngl_monthly_updates(
    conn,
    computed: pd.DataFrame,
    well_names: Sequence[str],
    month_start: date,
    month_end: date,
    *,
    log: Optional[Callable[[str], None]] = None,
) -> int:
    ngl_cols = _ngl_ratio_columns()
    has_match = computed[ngl_cols].notna().any(axis=1)
    to_write = computed.loc[has_match, ["UwiRaw", "ProdDate"] + ngl_cols].copy()
    to_write = to_write[to_write["UwiRaw"].notna() & (to_write["UwiRaw"] != "")]

    if to_write.empty:
        if log:
            log("No production rows with NGL values to write.")
        return 0

    clear_ngl_ratio_columns_month(
        conn, well_names, month_start, month_end, log=log
    )

    def _log(msg: str) -> None:
        if log:
            log(msg)

    rows = build_staging_insert_rows(to_write, ngl_cols, log=log)
    cur = conn.cursor()
    cur.fast_executemany = True
    with _sql_heartbeat(_log, "Truncating staging table"):
        cur.execute(f"TRUNCATE TABLE {NGL_STAGING_TABLE}")

    insert_sql = _staging_insert_sql(ngl_cols)
    total = len(rows)
    _log(f"Loading {total:,} row(s) into {NGL_STAGING_TABLE}...")
    for start in range(0, total, _STAGING_CHUNK_SIZE):
        chunk = rows[start : start + _STAGING_CHUNK_SIZE]
        with _sql_heartbeat(
            _log,
            f"Staging insert {start + 1:,}–{min(start + len(chunk), total):,} of {total:,}",
            interval_sec=10,
        ):
            cur.executemany(insert_sql, chunk)
        loaded = min(start + len(chunk), total)
        pct = int(round(100 * loaded / total))
        _log(f"  Staging load: {loaded:,} / {total:,} ({pct}%)")

    _log(
        "Applying staged NGL values to PCE_Production "
        "(single UPDATE … JOIN)..."
    )
    with _sql_heartbeat(_log, "UPDATE … JOIN into PCE_Production"):
        cur.execute(_bulk_update_from_staging_sql(ngl_cols))
    updated = cur.rowcount
    if updated < 0:
        cur.execute("SELECT @@ROWCOUNT")
        row = cur.fetchone()
        updated = int(row[0]) if row else len(to_write)
    conn.commit()
    _log(f"Updated {updated:,} production row(s) with NGL ratios.")
    return int(updated)


def run_ngl_monthly_from_allocation_factors(
    conn,
    month_start: datetime,
    *,
    log: Optional[Callable[[str], None]] = None,
) -> NglMonthlySummary:
    """Compute and write monthly NGL ratios from Allocation_Factors for one month."""

    def _log(msg: str) -> None:
        if log:
            log(msg)

    summary = NglMonthlySummary()
    month_first, month_last, _ = calendar_month_bounds(month_start)

    monthly = read_ngl_monthly_from_allocation_factors(conn, month_start)
    cur = conn.cursor()
    wm_to_prod = fetch_pce_wm_well_to_production_name(cur)
    if not monthly.empty:
        monthly = monthly.copy()
        monthly["WellName"] = monthly["WellName"].map(
            lambda w: wm_to_prod.get(str(w).strip(), str(w).strip())
        )
    summary.monthly_rows = len(monthly)
    if monthly.empty:
        summary.skipped = True
        summary.skip_reason = (
            "No NGL volumes in Allocation_Factors for this month — "
            "confirm ValNav sheet has NGL-C2…C5 and NGLs columns and re-run PA."
        )
        _log(f"NGL update skipped: {summary.skip_reason}")
        return summary

    well_names = sorted(monthly["WellName"].unique().tolist())
    summary.wells_matched = len(well_names)

    prod = load_production_for_ngl_month(
        conn, well_names, month_first, month_last, log=_log
    )
    summary.prod_rows = len(prod)
    if prod.empty:
        summary.skipped = True
        summary.skip_reason = "No PCE_Production rows for matched wells in selected month."
        _log(f"NGL update skipped: {summary.skip_reason}")
        return summary

    _log("Computing daily NGL ratio columns from Allocation_Factors...")
    computed = compute_daily_ngl_ratio_columns(prod, monthly, log=_log)
    ngl_cols = _ngl_ratio_columns()
    has_ngl = computed[ngl_cols].notna().any(axis=1)
    summary.rows_with_ngl = int(has_ngl.sum())

    summary.rows_updated = apply_ngl_monthly_updates(
        conn,
        computed,
        well_names,
        month_first,
        month_last,
        log=_log,
    )
    return summary


def run_ngl_monthly_from_valnav(
    conn,
    df_valnav: pd.DataFrame,
    month_start: datetime,
    *,
    log: Optional[Callable[[str], None]] = None,
) -> NglMonthlySummary:
    """Deprecated: NGL volumes are read from Allocation_Factors, not ValNav."""
    if log:
        log(
            "NGL apply uses Allocation_Factors (not ValNav sheet); "
            "delegating to run_ngl_monthly_from_allocation_factors."
        )
    return run_ngl_monthly_from_allocation_factors(conn, month_start, log=log)
