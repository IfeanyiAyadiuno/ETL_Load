"""
Monthly NGL Excel → daily Ratio (_R) and Fraction (_F) columns on PCE_Production.

Standalone trial; not integrated into production rebuild until method is chosen.
"""

from __future__ import annotations

import re
import threading
import time
from calendar import monthrange
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

NGL_EXCEL_FIELDS = (
    ("NGL-C2", "NGL-C2_R", "NGL-C2_F"),
    ("NGL-C3", "NGL-C3_R", "NGL-C3_F"),
    ("NGL-C4", "NGL-C4_R", "NGL-C4_F"),
    ("NGL-C5", "NGL-C5_R", "NGL-C5_F"),
    ("PA_NGLs", "PA_NGLs_R", "PA_NGLs_F"),
)

NGL_STAGING_TABLE = "dbo.PCE_NGL_Daily_Staging"
_STAGING_CHUNK_SIZE = 25_000
_STAGING_PREP_PROGRESS = 50_000
_SQL_HEARTBEAT_SEC = 15


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
    """Log elapsed time while a blocking SQL call runs on the main thread."""
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

_EXCEL_HEADER_ROW = 2  # row 3 in Excel (0-based)
_COLUMN_ALIASES = {
    "NGL_C2": "NGL-C2",
    "NGL_C3": "NGL-C3",
    "NGL_C4": "NGL-C4",
    "NGL-C4": "NGL-C4",
    "NGL_C5": "NGL-C5",
    "PA_NGLS": "PA_NGLs",
}


def normalize_uwi(value: Any) -> Optional[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = _clean_uwi_text(str(value))
    return text if text else None


def _clean_uwi_text(uwi: str) -> str:
    text = str(uwi).replace("\r", "").replace("\n", "").strip()
    for ch in ("\u2010", "\u2011", "\u2012", "\u2013", "\u2014", "\u2212"):
        text = text.replace(ch, "-")
    return text


def _should_strip_province_prefix(text: str) -> bool:
    """True when WM stored one extra province digit before the Excel UWI."""
    slash = text.find("/")
    if slash <= 0:
        return False
    seg = text[:slash]
    if not seg.isdigit():
        return False
    if len(seg) >= 4:
        return True
    if len(seg) == 3:
        if seg == "200":
            return True
        if seg == "100":
            # WM: leading 1 + Excel ``00/10-30...`` stored as ``100/10-30...``
            return True
        if seg[0] in "12" and seg[1] == "0" and seg[2] != "0":
            return True
    return False


def _strip_sql_province_prefix(uwi: str) -> str:
    """
    Remove one leading province digit from WM/SQL UWI when WM prepended it.

    Examples: ``200/b-049`` → ``00/b-049``, ``100/10-30`` → ``00/10-30``,
    ``102/05-32`` → ``02/05-32``, ``1100/16-28`` → ``100/16-28`` (one digit only).
    Already-correct values like ``02/a-028`` or ``00/B-078`` are left unchanged.
    """
    text = _clean_uwi_text(uwi)
    if len(text) <= 1 or not _should_strip_province_prefix(text):
        return text
    return text[1:]


def uwi_match_key(uwi: str, *, strip_leading_digit: bool = False) -> str:
    """
    Normalize UWI for joining Excel to SQL (case-insensitive).

    SQL UWIs may drop one WM province prefix digit before match.
    """
    text = _clean_uwi_text(uwi)
    if strip_leading_digit:
        text = _strip_sql_province_prefix(text)
    return text.upper()


def trim_sql_uwi_for_match(uwi: str) -> str:
    """Drop WM province prefix from SQL UWI when present (case preserved; for logging)."""
    return _strip_sql_province_prefix(uwi)


def parse_production_date(value: Any) -> Tuple[int, int]:
    """
    Parse PRODUCTION_DATE YYYYMM (e.g. 202208) or datetime-like values.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        raise ValueError("PRODUCTION_DATE is empty")

    if isinstance(value, pd.Timestamp):
        return value.year, value.month

    if hasattr(value, "year") and hasattr(value, "month"):
        return int(value.year), int(value.month)

    text = str(value).strip()
    if re.fullmatch(r"\d{6}", text):
        year = int(text[:4])
        month = int(text[4:6])
        if month < 1 or month > 12:
            raise ValueError(f"Invalid month in PRODUCTION_DATE: {value!r}")
        return year, month

    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"Cannot parse PRODUCTION_DATE: {value!r}")
    return int(parsed.year), int(parsed.month)


def days_in_month(year: int, month: int) -> int:
    return monthrange(year, month)[1]


def compute_ratio_value(
    monthly_ngl: float,
    month_gas_sum: float,
    daily_gas: float,
) -> Optional[float]:
    if month_gas_sum <= 0:
        return None
    if monthly_ngl is None or (isinstance(monthly_ngl, float) and pd.isna(monthly_ngl)):
        return None
    if daily_gas is None or (isinstance(daily_gas, float) and pd.isna(daily_gas)):
        return None
    return (float(monthly_ngl) / float(month_gas_sum)) * float(daily_gas)


def compute_fraction_value(monthly_ngl: float, year: int, month: int) -> Optional[float]:
    if monthly_ngl is None or (isinstance(monthly_ngl, float) and pd.isna(monthly_ngl)):
        return None
    dim = days_in_month(year, month)
    return float(monthly_ngl) / dim


def _normalize_excel_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename: Dict[str, str] = {}
    for col in df.columns:
        key = str(col).strip()
        upper = key.upper().replace(" ", "_")
        if upper in _COLUMN_ALIASES:
            rename[col] = _COLUMN_ALIASES[upper]
        else:
            rename[col] = key
    out = df.rename(columns=rename)
    return out


def read_monthly_ngl_excel(
    path: str,
    *,
    sheet_name=0,
    uwi_column: str = "UWI",
) -> pd.DataFrame:
    """Read monthly NGL sheet; header on row 3, data from row 4."""
    raw = pd.read_excel(path, sheet_name=sheet_name, header=_EXCEL_HEADER_ROW)
    df = _normalize_excel_columns(raw)
    required = ["PRODUCTION_DATE", uwi_column] + [f[0] for f in NGL_EXCEL_FIELDS]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Excel missing columns: {missing}. Found: {list(df.columns)}"
        )
    rows: List[dict] = []
    for _, row in df.iterrows():
        uwi = normalize_uwi(row[uwi_column])
        if not uwi:
            continue
        try:
            year, month = parse_production_date(row["PRODUCTION_DATE"])
        except ValueError:
            continue
        entry: Dict[str, Any] = {
            "Uwi": uwi_match_key(uwi, strip_leading_digit=False),
            "Year": year,
            "Month": month,
        }
        for excel_col, _, _ in NGL_EXCEL_FIELDS:
            val = row.get(excel_col)
            if val is None or (isinstance(val, float) and pd.isna(val)):
                entry[excel_col] = None
            else:
                entry[excel_col] = float(val)
        rows.append(entry)
    if not rows:
        raise ValueError("No valid monthly NGL rows after parsing Excel.")
    return pd.DataFrame(rows)


def load_production_for_ngl(
    conn,
    *,
    log: Optional[Callable[[str], None]] = None,
) -> pd.DataFrame:
    sql = """
    SELECT
          LTRIM(RTRIM(CAST([UWI] AS NVARCHAR(4000)))) AS UwiRaw
        , CAST([Date] AS DATE) AS ProdDate
        , CAST([Gathered Gas (e³m³/d)] AS FLOAT) AS GatheredGas
    FROM dbo.PCE_Production
    WHERE [UWI] IS NOT NULL
      AND LTRIM(RTRIM(CAST([UWI] AS NVARCHAR(4000)))) <> N''
    """
    if log:
        log("  Querying PCE_Production from SQL Server...")
    start = time.monotonic()
    prod = pd.read_sql(sql, conn)
    if log:
        log(
            f"  Loaded {len(prod):,} production row(s) "
            f"({_format_elapsed(time.monotonic() - start)})."
        )
    prod["UwiRaw"] = prod["UwiRaw"].map(
        lambda v: _clean_uwi_text(str(v)) if pd.notna(v) else v
    )
    prod["Uwi"] = prod["UwiRaw"].map(
        lambda v: uwi_match_key(str(v), strip_leading_digit=True) if pd.notna(v) else v
    )
    return prod


def compute_daily_ngl_columns(
    prod: pd.DataFrame,
    monthly: pd.DataFrame,
    *,
    log: Optional[Callable[[str], None]] = None,
) -> pd.DataFrame:
    """
    Return production rows with Ratio and Fraction NGL columns added.
    Rows without Excel match keep NGL columns as NaN.
    """
    def _log(msg: str) -> None:
        if log:
            log(msg)

    if prod.empty:
        return prod.copy()

    row_count = len(prod)
    _log(f"  Preparing {row_count:,} production row(s)...")
    out = prod.copy()
    out["ProdYear"] = pd.to_datetime(out["ProdDate"]).dt.year
    out["ProdMonth"] = pd.to_datetime(out["ProdDate"]).dt.month

    _log("  Summing monthly gathered gas by UWI...")
    gas_sum = (
        out.groupby(["Uwi", "ProdYear", "ProdMonth"], as_index=False)["GatheredGas"]
        .sum()
        .rename(columns={"GatheredGas": "MonthGasSum"})
    )
    out = out.merge(gas_sum, on=["Uwi", "ProdYear", "ProdMonth"], how="left")

    _log(f"  Joining {len(monthly):,} Excel monthly row(s)...")
    monthly_key = monthly.rename(
        columns={"Year": "ProdYear", "Month": "ProdMonth"}
    )
    out = out.merge(monthly_key, on=["Uwi", "ProdYear", "ProdMonth"], how="left")

    ym = pd.to_datetime(
        out["ProdYear"].astype(int).astype(str)
        + "-"
        + out["ProdMonth"].astype(int).astype(str).str.zfill(2)
        + "-01"
    )
    days_in_month_col = ym.dt.daysinmonth.astype(float)

    total_fields = len(NGL_EXCEL_FIELDS)
    for idx, (excel_col, col_r, col_f) in enumerate(NGL_EXCEL_FIELDS, start=1):
        pct = int(round(100 * idx / total_fields))
        _log(f"  Calculating {excel_col} ({idx}/{total_fields}, {pct}%)...")
        ngl_m = out[excel_col]
        has_ngl = ngl_m.notna()

        out[col_r] = np.nan
        out[col_f] = np.nan

        mask_r = has_ngl & out["MonthGasSum"].fillna(0).gt(0)
        out.loc[mask_r, col_r] = (
            ngl_m.loc[mask_r] / out.loc[mask_r, "MonthGasSum"]
        ) * out.loc[mask_r, "GatheredGas"]

        out.loc[has_ngl, col_f] = ngl_m.loc[has_ngl] / days_in_month_col.loc[has_ngl]

        matched_rows = int(has_ngl.sum())
        _log(f"    {excel_col}: {matched_rows:,} row(s) with Excel monthly data.")

    _log("  NGL column calculation complete.")
    return out


@dataclass
class NglUpdateSummary:
    excel_rows: int
    prod_rows: int
    prod_rows_without_uwi_hint: int
    rows_with_excel_match: int
    rows_updated: int
    excel_uwis: int
    prod_uwis: int
    unmatched_excel_keys: int
    excel_uwis_matched: int = 0
    unmatched_excel_uwis: Tuple[str, ...] = ()
    excel_uwis_not_in_prod: Tuple[str, ...] = ()
    prod_uwis_matched: int = 0
    unmatched_prod_uwis: Tuple[str, ...] = ()
    prod_uwis_not_in_excel: Tuple[str, ...] = ()


def _ngl_value_columns() -> List[str]:
    return [c for _, r, f in NGL_EXCEL_FIELDS for c in (r, f)]


def find_unmatched_excel_uwis(
    monthly: pd.DataFrame,
    computed: pd.DataFrame,
) -> Tuple[int, Tuple[str, ...], Tuple[str, ...]]:
    """
    Return (matched_count, unmatched_excel_uwis, excel_uwis_not_in_production).

    Unmatched = Excel UWI keys with zero production rows that received NGL values.
    Not-in-prod = Excel UWI keys absent from production after SQL trim/normalize.
    """
    ngl_cols = _ngl_value_columns()
    has_match = computed[ngl_cols].notna().any(axis=1)
    matched_uwis = set(computed.loc[has_match, "Uwi"].unique())
    excel_uwis = set(monthly["Uwi"].unique())
    prod_uwis = set(computed["Uwi"].unique())
    unmatched = tuple(sorted(excel_uwis - matched_uwis))
    not_in_prod = tuple(sorted(excel_uwis - prod_uwis))
    return len(matched_uwis), unmatched, not_in_prod


def find_unmatched_prod_uwis(
    monthly: pd.DataFrame,
    computed: pd.DataFrame,
) -> Tuple[int, Tuple[str, ...], Tuple[str, ...]]:
    """
    Return (matched_count, unmatched_sql_uwis_raw, prod_uwis_not_in_excel).

    Unmatched = distinct SQL UWIs (original value) with zero rows that received NGL values.
    Not-in-excel = normalized production UWI keys absent from Excel.
    """
    ngl_cols = _ngl_value_columns()
    has_match = computed[ngl_cols].notna().any(axis=1)
    matched_keys = set(computed.loc[has_match, "Uwi"].unique())
    prod_keys = set(computed["Uwi"].unique())
    excel_keys = set(monthly["Uwi"].unique())
    unmatched_keys = prod_keys - matched_keys

    raw_by_key = computed.groupby("Uwi", as_index=False)["UwiRaw"].first()
    unmatched_raw = tuple(
        sorted(
            raw_by_key.loc[raw_by_key["Uwi"].isin(unmatched_keys), "UwiRaw"].tolist()
        )
    )
    not_in_excel = tuple(sorted(prod_keys - excel_keys))
    return len(matched_keys), unmatched_raw, not_in_excel


def _log_uwi_list(
    label: str,
    uwis: Tuple[str, ...],
    *,
    log: Callable[[str], None],
    max_list: int,
) -> None:
    if not uwis:
        return
    log(f"  {label} ({len(uwis)}):")
    for uwi in uwis[:max_list]:
        log(f"    {uwi}")
    if len(uwis) > max_list:
        log(f"    ... and {len(uwis) - max_list} more (use --unmatched-csv).")


def log_unmatched_uwis(
    summary: NglUpdateSummary,
    *,
    log: Optional[Callable[[str], None]] = None,
    max_list: int = 100,
) -> None:
    if not log:
        return

    log(
        f"Excel UWI match: {summary.excel_uwis_matched} of {summary.excel_uwis} "
        "UWI(s) have production rows with NGL values."
    )
    if summary.unmatched_excel_uwis:
        _log_uwi_list("Unmatched Excel UWI(s)", summary.unmatched_excel_uwis, log=log, max_list=max_list)
        if summary.excel_uwis_not_in_prod:
            log(
                f"  Of those, {len(summary.excel_uwis_not_in_prod)} UWI(s) are not present "
                "in production at all (after trim/case normalize)."
            )
    else:
        log("  All Excel UWIs matched at least one production row.")

    log(
        f"SQL UWI match: {summary.prod_uwis_matched} of {summary.prod_uwis} "
        "UWI(s) received NGL values from Excel."
    )
    if summary.unmatched_prod_uwis:
        _log_uwi_list(
            "Unmatched SQL UWI(s)",
            summary.unmatched_prod_uwis,
            log=log,
            max_list=max_list,
        )
        if summary.prod_uwis_not_in_excel:
            log(
                f"  Of those, {len(summary.prod_uwis_not_in_excel)} UWI(s) are not present "
                "in Excel at all (after trim/case normalize)."
            )
    else:
        log("  All SQL UWIs matched at least one Excel monthly row.")


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
    """Build (UwiRaw, ProdDate, …NGL cols) tuples for fast_executemany INSERT."""
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
    col_list = ", ".join(
        f"[{name}]" for name in ("UwiRaw", "ProdDate", *ngl_cols)
    )
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


def _load_staging_table(
    conn,
    to_write: pd.DataFrame,
    ngl_cols: Sequence[str],
    *,
    log: Optional[Callable[[str], None]] = None,
) -> int:
    def _log(msg: str) -> None:
        if log:
            log(msg)

    total_rows = len(to_write)
    _log(f"Preparing {total_rows:,} row(s) for staging...")
    rows = build_staging_insert_rows(to_write, ngl_cols, log=log)
    if not rows:
        return 0

    cur = conn.cursor()
    cur.fast_executemany = True
    with _sql_heartbeat(log, "Truncating staging table"):
        cur.execute(f"TRUNCATE TABLE {NGL_STAGING_TABLE}")

    insert_sql = _staging_insert_sql(ngl_cols)
    total = len(rows)
    _log(f"Loading {total:,} row(s) into {NGL_STAGING_TABLE}...")
    for start in range(0, total, _STAGING_CHUNK_SIZE):
        chunk = rows[start : start + _STAGING_CHUNK_SIZE]
        with _sql_heartbeat(
            log,
            f"Staging insert {start + 1:,}–{min(start + len(chunk), total):,} of {total:,}",
            interval_sec=10,
        ):
            cur.executemany(insert_sql, chunk)
        loaded = min(start + len(chunk), total)
        pct = int(round(100 * loaded / total))
        _log(f"  Staging load: {loaded:,} / {total:,} ({pct}%)")
    return total


def _apply_staging_to_production(
    conn,
    ngl_cols: Sequence[str],
    *,
    log: Optional[Callable[[str], None]] = None,
) -> int:
    def _log(msg: str) -> None:
        if log:
            log(msg)

    cur = conn.cursor()
    _log(
        "Applying staged NGL values to PCE_Production "
        "(single UPDATE … JOIN — may take several minutes)..."
    )
    with _sql_heartbeat(log, "UPDATE … JOIN into PCE_Production"):
        cur.execute(_bulk_update_from_staging_sql(ngl_cols))
    updated = cur.rowcount
    if updated < 0:
        cur.execute("SELECT @@ROWCOUNT")
        row = cur.fetchone()
        updated = int(row[0]) if row else 0
    return updated


def clear_ngl_columns(
    conn,
    *,
    log: Optional[Callable[[str], None]] = None,
) -> None:
    cur = conn.cursor()
    sql = """
        UPDATE dbo.PCE_Production
        SET
              [NGL-C2_R] = NULL, [NGL-C3_R] = NULL, [NGL-C4_R] = NULL,
              [NGL-C5_R] = NULL, [PA_NGLs_R] = NULL,
              [NGL-C2_F] = NULL, [NGL-C3_F] = NULL, [NGL-C4_F] = NULL,
              [NGL-C5_F] = NULL, [PA_NGLs_F] = NULL
        WHERE [UWI] IS NOT NULL
        """
    if log:
        log("Clearing existing NGL columns on rows with UWI (may take a few minutes)...")
    with _sql_heartbeat(log, "Clear NGL columns"):
        cur.execute(sql)
    conn.commit()


def _count_prod_without_uwi(conn) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
        FROM dbo.PCE_Production
        WHERE [UWI] IS NULL
           OR LTRIM(RTRIM(CAST([UWI] AS NVARCHAR(4000)))) = N''
        """
    )
    row = cur.fetchone()
    return int(row[0]) if row else 0


def apply_ngl_updates(
    conn,
    computed: pd.DataFrame,
    *,
    dry_run: bool = False,
    clear_first: bool = True,
    log: Optional[Callable[[str], None]] = None,
) -> NglUpdateSummary:
    def _log(msg: str) -> None:
        if log:
            log(msg)

    ngl_cols = [c for _, r, f in NGL_EXCEL_FIELDS for c in (r, f)]
    has_match = computed[ngl_cols].notna().any(axis=1)
    to_write = computed.loc[has_match, ["UwiRaw", "ProdDate"] + ngl_cols].copy()

    summary = NglUpdateSummary(
        excel_rows=0,
        prod_rows=len(computed),
        prod_rows_without_uwi_hint=_count_prod_without_uwi(conn),
        rows_with_excel_match=int(has_match.sum()),
        rows_updated=0,
        excel_uwis=computed["Uwi"].nunique(),
        prod_uwis=computed["Uwi"].nunique(),
        unmatched_excel_keys=0,
    )

    if dry_run:
        _log(f"Dry run: would update {len(to_write)} production row(s).")
        if len(to_write) > 0:
            sample = to_write.iloc[0]
            _log(
                f"Sample {sample['UwiRaw']} {sample['ProdDate']}: "
                f"NGL-C2_R={sample.get('NGL-C2_R')}, NGL-C2_F={sample.get('NGL-C2_F')}"
            )
        return summary

    if clear_first:
        clear_ngl_columns(conn, log=log)

    if to_write.empty:
        conn.commit()
        _log("No production rows to update.")
        return summary

    _load_staging_table(conn, to_write, ngl_cols, log=log)
    summary.rows_updated = _apply_staging_to_production(conn, ngl_cols, log=log)
    conn.commit()
    _log(f"Updated {summary.rows_updated:,} production row(s).")
    return summary


def write_unmatched_uwis_csv(
    path: str,
    summary: NglUpdateSummary,
) -> None:
    not_in_prod = set(summary.excel_uwis_not_in_prod)
    not_in_excel = set(summary.prod_uwis_not_in_excel)
    rows: List[dict] = [
        {
            "source": "excel",
            "uwi": uwi,
            "in_other_side": uwi not in not_in_prod,
        }
        for uwi in summary.unmatched_excel_uwis
    ]
    for uwi_raw in summary.unmatched_prod_uwis:
        match_key = uwi_match_key(str(uwi_raw), strip_leading_digit=True)
        rows.append(
            {
                "source": "sql",
                "uwi": uwi_raw,
                "in_other_side": match_key not in not_in_excel,
            }
        )
    pd.DataFrame(rows).to_csv(path, index=False)


def run_ngl_daily_compare(
    excel_path: str,
    *,
    sheet_name=0,
    uwi_column: str = "UWI",
    dry_run: bool = False,
    clear_first: bool = True,
    unmatched_csv: Optional[str] = None,
    log: Optional[Callable[[str], None]] = None,
    conn=None,
) -> NglUpdateSummary:
    """Load Excel + production, compute columns, UPDATE PCE_Production."""
    from db_connection import get_sql_conn

    def _log(msg: str) -> None:
        if log:
            log(msg)

    run_start = time.monotonic()
    _log("[1/5] Reading monthly NGL Excel...")
    monthly = read_monthly_ngl_excel(excel_path, sheet_name=sheet_name, uwi_column=uwi_column)
    _log(
        f"  Excel: {len(monthly):,} monthly row(s), "
        f"{monthly['Uwi'].nunique()} UWI(s)."
    )

    own_conn = conn is None
    if own_conn:
        conn = get_sql_conn()
    try:
        _log("[2/5] Loading production from SQL...")
        without_uwi = _count_prod_without_uwi(conn)
        if without_uwi > 0:
            _log(
                f"  Warning: {without_uwi:,} production row(s) lack UWI — "
                "run scripts/add_pce_ngl_columns.sql Part 2 first."
            )

        prod = load_production_for_ngl(conn, log=log)
        trimmed = prod.loc[
            prod["UwiRaw"].map(
                lambda v: (
                    pd.notna(v)
                    and _strip_sql_province_prefix(str(v)) != _clean_uwi_text(str(v))
                )
            ),
            "UwiRaw",
        ].drop_duplicates()
        _log(
            "UWI match: strip WM province prefix digit from SQL UWIs when present; "
            "case-insensitive match to Excel."
        )
        if len(trimmed) > 0:
            sample = str(trimmed.iloc[0])
            _log(
                f"  {len(trimmed)} distinct SQL UWI(s) trimmed "
                f"(e.g. {sample!r} → {uwi_match_key(sample, strip_leading_digit=True)!r})."
            )

        _log("[3/5] Computing daily NGL columns...")
        computed = compute_daily_ngl_columns(prod, monthly, log=_log)
        _log("[4/5] Checking UWI matches...")
        excel_matched, excel_unmatched, excel_not_in_prod = find_unmatched_excel_uwis(
            monthly, computed
        )
        prod_matched, prod_unmatched, prod_not_in_excel = find_unmatched_prod_uwis(
            monthly, computed
        )
        ngl_cols = _ngl_value_columns()
        rows_to_apply = int(computed[ngl_cols].notna().any(axis=1).sum())
        _log(
            f"[5/5] Writing to SQL ({'dry run' if dry_run else 'live'}) — "
            f"{rows_to_apply:,} row(s) to apply..."
        )
        summary = apply_ngl_updates(
            conn,
            computed,
            dry_run=dry_run,
            clear_first=clear_first,
            log=log,
        )
        summary.excel_rows = len(monthly)
        summary.excel_uwis = int(monthly["Uwi"].nunique())
        summary.prod_uwis = int(computed["Uwi"].nunique())
        summary.excel_uwis_matched = excel_matched
        summary.unmatched_excel_uwis = excel_unmatched
        summary.excel_uwis_not_in_prod = excel_not_in_prod
        summary.prod_uwis_matched = prod_matched
        summary.unmatched_prod_uwis = prod_unmatched
        summary.prod_uwis_not_in_excel = prod_not_in_excel
        summary.unmatched_excel_keys = len(excel_unmatched)
        if dry_run:
            log_unmatched_uwis(summary, log=log)
            if unmatched_csv:
                write_unmatched_uwis_csv(unmatched_csv, summary)
                _log(f"Wrote unmatched UWIs (Excel + SQL) to {unmatched_csv}")
        _log(f"Finished ({_format_elapsed(time.monotonic() - run_start)} total).")
        return summary
    finally:
        if own_conn and conn is not None:
            conn.close()
