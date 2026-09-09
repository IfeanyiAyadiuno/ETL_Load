"""
Monthly forecast workbook (first sheet, row 1 = headers) -> ``dbo.PCE_Monthly_Forecasts``.

Excel headers are mapped to the table's real column names (template uses labels like
``CDGR(Mcf/d)``; SQL uses ``CDGR_Mcf_d``, etc.). Unmapped columns are ignored.

Each import **appends** rows from the file only. To reload a month that is already
in the database, use **Remove selected months** first, then import again. The ``Month``
column must be ``YYYY Month`` (e.g. ``2026 May``). Import is blocked when any file
month already exists in ``PCE_Monthly_Forecasts``.

Afterwards ``rebuild_pce_frcst_prd`` refreshes ``dbo.PCE_FRCST_PRD`` (all forecast
months plus gathered production through ``prodview_effective_end_date()``).

Selected forecast months can be removed via ``delete_forecast_months`` (GUI), which
deletes rows matching the ``[Month]`` column value(s) and rebuilds ``PCE_FRCST_PRD``.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

import log_format as lf
from db_connection import get_sql_conn

TARGET_TABLE = "dbo.PCE_Monthly_Forecasts"

INSERT_BATCH_SIZE = 2500
DATE_WARNING_SAMPLE_LIMIT = 15

_YEAR_IN_STRING = re.compile(r"\d{4}")

_MONTH_NAME_TO_NUM: Dict[str, int] = {
    "january": 1,
    "jan": 1,
    "february": 2,
    "feb": 2,
    "march": 3,
    "mar": 3,
    "april": 4,
    "apr": 4,
    "may": 5,
    "june": 6,
    "jun": 6,
    "july": 7,
    "jul": 7,
    "august": 8,
    "aug": 8,
    "september": 9,
    "sep": 9,
    "sept": 9,
    "october": 10,
    "oct": 10,
    "november": 11,
    "nov": 11,
    "december": 12,
    "dec": 12,
}

_YEAR_MONTH_LABEL = re.compile(
    r"^\s*(\d{4})\s+("
    r"January|February|March|April|May|June|July|August|September|October|November|December|"
    r"Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec"
    r")\.?\s*$",
    re.IGNORECASE,
)

# Insert order matches typical table design; only columns present in the file are used.
SQL_COLUMN_ORDER: List[str] = [
    "Date",
    "UWI",
    "CDGR_Mcf_d",
    "CD_Cond_bbl_d",
    "CD_Water_bbl_d",
    "Month",
    "Pad",
    "Fault_Block",
    "Enersight Well Name",
]

_SQL_ALIASES_FOLD: Dict[str, str] = {
    # Core
    "date": "Date",
    "uwi": "UWI",
    # Gas / condensate / water (template vs SSMS names)
    "cdgr(mcf/d)": "CDGR_Mcf_d",
    "cdgr_mcf_d": "CDGR_Mcf_d",
    "cdgr mcf/d": "CDGR_Mcf_d",
    "cd cond.(bbl/d)": "CD_Cond_bbl_d",
    "cd_cond_bbld": "CD_Cond_bbl_d",
    "cd_cond_bbl_d": "CD_Cond_bbl_d",
    "cd cond.(bbld)": "CD_Cond_bbl_d",
    "cd water(bbl/d)": "CD_Water_bbl_d",
    "cd_water_bbld": "CD_Water_bbl_d",
    "cd_water_bbl_d": "CD_Water_bbl_d",
    "cd water(bbld)": "CD_Water_bbl_d",
    # Enersight (Excel / truncated header)
    "cei enersight wellname": "Enersight Well Name",
    "cei enersight wellna": "Enersight Well Name",
    "enersight well name": "Enersight Well Name",
    "enersight_well_name": "Enersight Well Name",
    "month": "Month",
    "pad": "Pad",
    "fault block": "Fault_Block",
    "fault_block": "Fault_Block",
}

_ALLOWED = set(SQL_COLUMN_ORDER)


def _init_identity_aliases():
    """Allow headers that already match SSMS names (any case)."""
    for col in SQL_COLUMN_ORDER:
        _SQL_ALIASES_FOLD.setdefault(col.casefold(), col)


_init_identity_aliases()


def _fold_header_key(h: object) -> str:
    if h is None or (isinstance(h, float) and np.isnan(h)):
        return ""
    return (
        str(h)
        .replace("\ufeff", "")
        .strip()
        .replace("\u00a0", " ")
        .casefold()
    )


def _strip_header(h: object) -> str:
    if h is None or (isinstance(h, float) and np.isnan(h)):
        return ""
    return (
        str(h)
        .replace("\ufeff", "")
        .strip()
        .replace("\u00a0", " ")
    )


def _sql_bracket_identifier(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def _cell_value_sql(val: object):
    if val is None:
        return None
    if isinstance(val, float) and np.isnan(val):
        return None
    if pd.isna(val):
        return None

    if isinstance(val, pd.Timestamp):
        pydt = val.to_pydatetime()
        if isinstance(pydt, datetime):
            return pydt.date()
        return val

    if isinstance(val, datetime):
        return val.date()

    if isinstance(val, date):
        return val

    if isinstance(val, np.integer):
        return int(val)

    if isinstance(val, (np.floating, float)):
        f = float(val)
        return f if np.isfinite(f) else None

    if isinstance(val, bool):
        return val

    return val


def _is_blank_date_value(val: object) -> bool:
    if val is None:
        return True
    if isinstance(val, float) and np.isnan(val):
        return True
    if pd.isna(val):
        return True
    if isinstance(val, str) and not val.strip():
        return True
    return False


def _parsed_date_year(val: object) -> Optional[int]:
    """Best-effort year extraction for date validation (before SQL coercion)."""
    if _is_blank_date_value(val):
        return None

    if isinstance(val, pd.Timestamp):
        return int(val.year)

    if isinstance(val, datetime):
        return int(val.year)

    if isinstance(val, date):
        return int(val.year)

    coerced = _cell_value_sql(val)
    if isinstance(coerced, date):
        return int(coerced.year)

    try:
        parsed = pd.to_datetime(val, errors="coerce")
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    return int(parsed.year)


def validate_forecast_dates(
    df: pd.DataFrame,
    max_samples: int = DATE_WARNING_SAMPLE_LIMIT,
) -> List[str]:
    """
    Return human-readable warning lines for Date values that lack a trustworthy year.
    """
    if "Date" not in df.columns:
        return ["Date column missing after header mapping."]

    issues: List[str] = []
    for idx, val in enumerate(df["Date"]):
        row_num = idx + 2
        issue: Optional[str] = None

        if _is_blank_date_value(val):
            issue = "is blank or missing"
        elif isinstance(val, str) and not _YEAR_IN_STRING.search(val.strip()):
            issue = f"has no year in text {val!r}"
        else:
            year = _parsed_date_year(val)
            if year is None:
                issue = f"could not be parsed ({val!r})"
            elif year == 1900:
                issue = "parsed as year 1900 (month/day-only Excel date?)"
            elif year < 2000:
                issue = f"parsed as year {year} (expected 2000 or later)"

        if issue:
            issues.append(f"Row {row_num}: Date {issue}")

    if len(issues) > max_samples:
        extra = len(issues) - max_samples
        return issues[:max_samples] + [
            f"... and {extra} more row(s) with date issues (total {len(issues)})."
        ]
    return issues


def forecast_month_display_label(
    year: int,
    month: int,
    month_column: Optional[str] = None,
) -> str:
    """Display label for a forecast calendar month."""
    if month_column and str(month_column).strip():
        return str(month_column).strip()
    return date(year, month, 1).strftime("%Y %B")


def normalize_forecast_month_label(raw: object) -> Optional[str]:
    """
    Parse a forecast ``[Month]`` value into canonical ``YYYY Month`` (e.g. ``2026 May``).

    Returns None when the value is blank or not year-first ``YYYY MonthName``.
    """
    if raw is None or (isinstance(raw, float) and np.isnan(raw)) or pd.isna(raw):
        return None
    text = str(raw).strip()
    if not text:
        return None
    match = _YEAR_MONTH_LABEL.match(text)
    if not match:
        return None
    year = int(match.group(1))
    month_token = match.group(2).strip().lower().rstrip(".")
    month_num = _MONTH_NAME_TO_NUM.get(month_token)
    if month_num is None:
        return None
    return date(year, month_num, 1).strftime("%Y %B")


def validate_forecast_month_labels(
    df: pd.DataFrame,
    max_samples: int = DATE_WARNING_SAMPLE_LIMIT,
) -> List[str]:
    """Return error lines for invalid or missing ``Month`` values (empty list = OK)."""
    if "Month" not in df.columns:
        return ["Month column missing after header mapping (required)."]

    issues: List[str] = []
    for idx, val in enumerate(df["Month"]):
        row_num = idx + 2
        if val is None or (isinstance(val, float) and np.isnan(val)) or pd.isna(val):
            issues.append(f"Row {row_num}: Month is blank or missing")
            continue
        if str(val).strip() == "":
            issues.append(f"Row {row_num}: Month is blank or missing")
            continue
        if normalize_forecast_month_label(val) is None:
            issues.append(
                f"Row {row_num}: Month {val!r} must be year-first "
                f"(e.g. '2026 May', not 'May 2026')"
            )

    if len(issues) > max_samples:
        extra = len(issues) - max_samples
        return issues[:max_samples] + [
            f"... and {extra} more row(s) with Month issues (total {len(issues)})."
        ]
    return issues


def normalize_forecast_month_column(df: pd.DataFrame) -> pd.DataFrame:
    """Rewrite ``Month`` to canonical ``YYYY Month`` labels."""
    out = df.copy()
    out["Month"] = out["Month"].map(
        lambda v: normalize_forecast_month_label(v) if not pd.isna(v) else None
    )
    return out


def distinct_forecast_months_in_df(df: pd.DataFrame) -> List[str]:
    """Distinct canonical ``Month`` labels in a mapped forecast dataframe."""
    if "Month" not in df.columns:
        return []
    seen = set()
    out: List[str] = []
    for val in df["Month"]:
        label = normalize_forecast_month_label(val)
        if label and label not in seen:
            seen.add(label)
            out.append(label)
    return sorted(out)


def forecast_months_already_in_db(
    file_months: Sequence[str],
    conn=None,
) -> List[str]:
    """Return file month labels that already exist in PCE_Monthly_Forecasts."""
    if not file_months:
        return []
    own_conn = conn is None
    if own_conn:
        conn = get_sql_conn()
    try:
        existing = fetch_distinct_forecast_months(conn)
    finally:
        if own_conn and conn is not None:
            conn.close()

    existing_by_fold = {str(m).strip().casefold(): str(m).strip() for m in existing}
    conflicts: List[str] = []
    seen = set()
    for raw in file_months:
        label = normalize_forecast_month_label(raw) or str(raw).strip()
        key = label.casefold()
        if key in existing_by_fold and key not in seen:
            seen.add(key)
            conflicts.append(existing_by_fold[key])
    return sorted(conflicts)


def _format_duplicate_months_error(conflicts: Sequence[str]) -> str:
    listed = ", ".join(conflicts)
    return (
        f"Month(s) already in PCE_Monthly_Forecasts: {listed}. "
        "Remove them using Remove selected months before importing again."
    )


def _run_frcst_prd_rebuild(
    log: Callable[[str], None],
    conn,
    own_conn: bool,
    data_lag_days: Optional[int] = None,
) -> Dict[str, Any]:
    from pce_frcst_prd_rebuild import rebuild_pce_frcst_prd

    return rebuild_pce_frcst_prd(
        log=log,
        conn=None if own_conn else conn,
        data_lag_days=data_lag_days,
    )


def read_monthly_forecast_excel(path: str, sheet_index: int = 0) -> pd.DataFrame:
    """
    Load first sheet; map headers to SQL column names; keep only known columns.
    Requires Date and UWI after mapping.
    """
    raw = pd.read_excel(path, sheet_name=sheet_index, header=0, dtype=object)
    if raw.empty:
        raise ValueError("Worksheet has no data rows.")

    pairs: List[Tuple[object, str]] = []
    target_hit: Dict[str, object] = {}

    for c in raw.columns:
        stripped = _strip_header(c)
        if not stripped:
            raise ValueError(f"Empty column header after trim: {c!r}")
        k = _fold_header_key(c)
        sql_col = _SQL_ALIASES_FOLD.get(k)
        if sql_col is None or sql_col not in _ALLOWED:
            continue
        if sql_col in target_hit and target_hit[sql_col] != c:
            raise ValueError(
                f"Two columns map to '{sql_col}': {target_hit[sql_col]!r} and {c!r}"
            )
        target_hit[sql_col] = c
        pairs.append((c, sql_col))

    if not pairs:
        raise ValueError(
            "No recognized columns found. Expected template headers such as "
            "Date, UWI, CDGR(Mcf/d), CD Cond.(bbl/d), etc."
        )

    df = raw[[p[0] for p in pairs]].copy()
    df.columns = [p[1] for p in pairs]

    missing = [r for r in ("Date", "UWI", "Month") if r not in df.columns]
    if missing:
        raise ValueError(
            "After header mapping, required column(s) missing: "
            + ", ".join(missing)
            + f". Mapped columns: {list(df.columns)}"
        )

    return df


def preview_monthly_forecast_import(
    path: str,
    conn=None,
) -> Tuple[pd.DataFrame, List[str]]:
    """Read Excel; validate Month labels; block duplicate DB months; return date warnings."""
    df = read_monthly_forecast_excel(path)
    month_errors = validate_forecast_month_labels(df)
    if month_errors:
        raise ValueError("\n".join(month_errors))
    df = normalize_forecast_month_column(df)
    conflicts = forecast_months_already_in_db(distinct_forecast_months_in_df(df), conn=conn)
    if conflicts:
        raise ValueError(_format_duplicate_months_error(conflicts))
    warnings = validate_forecast_dates(df)
    return df, warnings


def fetch_distinct_forecast_months(conn=None) -> List[str]:
    """
    Distinct non-blank ``[Month]`` values in PCE_Monthly_Forecasts.

    Returns display/delete keys (trimmed ``[Month]`` text), newest label first.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_sql_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT DISTINCT
                LTRIM(RTRIM(CAST(mf.[Month] AS NVARCHAR(4000)))) AS month_label
            FROM {TARGET_TABLE} AS mf
            WHERE NULLIF(
                LTRIM(RTRIM(CAST(mf.[Month] AS NVARCHAR(4000)))),
                N''
            ) IS NOT NULL
            ORDER BY month_label DESC
            """
        )
        out: List[str] = []
        for (month_label,) in cur.fetchall():
            if month_label is None:
                continue
            label = str(month_label).strip()
            if label:
                out.append(label)
        return out
    finally:
        if own_conn and conn is not None:
            conn.close()


def delete_forecast_months(
    months: Sequence[str],
    log_callback: Optional[Callable[[str], None]] = None,
    progress_callback: Optional[Callable[[int], None]] = None,
    conn=None,
) -> Dict[str, Any]:
    """
    Delete forecast rows whose ``[Month]`` column matches selected values,
    then rebuild PCE_FRCST_PRD.
    """
    def log(msg: str):
        if log_callback:
            log_callback(msg)

    def prog(pct: int):
        if progress_callback:
            progress_callback(min(100, max(0, pct)))

    unique_months = sorted(
        {str(m).strip() for m in months if m is not None and str(m).strip()}
    )
    if not unique_months:
        raise ValueError("No forecast months selected.")

    own_conn = conn is None
    if conn is None:
        conn = get_sql_conn()

    try:
        cur = conn.cursor()
        log(
            lf.step(
                f"Removing forecast data for {lf.num(len(unique_months))} "
                f"[Month] value(s) from {TARGET_TABLE}…"
            )
        )
        prog(10)

        deleted_total = 0
        for i, month_label in enumerate(unique_months):
            cur.execute(
                f"""
                DELETE FROM {TARGET_TABLE}
                WHERE LTRIM(RTRIM(CAST([Month] AS NVARCHAR(4000)))) = ?
                """,
                (month_label,),
            )
            deleted_total += max(getattr(cur, "rowcount", 0) or 0, 0)
            if unique_months:
                prog(10 + int(50 * (i + 1) / len(unique_months)))

        conn.commit()
        log(
            lf.detail(
                f"Deleted {lf.num(deleted_total)} forecast row(s) "
                f"for {lf.num(len(unique_months))} month(s); rebuilding PCE_FRCST_PRD…"
            )
        )
        prog(70)

        frcst_prd_rebuild: Dict[str, Any] = {}
        try:
            frcst_prd_rebuild = _run_frcst_prd_rebuild(log, conn, own_conn)
        except Exception as e:
            log(lf.error(f"PCE_FRCST_PRD rebuild after month delete: {e}"))
            raise

        prog(100)
        log(lf.success("Forecast month removal complete."))

        return {
            "deleted_forecast_rows": deleted_total,
            "months_removed": len(unique_months),
            "frcst_prd_rebuild": frcst_prd_rebuild,
        }
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        if own_conn and conn is not None:
            conn.close()


def append_monthly_forecasts_from_excel(
    path: str,
    log_callback: Optional[Callable[[str], None]] = None,
    progress_callback: Optional[Callable[[int], None]] = None,
    conn=None,
) -> Dict[str, Any]:
    def log(msg: str):
        if log_callback:
            log_callback(msg)

    def prog(pct: int):
        if progress_callback:
            progress_callback(min(100, max(0, pct)))

    log(lf.step("Reading Excel…"))
    df = read_monthly_forecast_excel(path)
    month_errors = validate_forecast_month_labels(df)
    if month_errors:
        raise ValueError("\n".join(month_errors))
    df = normalize_forecast_month_column(df)
    file_months = distinct_forecast_months_in_df(df)
    n_raw = len(df)
    date_warnings = validate_forecast_dates(df)
    colnames = [c for c in SQL_COLUMN_ORDER if c in df.columns]
    log(
        lf.detail(
            "Loaded "
            f"{lf.num(n_raw)} row(s); "
            f"{lf.num(len(colnames))} column(s): {', '.join(colnames)}; "
            f"month(s): {', '.join(file_months) or '—'}."
        )
    )
    prog(15)

    params: List[tuple] = []
    for _, row in df.iterrows():
        params.append(tuple(_cell_value_sql(row[col]) for col in colnames))

    prog(22)

    cols_sql = ", ".join(_sql_bracket_identifier(c) for c in colnames)
    ph = ", ".join(["?"] * len(colnames))
    insert_sql = f"INSERT INTO {TARGET_TABLE} ({cols_sql}) VALUES ({ph})"

    own_conn = conn is None
    if conn is None:
        conn = get_sql_conn()

    try:
        conflicts = forecast_months_already_in_db(file_months, conn=conn)
        if conflicts:
            raise ValueError(_format_duplicate_months_error(conflicts))

        cur = conn.cursor()
        cur.fast_executemany = True

        log(lf.step(f"Appending {lf.num(len(params))} row(s) into {TARGET_TABLE}…"))
        prog(28)

        total = len(params)
        for i in range(0, total, INSERT_BATCH_SIZE):
            chunk = params[i : i + INSERT_BATCH_SIZE]
            cur.executemany(insert_sql, chunk)
            done = min(i + len(chunk), total)
            if total:
                log(lf.detail(f"Inserted {lf.num(done)} / {lf.num(total)} row(s)..."))
                prog(28 + int(64 * done / total))

        conn.commit()
        log(
            lf.detail(
                "Committed PCE_Monthly_Forecasts; rebuilding PCE_FRCST_PRD "
                "from full forecasts table (append import)…"
            )
        )
        prog(93)

        frcst_prd_rebuild = _run_frcst_prd_rebuild(log, conn, own_conn)

        prog(100)

        inserted = len(params)
        eff_end = frcst_prd_rebuild.get("effective_end_date")
        log(
            lf.detail(
                "Monthly forecasts: "
                f"appended {lf.num(inserted)} row(s) for "
                f"{lf.num(len(file_months))} month(s); "
                f"PCE_FRCST_PRD forecast rows {lf.num(frcst_prd_rebuild.get('forecast_rows') or 0)}, "
                f"gathered rows {lf.num(frcst_prd_rebuild.get('gathered_rows') or 0)}"
                + (f", production through {eff_end}" if eff_end else "")
                + f"; columns: {', '.join(colnames)}."
            )
        )

        return {
            "inserted": inserted,
            "total_rows_read": n_raw,
            "months_added": file_months,
            "date_warnings": date_warnings,
            "frcst_prd_rebuild": frcst_prd_rebuild,
        }
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        if own_conn and conn is not None:
            conn.close()
