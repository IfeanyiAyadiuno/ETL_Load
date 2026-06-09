"""
Monthly forecast workbook (first sheet, row 1 = headers) -> ``dbo.PCE_Monthly_Forecasts``.

Excel headers are mapped to the table's real column names (template uses labels like
``CDGR(Mcf/d)``; SQL uses ``CDGR_Mcf_d``, etc.). Unmapped columns are ignored.

Each import **appends** rows from the file: existing ``(Date, UWI)`` keys in the file
are replaced (deleted then re-inserted). Afterwards ``rebuild_pce_frcst_prd`` refreshes
``dbo.PCE_FRCST_PRD`` (forecasts + gathered production capped at
``prodview_effective_end_date()``).

Selected forecast months can be removed via ``delete_forecast_months`` (GUI), which
deletes from ``PCE_Monthly_Forecasts`` and rebuilds ``PCE_FRCST_PRD``.
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
    return date(year, month, 1).strftime("%b %Y")


def distinct_forecast_keys(df: pd.DataFrame) -> List[Tuple[date, str]]:
    """Distinct (Date, UWI) keys from mapped forecast rows; skips null Date or UWI."""
    keys: set[Tuple[date, str]] = set()
    for _, row in df.iterrows():
        d = _cell_value_sql(row.get("Date"))
        u = _cell_value_sql(row.get("UWI"))
        if d is None or u is None:
            continue
        u_str = str(u).strip()
        if not u_str:
            continue
        if not isinstance(d, date):
            continue
        keys.add((d, u_str))
    return sorted(keys)


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

    missing = [r for r in ("Date", "UWI") if r not in df.columns]
    if missing:
        raise ValueError(
            "After header mapping, required column(s) missing: "
            + ", ".join(missing)
            + f". Mapped columns: {list(df.columns)}"
        )

    return df


def preview_monthly_forecast_import(path: str) -> Tuple[pd.DataFrame, List[str]]:
    """Read Excel and return mapped DataFrame plus date-year validation warnings."""
    df = read_monthly_forecast_excel(path)
    warnings = validate_forecast_dates(df)
    return df, warnings


def fetch_distinct_forecast_months(conn=None) -> List[Tuple[int, int, str]]:
    """
    Distinct calendar months present in PCE_Monthly_Forecasts.

    Returns (year, month, display_label) tuples, newest first.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_sql_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
                  YEAR(CAST(mf.[Date] AS DATE)) AS yr
                , MONTH(CAST(mf.[Date] AS DATE)) AS mo
                , MAX(
                      NULLIF(
                          LTRIM(RTRIM(CAST(mf.[Month] AS NVARCHAR(4000)))),
                          N''
                      )
                  ) AS month_label
            FROM {TARGET_TABLE} AS mf
            WHERE mf.[Date] IS NOT NULL
            GROUP BY
                  YEAR(CAST(mf.[Date] AS DATE))
                , MONTH(CAST(mf.[Date] AS DATE))
            ORDER BY yr DESC, mo DESC
            """
        )
        out: List[Tuple[int, int, str]] = []
        for yr, mo, month_label in cur.fetchall():
            year = int(yr)
            month = int(mo)
            label = forecast_month_display_label(year, month, month_label)
            out.append((year, month, label))
        return out
    finally:
        if own_conn and conn is not None:
            conn.close()


def _delete_forecast_keys_by_temp_table(cur, keys: List[Tuple[date, str]]) -> int:
    if not keys:
        return 0
    cur.execute("CREATE TABLE #ForecastKeys ([Date] DATE, [Uwi] NVARCHAR(4000))")
    try:
        cur.executemany(
            "INSERT INTO #ForecastKeys ([Date], [Uwi]) VALUES (?, ?)",
            keys,
        )
        cur.execute(
            f"""
            DELETE mf
            FROM {TARGET_TABLE} AS mf
            INNER JOIN #ForecastKeys AS k
                ON mf.[Date] = k.[Date]
               AND mf.[UWI] = k.[Uwi]
            """
        )
        return max(getattr(cur, "rowcount", 0) or 0, 0)
    finally:
        cur.execute("DROP TABLE #ForecastKeys")


def delete_forecast_months(
    months: Sequence[Tuple[int, int]],
    log_callback: Optional[Callable[[str], None]] = None,
    progress_callback: Optional[Callable[[int], None]] = None,
    conn=None,
) -> Dict[str, Any]:
    """
    Delete forecast rows for selected calendar months, then rebuild PCE_FRCST_PRD.
    """
    def log(msg: str):
        if log_callback:
            log_callback(msg)

    def prog(pct: int):
        if progress_callback:
            progress_callback(min(100, max(0, pct)))

    unique_months = sorted({(int(y), int(m)) for y, m in months})
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
                f"calendar month(s) from {TARGET_TABLE}…"
            )
        )
        prog(10)

        deleted_total = 0
        for i, (year, month) in enumerate(unique_months):
            cur.execute(
                f"""
                DELETE FROM {TARGET_TABLE}
                WHERE YEAR(CAST([Date] AS DATE)) = ?
                  AND MONTH(CAST([Date] AS DATE)) = ?
                """,
                (year, month),
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

        prd_rebuilt = False
        try:
            from pce_frcst_prd_rebuild import rebuild_pce_frcst_prd

            rebuild_pce_frcst_prd(log=log, conn=None if own_conn else conn)
            prd_rebuilt = True
        except Exception as e:
            log(lf.warn(f"PCE_FRCST_PRD rebuild after month delete: {e}"))

        prog(100)
        log(lf.success("Forecast month removal complete."))

        return {
            "deleted_forecast_rows": deleted_total,
            "months_removed": len(unique_months),
            "prd_rebuilt": prd_rebuilt,
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
    n_raw = len(df)
    date_warnings = validate_forecast_dates(df)
    colnames = [c for c in SQL_COLUMN_ORDER if c in df.columns]
    log(
        lf.detail(
            "Loaded "
            f"{lf.num(n_raw)} row(s); "
            f"{lf.num(len(colnames))} column(s): {', '.join(colnames)}."
        )
    )
    prog(15)

    keys = distinct_forecast_keys(df)
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
        cur = conn.cursor()
        cur.fast_executemany = True

        deleted_rows = 0
        if keys:
            log(
                lf.step(
                    f"Replacing existing rows for {lf.num(len(keys))} "
                    f"distinct (Date, UWI) key(s)…"
                )
            )
            prog(24)
            deleted_rows = _delete_forecast_keys_by_temp_table(cur, keys)
            log(lf.detail(f"Removed {lf.num(deleted_rows)} existing row(s) for overlap keys."))
        else:
            log(lf.detail("No valid (Date, UWI) keys in file; skipping key replace delete."))

        log(lf.step(f"Inserting into {TARGET_TABLE}…"))
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

        try:
            from pce_frcst_prd_rebuild import rebuild_pce_frcst_prd

            rebuild_pce_frcst_prd(log=log, conn=None if own_conn else conn)
        except Exception as e:
            log(lf.warn(f"PCE_FRCST_PRD rebuild after forecasts import: {e}"))

        prog(100)

        inserted = len(params)
        log(
            lf.detail(
                "Monthly forecasts: "
                f"imported {lf.num(inserted)} row(s); "
                f"replaced {lf.num(len(keys))} key(s) "
                f"({lf.num(deleted_rows)} prior row(s) removed); "
                f"columns: {', '.join(colnames)}; "
                f"{lf.num(n_raw)} row(s) read from workbook."
            )
        )

        return {
            "inserted": inserted,
            "total_rows_read": n_raw,
            "replaced_keys": len(keys),
            "deleted_rows": deleted_rows,
            "date_warnings": date_warnings,
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
