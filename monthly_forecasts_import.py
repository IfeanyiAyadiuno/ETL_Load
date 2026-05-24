"""
Monthly forecast workbook (sheet 1, header row) -> ``dbo.PCE_Monthly_Forcasts``.

Append-only: skips rows whose ``(Date, UWI)`` already exists in the table.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd

import log_format as lf
from db_connection import get_sql_conn

TARGET_TABLE = "dbo.PCE_Monthly_Forcasts"

_INSERT_COLUMNS_SQL = (
    "[Date], [UWI], [CDGR_Mcf_d], [CD_Cond_bbl_d], [CD_Water_bbl_d], "
    '[Enersight Well Name], [Month], [Pad], [Fault_Block]'
)

# Canonical column keys after normalization -> DataFrame column name we keep internally
HEADER_ALIASES: Dict[str, str] = {
    "date": "Date",
    "uwi": "UWI",
    "cdgr(mcf/d)": "CDGR_Mcf_d",
    "cdgr_mcf_d": "CDGR_Mcf_d",
    "cdgr mcf/d": "CDGR_Mcf_d",
    "cd cond.(bbl/d)": "CD_Cond_bbl_d",
    "cd_cond_bbld": "CD_Cond_bbl_d",
    "cd_cond_bbl_d": "CD_Cond_bbl_d",
    "cd water(bbl/d)": "CD_Water_bbl_d",
    "cd_water_bbld": "CD_Water_bbl_d",
    "cd_water_bbl_d": "CD_Water_bbl_d",
    "cei enersight wellname": "Enersight Well Name",
    "enersight well name": "Enersight Well Name",
    "enersight_well_name": "Enersight Well Name",
    "month": "Month",
    "pad": "Pad",
    "fault block": "Fault_Block",
    "fault_block": "Fault_Block",
}


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


def normalize_monthly_forecast_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Map flexible Excel headers to canonical names used for SQL INSERT."""
    if df.empty:
        raise ValueError("Worksheet has no rows.")
    rename: Dict[str, str] = {}
    for col in df.columns:
        fk = _fold_header_key(col)
        if fk in HEADER_ALIASES:
            rename[col] = HEADER_ALIASES[fk]
    out = df.rename(columns=rename)
    missing = [r for r in ("Date", "UWI") if r not in out.columns]
    if missing:
        raise ValueError(
            "Missing required column(s): "
            + ", ".join(missing)
            + ". Found columns: "
            + repr(list(df.columns))
        )
    return out


def read_monthly_forecast_excel(path: str, sheet_index: int = 0) -> pd.DataFrame:
    """Read Excel; first sheet, first row headers."""
    df = pd.read_excel(path, sheet_name=sheet_index, header=0, dtype=object)
    return normalize_monthly_forecast_columns(df)


def _to_py_date(val: object) -> Optional[date]:
    if val is None or (isinstance(val, float) and np.isnan(val)) or pd.isna(val):
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    try:
        ts = pd.Timestamp(val)
        if pd.isna(ts):
            return None
        return ts.date()
    except (ValueError, TypeError, OverflowError):
        return None


def _to_optional_float(val: object) -> Optional[float]:
    if val is None or val == "":
        return None
    if isinstance(val, float) and np.isnan(val):
        return None
    if pd.isna(val):
        return None
    try:
        return float(str(val).strip().replace(",", ""))
    except (ValueError, TypeError):
        return None


def _to_optional_str(val: object) -> Optional[str]:
    if val is None or val == "":
        return None
    if isinstance(val, float) and np.isnan(val):
        return None
    if pd.isna(val):
        return None
    s = str(val).strip()
    return s if s else None


def _distinct_pairs(rows: Iterable[Tuple[date, str]]) -> List[Tuple[date, str]]:
    seen: Set[Tuple[date, str]] = set()
    out: List[Tuple[date, str]] = []
    for d, u in rows:
        t = (d, u)
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _existing_pairs(cursor, pairs: Sequence[Tuple[date, str]]) -> Set[Tuple[date, str]]:
    """Return subset of pairs that already exist in PCE_Monthly_Forcasts."""
    if not pairs:
        return set()
    cursor.execute(
        """
CREATE TABLE #mf_dupcheck (
    d DATE NOT NULL,
    uwi NVARCHAR(512) NOT NULL
)
"""
    )
    cursor.fast_executemany = True
    cursor.executemany(
        "INSERT INTO #mf_dupcheck (d, uwi) VALUES (?, ?)",
        list(pairs),
    )
    cursor.execute(
        f"""
SELECT CAST(m.[Date] AS DATE) AS dt, LTRIM(RTRIM(CAST(m.[UWI] AS NVARCHAR(512)))) AS uwi
FROM {TARGET_TABLE} AS m
INNER JOIN #mf_dupcheck AS k
  ON CAST(m.[Date] AS DATE) = k.d
 AND LTRIM(RTRIM(CAST(m.[UWI] AS NVARCHAR(512)))) = k.uwi
"""
    )
    found: Set[Tuple[date, str]] = {(r[0], r[1]) for r in cursor.fetchall()}
    cursor.execute("DROP TABLE #mf_dupcheck")
    return found


def append_monthly_forecasts_from_excel(
    path: str,
    log_callback: Optional[Callable[[str], None]] = None,
    progress_callback: Optional[Callable[[int], None]] = None,
    conn=None,
) -> Dict[str, int]:
    """
    Insert rows from *path*. Returns counts:
    inserted, skipped_duplicate, skipped_invalid, total_rows_read.
    """

    def log(msg: str):
        if log_callback:
            log_callback(msg)

    def prog(pct: int):
        if progress_callback:
            progress_callback(min(100, max(0, pct)))

    df = read_monthly_forecast_excel(path)
    prog(10)

    prepared: List[Tuple] = []
    skipped_invalid = 0
    for _, row in df.iterrows():
        d = _to_py_date(row.get("Date"))
        uwi = _to_optional_str(row.get("UWI"))
        if d is None or not uwi:
            skipped_invalid += 1
            continue
        prepared.append(
            (
                d,
                uwi,
                _to_optional_float(row.get("CDGR_Mcf_d")),
                _to_optional_float(row.get("CD_Cond_bbl_d")),
                _to_optional_float(row.get("CD_Water_bbl_d")),
                _to_optional_str(row.get("Enersight Well Name")),
                _to_optional_str(row.get("Month")),
                _to_optional_str(row.get("Pad")),
                _to_optional_str(row.get("Fault_Block")),
            )
        )

    prog(25)

    if not prepared:
        log(lf.detail("Monthly forecasts: no valid rows (Date + UWI required)."))
        return {
            "inserted": 0,
            "skipped_duplicate": 0,
            "skipped_invalid": skipped_invalid,
            "total_rows_read": len(df),
        }

    own_conn = conn is None
    if conn is None:
        conn = get_sql_conn()

    try:
        cur = conn.cursor()
        uniq = _distinct_pairs([(p[0], p[1]) for p in prepared])
        existing = _existing_pairs(cur, uniq)
        prog(50)

        insert_sql = f"INSERT INTO {TARGET_TABLE} ({_INSERT_COLUMNS_SQL}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"

        inserted = 0
        skipped_duplicate = 0
        n = len(prepared)
        for i, rec in enumerate(prepared):
            d, uwi = rec[0], rec[1]
            pair = (d, uwi)
            if pair in existing:
                skipped_duplicate += 1
            else:
                cur.execute(insert_sql, rec)
                inserted += 1
                existing.add(pair)

            if n > 1 and i % max(1, n // 20) == 0:
                prog(50 + int(35 * i / n))

        conn.commit()
        prog(100)
        log(
            lf.detail(
                "Monthly forecasts: "
                f"imported {lf.num(inserted)} row(s); "
                f"skipped duplicate Date+UWI: {lf.num(skipped_duplicate)}; "
                f"skipped invalid: {lf.num(skipped_invalid)}; "
                f"spreadsheet rows: {lf.num(len(df))}"
            )
        )

        return {
            "inserted": inserted,
            "skipped_duplicate": skipped_duplicate,
            "skipped_invalid": skipped_invalid,
            "total_rows_read": len(df),
        }
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        if own_conn:
            conn.close()
