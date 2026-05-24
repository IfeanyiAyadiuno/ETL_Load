"""
Monthly forecast workbook (first sheet, row 1 = headers) -> ``dbo.PCE_Monthly_Forecasts``.

Imports rows as-is: column names come straight from Excel (trimmed/BOM stripped only),
all data rows are inserted. No duplicate checking or header renaming.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Callable, Dict, List, Optional

import numpy as np
import pandas as pd

import log_format as lf
from db_connection import get_sql_conn

TARGET_TABLE = "dbo.PCE_Monthly_Forecasts"


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
    """Bracketed identifier for SQL Server (escape ] as ]])."""
    return "[" + name.replace("]", "]]") + "]"


def _cell_value_sql(val: object):
    """Map Excel/pandas cell to a value pyodbc can bind."""
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


def read_monthly_forecast_excel(path: str, sheet_index: int = 0) -> pd.DataFrame:
    """Load first sheet with header row 0; strip whitespace from column titles only."""
    df = pd.read_excel(path, sheet_name=sheet_index, header=0, dtype=object)
    if df.empty:
        raise ValueError("Worksheet has no data rows.")

    rename: Dict[object, str] = {}
    seen: set = set()
    new_cols: List[str] = []
    for c in df.columns:
        stripped = _strip_header(c)
        if not stripped:
            raise ValueError(f"Empty column header after trim: {c!r}")
        if stripped in seen:
            raise ValueError(f"Duplicate column name after trim: {stripped!r}")
        seen.add(stripped)
        rename[c] = stripped
        new_cols.append(stripped)

    return df.rename(columns=rename)


def append_monthly_forecasts_from_excel(
    path: str,
    log_callback: Optional[Callable[[str], None]] = None,
    progress_callback: Optional[Callable[[int], None]] = None,
    conn=None,
) -> Dict[str, int]:
    """
    Insert every data row using the workbook column names as SQL column names.

    Returns: ``inserted`` (rows written), ``total_rows_read``.
    """

    def log(msg: str):
        if log_callback:
            log_callback(msg)

    def prog(pct: int):
        if progress_callback:
            progress_callback(min(100, max(0, pct)))

    df = read_monthly_forecast_excel(path)
    nrows = len(df)
    colnames = list(df.columns)
    cols_sql = ", ".join(_sql_bracket_identifier(c) for c in colnames)
    ph = ", ".join(["?"] * len(colnames))
    insert_sql = f"INSERT INTO {TARGET_TABLE} ({cols_sql}) VALUES ({ph})"

    prog(15)

    params: List[tuple] = []
    for idx, (_, row) in enumerate(df.iterrows()):
        params.append(tuple(_cell_value_sql(row[col]) for col in colnames))
        if nrows > 1 and idx % max(1, nrows // 15) == 0:
            prog(15 + int(75 * idx / nrows))

    own_conn = conn is None
    if conn is None:
        conn = get_sql_conn()

    try:
        cur = conn.cursor()
        cur.fast_executemany = True
        cur.executemany(insert_sql, params)
        conn.commit()
        prog(100)

        inserted = len(params)
        log(
            lf.detail(
                "Monthly forecasts: "
                f"imported {lf.num(inserted)} row(s); "
                f"{lf.num(len(colnames))} column(s): {', '.join(colnames)}; "
                f"spreadsheet data rows: {lf.num(nrows)}"
            )
        )

        return {
            "inserted": inserted,
            "total_rows_read": nrows,
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
