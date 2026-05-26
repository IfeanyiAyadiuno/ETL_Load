"""
Monthly forecast workbook (first sheet, row 1 = headers) -> ``dbo.PCE_Monthly_Forecasts``.

Excel headers are mapped to the table’s real column names (template uses labels like
``CDGR(Mcf/d)``; SQL uses ``CDGR_Mcf_d``, etc.). Unmapped columns are ignored.

Each import **clears** ``dbo.PCE_Monthly_Forecasts``, then inserts all rows from the file
(in batches). Afterwards ``rebuild_pce_frcst_prd`` refreshes ``dbo.PCE_FRCST_PRD``
(forecasts + gathered production capped at ``prodview_effective_end_date()``).
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

import log_format as lf
from db_connection import get_sql_conn

TARGET_TABLE = "dbo.PCE_Monthly_Forecasts"

INSERT_BATCH_SIZE = 2500

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


def append_monthly_forecasts_from_excel(
    path: str,
    log_callback: Optional[Callable[[str], None]] = None,
    progress_callback: Optional[Callable[[int], None]] = None,
    conn=None,
) -> Dict[str, int]:
    def log(msg: str):
        if log_callback:
            log_callback(msg)

    def prog(pct: int):
        if progress_callback:
            progress_callback(min(100, max(0, pct)))

    log(lf.step("Reading Excel…"))
    df = read_monthly_forecast_excel(path)
    n_raw = len(df)
    colnames = [c for c in SQL_COLUMN_ORDER if c in df.columns]
    log(
        lf.detail(
            "Loaded "
            f"{lf.num(n_raw)} row(s); "
            f"{lf.num(len(colnames))} column(s): {', '.join(colnames)}."
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
        cur = conn.cursor()
        cur.fast_executemany = True

        log(lf.step("Clearing dbo.PCE_Monthly_Forecasts (full replace for this import)…"))
        prog(24)
        cur.execute(f"DELETE FROM {TARGET_TABLE}")
        deleted_prior = getattr(cur, "rowcount", -1)
        if deleted_prior is not None and deleted_prior >= 0:
            log(lf.detail(f"Cleared prior forecast row(s): {lf.num(deleted_prior)}."))

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
        log(lf.detail("Committed PCE_Monthly_Forecasts; syncing PCE_FRCST_PRD…"))
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
                f"columns: {', '.join(colnames)}; "
                f"{lf.num(n_raw)} row(s) read from workbook."
            )
        )

        return {
            "inserted": inserted,
            "total_rows_read": n_raw,
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
