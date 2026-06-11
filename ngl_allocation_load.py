"""
Bulk load monthly NGL volumes from Excel into Allocation_Factors.

Excel format: header row 3, columns PRODUCTION_DATE (YYYYMM), UWI, NGL-C2…NGL-C5, PA_NGLs.
UWI is matched to PCE_WM [Well Name] (same rules as ValNav PA monthly loader).
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from sales_allocation_updates import (
    fetch_pce_uwi_to_well_name,
    resolve_valnav_uwi_to_well_name,
)

_EXCEL_HEADER_ROW = 2  # row 3 in Excel (0-based)

_COLUMN_ALIASES = {
    "NGL_C2": "NGL-C2",
    "NGL_C3": "NGL-C3",
    "NGL_C4": "NGL-C4",
    "NGL-C4": "NGL-C4",
    "NGL_C5": "NGL-C5",
    "PA_NGLS": "PA_NGLs",
}

# (Excel column, Allocation_Factors column)
NGL_AF_FIELDS: Tuple[Tuple[str, str], ...] = (
    ("NGL-C2", "NGL_C2"),
    ("NGL-C3", "NGL_C3"),
    ("NGL-C4", "NGL_C4"),
    ("NGL-C5", "NGL_C5"),
    ("PA_NGLs", "PA_NGLs"),
)

_AF_NGL_COLS = [af for _, af in NGL_AF_FIELDS]


def _clean_uwi_text(uwi: str) -> str:
    text = str(uwi).replace("\r", "").replace("\n", "").strip()
    for ch in ("\u2010", "\u2011", "\u2012", "\u2013", "\u2014", "\u2212"):
        text = text.replace(ch, "-")
    return text


def normalize_uwi(value: Any) -> Optional[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = _clean_uwi_text(str(value))
    return text if text else None


def parse_production_date(value: Any) -> Tuple[int, int]:
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


def month_start_date(year: int, month: int) -> date:
    return date(year, month, 1)


def _normalize_excel_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename: Dict[str, str] = {}
    for col in df.columns:
        key = str(col).strip()
        upper = key.upper().replace(" ", "_")
        if upper in _COLUMN_ALIASES:
            rename[col] = _COLUMN_ALIASES[upper]
        else:
            rename[col] = key
    return df.rename(columns=rename)


def read_monthly_ngl_excel(
    path: str,
    *,
    sheet_name=0,
    uwi_column: str = "UWI",
) -> pd.DataFrame:
    """Read monthly NGL sheet; header on row 3, data from row 4."""
    raw = pd.read_excel(path, sheet_name=sheet_name, header=_EXCEL_HEADER_ROW)
    df = _normalize_excel_columns(raw)
    required = ["PRODUCTION_DATE", uwi_column] + [f[0] for f in NGL_AF_FIELDS]
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
            "Uwi": uwi,
            "Year": year,
            "Month": month,
            "MonthStartDate": month_start_date(year, month),
        }
        for excel_col, af_col in NGL_AF_FIELDS:
            val = row.get(excel_col)
            if val is None or (isinstance(val, float) and pd.isna(val)):
                entry[af_col] = None
            else:
                entry[af_col] = float(val)
        rows.append(entry)

    if not rows:
        raise ValueError("No valid monthly NGL rows after parsing Excel.")
    out = pd.DataFrame(rows)
    return out.drop_duplicates(
        subset=["Uwi", "Year", "Month"], keep="last"
    )


def resolve_excel_uwi_to_well_name(
    uwi: str, pce_uwi_dict: Dict[str, str]
) -> Optional[str]:
    return resolve_valnav_uwi_to_well_name(uwi, pce_uwi_dict)


@dataclass
class NglAllocationLoadSummary:
    excel_rows: int = 0
    matched_rows: int = 0
    rows_with_ngl_values: int = 0
    unmatched_uwis: List[str] = field(default_factory=list)
    rows_updated: int = 0
    rows_updated_by_uwi: int = 0
    rows_inserted: int = 0
    rows_no_af_match: int = 0
    dry_run: bool = False


def _float_or_none(value: Any) -> Optional[float]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if pd.isna(value):
        return None
    return float(value)


def _sql_month_start(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, datetime):
        return value.date()
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"Cannot convert month start date: {value!r}")
    return parsed.date()


def _trim_text(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def _row_has_ngl_values(item: dict) -> bool:
    return any(_float_or_none(item.get(col)) is not None for col in _AF_NGL_COLS)


def _sql_rows_affected(cursor) -> int:
    cursor.execute("SELECT @@ROWCOUNT")
    row = cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def load_ngl_excel_to_allocation_factors(
    conn,
    excel_path: str,
    *,
    dry_run: bool = False,
    log: Optional[Callable[[str], None]] = None,
) -> NglAllocationLoadSummary:
    def _log(msg: str) -> None:
        if log:
            log(msg)

    summary = NglAllocationLoadSummary(dry_run=dry_run)
    df = read_monthly_ngl_excel(excel_path)
    summary.excel_rows = len(df)
    _log(f"Parsed {summary.excel_rows:,} row(s) from {os.path.basename(excel_path)}")

    cur = conn.cursor()
    pce_uwi_dict = fetch_pce_uwi_to_well_name(cur)

    upsert_rows: List[dict] = []
    seen_unmatched: set = set()
    for _, row in df.iterrows():
        uwi = str(row["Uwi"])
        well_name = resolve_excel_uwi_to_well_name(uwi, pce_uwi_dict)
        if not well_name:
            if uwi not in seen_unmatched:
                seen_unmatched.add(uwi)
                summary.unmatched_uwis.append(uwi)
            continue
        item = {
            "MonthStartDate": _sql_month_start(row["MonthStartDate"]),
            "WellName": _trim_text(well_name),
            "UWI": _trim_text(uwi),
            **{af: row[af] for af in _AF_NGL_COLS},
        }
        upsert_rows.append(item)
        if _row_has_ngl_values(item):
            summary.rows_with_ngl_values += 1

    summary.matched_rows = len(upsert_rows)
    _log(
        f"Matched {summary.matched_rows:,} row(s) to PCE_WM wells; "
        f"{len(summary.unmatched_uwis):,} unmatched UWI(s)"
    )
    if summary.unmatched_uwis[:5]:
        for u in summary.unmatched_uwis[:5]:
            _log(f"  Unmatched: {u}")
        if len(summary.unmatched_uwis) > 5:
            _log(f"  … and {len(summary.unmatched_uwis) - 5} more")

    if not upsert_rows or dry_run:
        if dry_run:
            _log("Dry run — no database changes.")
        return summary

    source_file = os.path.basename(excel_path)
    loaded_at = datetime.now()
    set_clause = """
              [UWI] = ?
            , [NGL_C2] = ?
            , [NGL_C3] = ?
            , [NGL_C4] = ?
            , [NGL_C5] = ?
            , [PA_NGLs] = ?
            , [SourceFile] = ?
            , [LoadedAt] = ?
    """
    update_by_well_sql = f"""
        UPDATE Allocation_Factors SET {set_clause}
        WHERE CAST(MonthStartDate AS DATE) = ?
          AND LTRIM(RTRIM([Well Name])) = ?
    """
    update_by_uwi_sql = f"""
        UPDATE Allocation_Factors SET {set_clause}
        WHERE CAST(MonthStartDate AS DATE) = ?
          AND LTRIM(RTRIM([UWI])) = ?
    """
    insert_sql = """
        INSERT INTO Allocation_Factors (
            MonthStartDate, [Well Name], [UWI],
            [NGL_C2], [NGL_C3], [NGL_C4], [NGL_C5], [PA_NGLs],
            [SourceFile], [LoadedAt]
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    def _ngl_params(item: dict) -> tuple:
        return (
            item["UWI"],
            _float_or_none(item["NGL_C2"]),
            _float_or_none(item["NGL_C3"]),
            _float_or_none(item["NGL_C4"]),
            _float_or_none(item["NGL_C5"]),
            _float_or_none(item["PA_NGLs"]),
            source_file,
            loaded_at,
        )

    for item in upsert_rows:
        month_start = item["MonthStartDate"]
        well_name = item["WellName"]
        uwi = item["UWI"]
        ngl_params = _ngl_params(item)

        cur.execute(
            update_by_well_sql,
            (*ngl_params, month_start, well_name),
        )
        affected = _sql_rows_affected(cur)
        if affected > 0:
            summary.rows_updated += 1
            continue

        if uwi:
            cur.execute(
                update_by_uwi_sql,
                (*ngl_params, month_start, uwi),
            )
            affected = _sql_rows_affected(cur)
            if affected > 0:
                summary.rows_updated_by_uwi += 1
                continue

        try:
            cur.execute(
                insert_sql,
                (
                    month_start,
                    well_name,
                    uwi,
                    _float_or_none(item["NGL_C2"]),
                    _float_or_none(item["NGL_C3"]),
                    _float_or_none(item["NGL_C4"]),
                    _float_or_none(item["NGL_C5"]),
                    _float_or_none(item["PA_NGLs"]),
                    source_file,
                    loaded_at,
                ),
            )
            summary.rows_inserted += 1
        except Exception:
            summary.rows_no_af_match += 1

    conn.commit()
    _log(
        f"Allocation_Factors: {summary.rows_updated:,} updated by well name, "
        f"{summary.rows_updated_by_uwi:,} updated by UWI, "
        f"{summary.rows_inserted:,} inserted"
    )
    if summary.rows_no_af_match:
        _log(
            f"WARNING: {summary.rows_no_af_match:,} row(s) could not be matched or inserted"
        )
    return summary
