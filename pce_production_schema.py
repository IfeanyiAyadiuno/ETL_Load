"""
Canonical PCE_Production INSERT schema and bulk-insert helpers.

Single source of truth for column lists and INSERT SQL used by production rebuild,
Prodview quick update, and type-curve materialization paths.
"""

from __future__ import annotations

from typing import Callable, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

import log_format as lf

SQL_INSERT_BATCH_SIZE = 20000

PCE_PRODUCTION_INSERT_COLUMNS: Tuple[str, ...] = (
    "Date",
    "Days Seq",
    "Day Seq UPRT",
    "Well Name",
    "UWI",
    "Gas WH Production (10³m³)",
    "Condensate WH (m³/d)",
    "Gas S2 Production (10³m³)",
    "Gas Sales Production (10³m³)",
    "Condensate Sales (m³/d)",
    "Gathered Gas (e³m³/d)",
    "Gathered Condensate (m³/d)",
    "Gath. Water Rate (m³/d)",
    "Sales CGR (m³/e³m³)",
    "CGR (m³/e³m³)",
    "WGR (m³/e³m³)",
    "ECF",
    "Hours On",
    "Tubing Pressure (kPa)",
    "Casing Pressure (kPa)",
    "Choke Size",
    "Gas WH Cumulative Production (10³m³)",
    "Gas S2 Cumulative Production (10³m³)",
    "Gas Sales Cumulative Production (10³m³)",
    "Condensate Sales Cumulative Production (m³)",
    "Condensate WH Cumulative Production (m³)",
    "Gas Gathered Cumulative (e³m³)",
    "Condensate Gathered Cumulative (m³)",
    "Gath. Water Cumulative (m³)",
    "Formation Producer",
    "Layer Producer",
    "Fault Block",
    "Pad Name",
    "Lateral Length",
    "Orientation",
    "On Production Year",
    "Alloc. Water Rate (m³)",
    "NGL (m³)",
    "Gas WH Avg (10³m³)",
    "Gas S2 Avg (10³m³)",
    "Gas Gathered Avg (e³m³/d)",
    "Condensate Gathered Avg (m³/d)",
    "Gath. Water Avg (m³/d)",
    "Alloc. Water Avg (m³)",
    "Month",
)

PCE_PRODUCTION_TC_EXTRA_COLUMNS: Tuple[str, ...] = ("Remarks",)


def production_insert_columns(
    *,
    include_uwi: bool = True,
    extra_columns: Sequence[str] = (),
) -> Tuple[str, ...]:
    """Column list for INSERT; type-curve path omits UWI and appends Remarks."""
    cols = list(PCE_PRODUCTION_INSERT_COLUMNS)
    if not include_uwi and "UWI" in cols:
        cols.remove("UWI")
    for col in extra_columns:
        if col not in cols:
            cols.append(col)
    return tuple(cols)


def build_production_insert_sql(
    *,
    include_uwi: bool = True,
    extra_columns: Sequence[str] = (),
    table_name: str = "PCE_Production",
) -> str:
    cols = production_insert_columns(include_uwi=include_uwi, extra_columns=extra_columns)
    bracketed = ", ".join(f"[{c}]" for c in cols)
    placeholders = ", ".join("?" for _ in cols)
    return f"""
INSERT INTO {table_name} (
    {bracketed}
) VALUES ({placeholders})
""".strip()


def df_to_insert_rows(df: pd.DataFrame, columns: Sequence[str]) -> List[Tuple]:
    """Vectorized NaN->None via astype(object) + itertuples."""
    sub = df[list(columns)].astype(object)
    sub[sub.isna()] = None
    return list(sub.itertuples(index=False, name=None))


def batch_executemany(
    cursor,
    sql: str,
    rows: Sequence[Tuple],
    *,
    batch_size: int = SQL_INSERT_BATCH_SIZE,
    log: Optional[Callable[[str], None]] = None,
    label: str = "Insert",
    progress: Optional[Callable[[int], None]] = None,
    progress_lo: int = 0,
    progress_hi: int = 100,
) -> None:
    """Execute INSERT/UPDATE in batches with optional log and progress callbacks."""
    total = len(rows)
    if total == 0:
        return
    for i in range(0, total, batch_size):
        batch = rows[i : i + batch_size]
        cursor.executemany(sql, batch)
        done = min(i + len(batch), total)
        if log:
            pct = int(100 * done / total)
            log(lf.detail(f"{label} progress: {lf.num(done)}/{lf.num(total)} ({pct}%)"))
        if progress:
            pct = progress_lo + (progress_hi - progress_lo) * (done / total)
            progress(int(pct))


def executemany_with_row_fallback(
    cursor,
    sql: str,
    rows: Sequence[Tuple],
    *,
    batch_size: int = SQL_INSERT_BATCH_SIZE,
    commit_every_rows: int = 100_000,
    log: Optional[Callable[[str], None]] = None,
    progress: Optional[Callable[[int], None]] = None,
    progress_lo: int = 0,
    progress_hi: int = 100,
) -> Tuple[int, int]:
    """
    Batch executemany with row-by-row fallback on batch failure.

    Returns (rows_inserted, duplicate_skipped).
    """
    total_inserted = 0
    duplicate_skipped = 0
    total_rows = len(rows)
    rows_since_commit = 0

    for i in range(0, total_rows, batch_size):
        batch = rows[i : i + batch_size]
        try:
            cursor.executemany(sql, batch)
            total_inserted += len(batch)
        except Exception as batch_e:
            if log:
                log(lf.warn(f"Batch at row {i} failed ({batch_e}); row-by-row fallback."))
            for j, row in enumerate(batch):
                try:
                    cursor.execute(sql, row)
                    total_inserted += 1
                except Exception as row_e:
                    if "Violation of UNIQUE KEY" in str(row_e):
                        duplicate_skipped += 1
                    elif log:
                        log(lf.error(f"Error inserting row {i + j}: {row_e}"))

        rows_done = i + len(batch)
        rows_since_commit += len(batch)
        if progress and total_rows:
            pct = progress_lo + (progress_hi - progress_lo) * (rows_done / total_rows)
            progress(int(pct))

        if rows_since_commit >= commit_every_rows:
            cursor.connection.commit()
            rows_since_commit = 0

    return total_inserted, duplicate_skipped
