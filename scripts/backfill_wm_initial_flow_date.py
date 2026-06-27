"""
One-time backfill: set PCE_WM [Initial flow date] from an Excel file.

Expected columns (row 1 headers):
  - Gas ID (GasIDREC values; also accepts "Gas IDREC", etc.)
  - Initial Flow Date (also accepts "First Prod Date", etc.)

Usage:
  python scripts/backfill_wm_initial_flow_date.py path/to/file.xlsx
  python scripts/backfill_wm_initial_flow_date.py path/to/file.xlsx --dry-run
  python scripts/backfill_wm_initial_flow_date.py path/to/file.xlsx --yes
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db_connection import get_sql_conn, sql_target_label
from well_master_db import WellMasterDB

_GAS_HEADER_ALIASES = frozenset(
    {
        "gas id",
        "gasidrec",
        "gas idrec",
        "gas rec id",
        "gas_idrec",
        "gas id rec",
        "gasid",
    }
)

_DATE_HEADER_ALIASES = frozenset(
    {
        "initial flow date",
        "initial_flow_date",
        "initial flow",
        "first prod date",
        "first production date",
        "first_prod_date",
        "inital flow date",  # common typo
    }
)


def normalize_header(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).replace("\r", " ").replace("\n", " ")
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def normalize_gas_idrec(value: Any) -> Optional[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text or text.lower() in ("nan", "none", "n/a", "na", "-"):
        return None
    num = pd.to_numeric(text, errors="coerce")
    if pd.notna(num):
        if float(num).is_integer():
            return str(int(num))
        return str(num).rstrip("0").rstrip(".") if "." in str(num) else str(int(num))
    return text


def parse_initial_flow_date(value: Any) -> Optional[date]:
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, date):
        return value
    return WellMasterDB._coerce_additional_date(value)


def detect_columns(columns: Iterable[Any]) -> Tuple[str, str]:
    gas_col = date_col = None
    for col in columns:
        norm = normalize_header(col)
        if norm in _GAS_HEADER_ALIASES:
            gas_col = col
        elif norm in _DATE_HEADER_ALIASES:
            date_col = col
    if not gas_col or not date_col:
        raise ValueError(
            "Could not find required columns. "
            f"Need GasIDREC (aliases: {sorted(_GAS_HEADER_ALIASES)}) and "
            f"Initial flow date (aliases: {sorted(_DATE_HEADER_ALIASES)}). "
            f"Found headers: {list(columns)!r}"
        )
    return gas_col, date_col


def read_excel_rows(path: Path, *, sheet: Optional[str | int] = None) -> pd.DataFrame:
    df = pd.read_excel(
        path, sheet_name=0 if sheet is None else sheet, header=0, dtype=object
    )
    if df.empty:
        raise ValueError(f"Excel file is empty: {path}")
    gas_col, date_col = detect_columns(df.columns)
    out = df[[gas_col, date_col]].copy()
    out.columns = ["gas_idrec", "initial_flow_date"]
    return out


@dataclass
class ParsedRow:
    gas_idrec: str
    initial_flow_date: date
    excel_row: int


def parse_rows(df: pd.DataFrame) -> Tuple[List[ParsedRow], List[str]]:
    parsed: List[ParsedRow] = []
    errors: List[str] = []
    seen: Counter[str] = Counter()

    for idx, row in df.iterrows():
        excel_row = int(idx) + 2  # header is row 1
        gas_key = normalize_gas_idrec(row.get("gas_idrec"))
        flow_date = parse_initial_flow_date(row.get("initial_flow_date"))

        if not gas_key:
            errors.append(f"Row {excel_row}: missing GasIDREC")
            continue
        if flow_date is None:
            errors.append(
                f"Row {excel_row} (GasIDREC {gas_key!r}): invalid or empty Initial flow date"
            )
            continue

        seen[gas_key] += 1
        parsed.append(ParsedRow(gas_key, flow_date, excel_row))

    dupes = [k for k, n in seen.items() if n > 1]
    if dupes:
        sample = ", ".join(dupes[:5])
        more = f" (+{len(dupes) - 5} more)" if len(dupes) > 5 else ""
        errors.append(
            f"Duplicate GasIDREC in Excel ({len(dupes)}): {sample}{more}. "
            "Resolve duplicates before running."
        )
        return [], errors

    return parsed, errors


def load_wm_gas_index(conn) -> Dict[str, List[str]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT [Well Name], [GasIDREC]
        FROM dbo.PCE_WM
        WHERE [GasIDREC] IS NOT NULL
          AND LTRIM(RTRIM(CAST([GasIDREC] AS NVARCHAR(4000)))) <> N''
        """
    )
    index: Dict[str, List[str]] = {}
    for well_name, gas_idrec in cur.fetchall():
        key = normalize_gas_idrec(gas_idrec)
        if not key:
            continue
        index.setdefault(key, []).append(str(well_name).strip())
    return index


def apply_updates(
    conn,
    rows: List[ParsedRow],
    wm_index: Dict[str, List[str]],
    *,
    dry_run: bool,
) -> Tuple[int, int, List[str]]:
    updated = 0
    not_found = 0
    messages: List[str] = []

    for item in rows:
        well_names = wm_index.get(item.gas_idrec)
        if not well_names:
            not_found += 1
            messages.append(
                f"Row {item.excel_row}: GasIDREC {item.gas_idrec!r} not found in PCE_WM"
            )
            continue

        for well_name in well_names:
            if dry_run:
                messages.append(
                    f"Would set [Initial flow date] = {item.initial_flow_date.isoformat()} "
                    f"for {well_name!r} (GasIDREC {item.gas_idrec!r})"
                )
                updated += 1
                continue

            cur = conn.cursor()
            cur.execute(
                """
                UPDATE dbo.PCE_WM
                SET [Initial flow date] = ?
                WHERE [Well Name] = ?
                """,
                item.initial_flow_date,
                well_name,
            )
            if cur.rowcount > 0:
                updated += 1
                messages.append(
                    f"Updated {well_name!r} (GasIDREC {item.gas_idrec!r}) "
                    f"-> {item.initial_flow_date.isoformat()}"
                )
            else:
                messages.append(
                    f"Row {item.excel_row}: update matched 0 rows for {well_name!r}"
                )

    return updated, not_found, messages


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Backfill PCE_WM [Initial flow date] from Excel (GasIDREC match)."
    )
    parser.add_argument(
        "excel_path",
        type=Path,
        help="Path to .xlsx or .xls with GasIDREC and Initial flow date columns",
    )
    parser.add_argument(
        "--sheet",
        default=None,
        help="Excel sheet name or 0-based index (default: first sheet)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and match only; do not write to SQL Server",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip confirmation prompt before updating",
    )
    args = parser.parse_args(argv)

    path = args.excel_path.expanduser().resolve()
    if not path.is_file():
        print(f"File not found: {path}", file=sys.stderr)
        return 1

    sheet: Optional[str | int] = args.sheet
    if sheet is not None and str(sheet).isdigit():
        sheet = int(sheet)

    try:
        df = read_excel_rows(path, sheet=sheet)
    except Exception as exc:
        print(f"Failed to read Excel: {exc}", file=sys.stderr)
        return 1

    rows, parse_errors = parse_rows(df)
    if parse_errors:
        for msg in parse_errors:
            print(msg, file=sys.stderr)
        return 1

    if not rows:
        print("No valid rows to process.", file=sys.stderr)
        return 1

    print(f"Target: {sql_target_label()}")
    print(f"Excel: {path}")
    print(f"Valid rows: {len(rows)}")

    if not args.dry_run and not args.yes:
        reply = input(
            f"Update [Initial flow date] for up to {len(rows)} GasIDREC row(s)? [y/N]: "
        ).strip().lower()
        if reply not in ("y", "yes"):
            print("Cancelled.")
            return 0

    conn = get_sql_conn()
    try:
        wm_index = load_wm_gas_index(conn)
        updated, not_found, messages = apply_updates(
            conn, rows, wm_index, dry_run=args.dry_run
        )

        for msg in messages:
            print(msg)

        if not args.dry_run:
            conn.commit()

        mode = "Dry run" if args.dry_run else "Done"
        print(
            f"\n{mode}: {updated} well update(s), "
            f"{not_found} GasIDREC not found in PCE_WM"
        )

        if not_found and not args.dry_run:
            return 2
        return 0
    except Exception as exc:
        conn.rollback()
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
