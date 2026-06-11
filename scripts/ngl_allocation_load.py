#!/usr/bin/env python3
"""
Bulk load monthly NGL volumes from Excel into Allocation_Factors.

  python scripts/ngl_allocation_load.py --excel /path/to/ngl.xlsx
  python scripts/ngl_allocation_load.py --excel /path/to/ngl.xlsx --dry-run

Run scripts/add_allocation_factors_ngl_columns.sql in SSMS first if columns
are missing. After bulk load, use ValNav Monthly Update (Sales + NGL) per month.
"""

import argparse
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

import log_format as lf  # noqa: E402
from db_connection import get_sql_conn  # noqa: E402
from ngl_allocation_load import load_ngl_excel_to_allocation_factors  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Load NGL Excel (PRODUCTION_DATE + UWI) into Allocation_Factors"
    )
    parser.add_argument(
        "--excel",
        required=True,
        help="Path to NGL Excel workbook (header on row 3)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and match UWIs only; do not write to the database",
    )
    args = parser.parse_args()

    excel_path = Path(args.excel)
    if not excel_path.is_file():
        print(lf.error(f"Excel file not found: {excel_path}"))
        return 1

    print(lf.step(f"Loading NGL Excel → Allocation_Factors"))
    print(lf.detail(f"File: {excel_path}"))

    try:
        conn = get_sql_conn()
        summary = load_ngl_excel_to_allocation_factors(
            conn,
            str(excel_path),
            dry_run=args.dry_run,
            log=print,
        )
        conn.close()
    except Exception as exc:
        print(lf.error(str(exc)))
        return 1

    print(
        lf.summary(
            "Complete",
            {
                "Excel rows": summary.excel_rows,
                "Matched rows": summary.matched_rows,
                "Updated": summary.rows_updated,
                "Inserted": summary.rows_inserted,
                "Unmatched UWIs": len(summary.unmatched_uwis),
                "Dry run": summary.dry_run,
            },
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
