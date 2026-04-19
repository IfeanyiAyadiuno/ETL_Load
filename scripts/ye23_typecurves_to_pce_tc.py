#!/usr/bin/env python3
"""
YE23 / YE2 bulk load: first sheet of the type-curve workbook -> dbo.PCE_TC (append-only),
then materialize into PCE_Production (same pipeline as GUI).

Well names are taken verbatim from the Excel Well Name column (no WM resolution, no `` - TC``).
Use names starting with ``YE2`` (or your agreed convention) so Prodview date-range deletes
and allocation guards can recognize them.

Usage:
  python scripts/ye23_typecurves_to_pce_tc.py /path/to/typecurves.xlsx
"""

from __future__ import annotations

import argparse
import sys

import log_format as lf

from type_curves_import import ye2_append_rows_to_pce_tc


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="YE2/YE23 type curves -> PCE_TC (+ production sync).")
    p.add_argument("excel_path", help="Path to type-curve Excel (sheet 1, header row 1)")
    args = p.parse_args(argv)

    def log(msg: str) -> None:
        print(msg)

    log(lf.header("YE23 TYPE CURVES -> PCE_TC", File=args.excel_path))
    out = ye2_append_rows_to_pce_tc(args.excel_path, log_callback=log)
    if not out.get("ok"):
        return 1
    log(lf.success(f"Done: {lf.num(int(out.get('rows_inserted') or 0))} row(s) inserted."))
    return 0


if __name__ == "__main__":
    sys.exit(main())
