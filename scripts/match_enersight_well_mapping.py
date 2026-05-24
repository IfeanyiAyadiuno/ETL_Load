#!/usr/bin/env python3
"""
Match ``PCE_WM.[Composite Name]`` to ``Well_Mapping.[Prod_name]`` and set
``PCE_WM.[Enersight Well Name]`` from ``Well_Mapping.[Enersight_Well_name]``.

Uses the same normalization as type curves / surveys: slash vs hyphen,
collapsed digit runs (``B098`` vs ``B98``), trailing ``W6M`` → ``W6``, and
optional stripping of the last two hyphen segments when there are many parts
(``_excel_base_for_wm_match``), so SQL-side composite strings with extra zeros
still line up with ``Prod_name`` from the mapping sheet.

Run from the repo root (requires ``.env`` / ``db_connection`` like the app)::

    python scripts/match_enersight_well_mapping.py
    python scripts/match_enersight_well_mapping.py --dry-run
    python scripts/match_enersight_well_mapping.py --mapping-table dbo.Well_Mapping

If your mapping table or column names differ, pass ``--mapping-table`` and/or
adjust the constants below.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db_connection import get_sql_conn  # noqa: E402
from type_curves_import import (  # noqa: E402
    _excel_base_for_wm_match,
    _tc_clean_well_string,
    _tc_well_match_key,
)

# Default mapping table (change via --mapping-table)
DEFAULT_MAPPING_TABLE = "dbo.Well_Mapping"

# Expected mapping columns (as in your imported sheet)
COL_PROD = "Prod_name"
COL_ENERSIGHT = "Enersight_Well_name"


def _match_key_variants(cell: object) -> List[str]:
    """Return distinct non-empty keys for one label (composite or Prod_name)."""
    if cell is None or (isinstance(cell, float) and pd.isna(cell)):
        return []
    text = str(cell).strip()
    if not text or text.lower() == "null":
        return []
    seen: Set[str] = set()
    out: List[str] = []
    cleaned = _tc_clean_well_string(text)
    if not cleaned:
        return []
    base = _excel_base_for_wm_match(cleaned)
    for frag in (cleaned, base):
        if not frag or not str(frag).strip():
            continue
        k = _tc_well_match_key(frag)
        if k and k not in seen:
            seen.add(k)
            out.append(k)
    return out


def _build_prod_key_to_enersight(
    mapping_df: pd.DataFrame,
) -> Tuple[Dict[str, str], List[str]]:
    """Map each normalized key -> Enersight well name; log key collisions."""
    key_to_label_dict: Dict[str, str] = {}
    warnings: List[str] = []

    if COL_PROD not in mapping_df.columns or COL_ENERSIGHT not in mapping_df.columns:
        raise SystemExit(
            f"Mapping table must have columns {COL_PROD!r} and {COL_ENERSIGHT!r}. "
            f"Found: {list(mapping_df.columns)}"
        )

    for _, row in mapping_df.iterrows():
        prod = row.get(COL_PROD)
        label = row.get(COL_ENERSIGHT)
        if label is None or (isinstance(label, float) and pd.isna(label)):
            continue
        enersight = str(label).strip()
        if not enersight:
            continue
        for k in _match_key_variants(prod):
            if k in key_to_label_dict:
                if key_to_label_dict[k] != enersight:
                    warnings.append(
                        f"Key collision: {k!r} -> {key_to_label_dict[k]!r} vs {enersight!r} "
                        f"(keeping first)"
                    )
                continue
            key_to_label_dict[k] = enersight

    return key_to_label_dict, warnings


def _load_mapping(conn, mapping_table: str) -> pd.DataFrame:
    q = f"SELECT * FROM {mapping_table}"
    return pd.read_sql(q, conn)


def _load_wm(conn) -> pd.DataFrame:
    q = """
    SELECT [Well Name], [Composite Name], [Enersight Well Name]
    FROM dbo.PCE_WM
    WHERE ([Exception] IS NULL OR [Exception] = '' OR [Exception] = 'N')
    """
    return pd.read_sql(q, conn)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--mapping-table",
        default=DEFAULT_MAPPING_TABLE,
        help=f"Mapping table name (default: {DEFAULT_MAPPING_TABLE})",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be updated without committing",
    )
    p.add_argument(
        "--also-well-name",
        action="store_true",
        help="If [Composite Name] is empty, try keys from [Well Name] instead",
    )
    args = p.parse_args()

    with get_sql_conn() as conn:
        mapping_df = _load_mapping(conn, args.mapping_table)
        wm_df = _load_wm(conn)

    key_to_enersight, collisions = _build_prod_key_to_enersight(mapping_df)
    for w in collisions[:50]:
        print(w, file=sys.stderr)
    if len(collisions) > 50:
        print(f"... and {len(collisions) - 50} more key collision warnings", file=sys.stderr)

    updates: List[Tuple[str, str]] = []
    unmatched: List[str] = []
    skipped_no_label: List[str] = []

    for _, row in wm_df.iterrows():
        wn = row.get("Well Name")
        if wn is None or (isinstance(wn, float) and pd.isna(wn)):
            continue
        well_name = str(wn).strip()
        if not well_name:
            continue

        composite = row.get("Composite Name")
        cells_to_try: List[object] = []
        if composite is not None and not (isinstance(composite, float) and pd.isna(composite)):
            cs = str(composite).strip()
            if cs:
                cells_to_try.append(composite)
        if args.also_well_name and not cells_to_try:
            cells_to_try.append(row.get("Well Name"))

        enersight: str | None = None
        for cell in cells_to_try:
            for k in _match_key_variants(cell):
                if k in key_to_enersight:
                    enersight = key_to_enersight[k]
                    break
            if enersight is not None:
                break

        if enersight is None:
            if cells_to_try:
                unmatched.append(well_name)
            else:
                skipped_no_label.append(well_name)
            continue

        current = row.get("Enersight Well Name")
        cur_s = (
            None
            if current is None or (isinstance(current, float) and pd.isna(current))
            else str(current).strip()
        )
        if cur_s == enersight:
            continue
        updates.append((enersight, well_name))

    print(f"Mapping rows: {len(mapping_df)}")
    print(f"PCE_WM rows (non-exception): {len(wm_df)}")
    print(f"Distinct prod keys: {len(key_to_enersight)}")
    print(f"Rows to update: {len(updates)}")
    print(f"Unmatched (had composite/label tried, no map key): {len(unmatched)}")
    print(f"Skipped (no composite name{' / well name' if args.also_well_name else ''}): {len(skipped_no_label)}")
    if unmatched:
        sample = unmatched[:25]
        print("Unmatched well names (sample):", ", ".join(sample))

    if args.dry_run:
        for en, wn in updates[:30]:
            print(f"  DRY-RUN: {wn!r} -> {en!r}")
        if len(updates) > 30:
            print(f"  ... and {len(updates) - 30} more")
        return

    if not updates:
        print("Nothing to commit.")
        return

    sql = "UPDATE dbo.PCE_WM SET [Enersight Well Name] = ? WHERE [Well Name] = ?"
    with get_sql_conn() as conn:
        cur = conn.cursor()
        cur.fast_executemany = True
        cur.executemany(sql, updates)
        conn.commit()

    print(f"Committed {len(updates)} update(s).")


if __name__ == "__main__":
    main()
