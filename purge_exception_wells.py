"""
Utility script to purge all data for wells marked as Exception = 'Y'.

Tables affected (by [Well Name], before any PCE_WM delete):
  - PCE_CDA
  - PCE_Production
  - Allocation_Factors
  - PCE_Surveys

PCE_WM is NOT modified by purge_exception_wells – it is used only to identify exception wells.
Well Master removal uses delete_dependent_rows_for_well_master then deletes PCE_WM.
"""

from typing import Dict, List, Sequence

import log_format as lf
from db_connection import get_sql_conn

_WM_DEPENDENT_TABLES = [
    ("PCE_CDA", "[Well Name]"),
    ("PCE_Production", "[Well Name]"),
    ("Allocation_Factors", "[Well Name]"),
    ("PCE_Surveys", "[Well Name]"),
]


def delete_dependent_rows_for_well_master(cursor, wells: Sequence[str]) -> Dict[str, int]:
    """
    DELETE rows in tables that reference PCE_WM.[Well Name].
    Does not commit — caller owns the transaction (e.g. Well Master delete + PCE_WM row).
    """
    counts = {t: 0 for t, _ in _WM_DEPENDENT_TABLES}
    cleaned = [str(w).strip() for w in wells if w and str(w).strip()]
    if not cleaned:
        return counts
    placeholders = ",".join("?" for _ in cleaned)
    params = tuple(cleaned)
    for table, col in _WM_DEPENDENT_TABLES:
        cursor.execute(
            f"DELETE FROM {table} WHERE {col} IN ({placeholders})",
            params,
        )
        counts[table] = cursor.rowcount
    return counts


def _purge_by_well_list(wells: List[str]):
    """Commit helper: delete dependent rows for a list of wells."""
    if not wells:
        return 0, 0, 0, 0

    with get_sql_conn() as conn:
        cursor = conn.cursor()
        counts = delete_dependent_rows_for_well_master(cursor, wells)
        conn.commit()

    return (
        counts["PCE_CDA"],
        counts["PCE_Production"],
        counts["Allocation_Factors"],
        counts["PCE_Surveys"],
    )


def purge_exception_wells():
    """Delete all non-WM data for wells where PCE_WM.[Exception] = 'Y'."""
    with get_sql_conn() as conn:
        cursor = conn.cursor()

        # Get list of exception wells from PCE_WM
        cursor.execute(
            """
            SELECT [Well Name]
            FROM PCE_WM
            WHERE [Exception] = 'Y'
              AND [Well Name] IS NOT NULL
            """
        )
        wells = [row[0] for row in cursor.fetchall()]

    if not wells:
        print(lf.detail("No wells found with Exception = 'Y'. Nothing to purge."))
        return

    print(lf.detail(f"Found {lf.num(len(wells))} well(s) with Exception = 'Y'."))
    cda, prod, af, surv = _purge_by_well_list(wells)

    print(lf.success("Purge complete."))
    print(lf.item(f"PCE_CDA rows deleted: {lf.num(cda)}"))
    print(lf.item(f"PCE_Production rows deleted: {lf.num(prod)}"))
    print(lf.item(f"Allocation_Factors rows deleted: {lf.num(af)}"))
    print(lf.item(f"PCE_Surveys rows deleted: {lf.num(surv)}"))


def purge_wells(well_names):
    """
    Delete CDA/Production/AF data for the specified wells
    (used when Exception is changed from 'N' to 'Y' in the GUI).
    """
    # Ensure unique, non-empty names
    cleaned = sorted({str(w).strip() for w in well_names if w})
    if not cleaned:
        return

    cda, prod, af, surv = _purge_by_well_list(cleaned)

    print(lf.success(f"Purged data for {lf.num(len(cleaned))} well(s)."))
    print(lf.item(f"PCE_CDA rows deleted: {lf.num(cda)}"))
    print(lf.item(f"PCE_Production rows deleted: {lf.num(prod)}"))
    print(lf.item(f"Allocation_Factors rows deleted: {lf.num(af)}"))
    print(lf.item(f"PCE_Surveys rows deleted: {lf.num(surv)}"))


if __name__ == "__main__":
    purge_exception_wells()

