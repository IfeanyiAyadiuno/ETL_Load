"""
Utility script to purge all data for wells marked as Exception = 'Y'.

Tables affected:
  - PCE_CDA
  - PCE_Production
  - Allocation_Factors

PCE_WM is NOT modified – it is used only to identify exception wells.
"""

import log_format as lf
from db_connection import get_sql_conn


def _purge_by_well_list(wells):
    """Internal helper: delete CDA/Production/AF rows for a list of wells."""
    if not wells:
        return 0, 0, 0

    with get_sql_conn() as conn:
        cursor = conn.cursor()
        placeholders = ",".join("?" for _ in wells)
        params = tuple(wells)

        total_cda_deleted = 0
        total_prod_deleted = 0
        total_af_deleted = 0

        for table, col in [
            ("PCE_CDA", "[Well Name]"),
            ("PCE_Production", "[Well Name]"),
            ("Allocation_Factors", "[Well Name]"),
        ]:
            cursor.execute(
                f"DELETE FROM {table} WHERE {col} IN ({placeholders})",
                params,
            )
            n = cursor.rowcount
            if table == "PCE_CDA":
                total_cda_deleted = n
            elif table == "PCE_Production":
                total_prod_deleted = n
            else:
                total_af_deleted = n

        conn.commit()

    return total_cda_deleted, total_prod_deleted, total_af_deleted


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
    total_cda_deleted, total_prod_deleted, total_af_deleted = _purge_by_well_list(wells)

    print(lf.success("Purge complete."))
    print(lf.item(f"PCE_CDA rows deleted: {lf.num(total_cda_deleted)}"))
    print(lf.item(f"PCE_Production rows deleted: {lf.num(total_prod_deleted)}"))
    print(lf.item(f"Allocation_Factors rows deleted: {lf.num(total_af_deleted)}"))


def purge_wells(well_names):
    """
    Delete CDA/Production/AF data for the specified wells
    (used when Exception is changed from 'N' to 'Y' in the GUI).
    """
    # Ensure unique, non-empty names
    cleaned = sorted({str(w).strip() for w in well_names if w})
    if not cleaned:
        return

    total_cda_deleted, total_prod_deleted, total_af_deleted = _purge_by_well_list(cleaned)

    print(lf.success(f"Purged data for {lf.num(len(cleaned))} well(s)."))
    print(lf.item(f"PCE_CDA rows deleted: {lf.num(total_cda_deleted)}"))
    print(lf.item(f"PCE_Production rows deleted: {lf.num(total_prod_deleted)}"))
    print(lf.item(f"Allocation_Factors rows deleted: {lf.num(total_af_deleted)}"))


if __name__ == "__main__":
    purge_exception_wells()

