"""
Utility script to purge all data for wells marked as Exception = 'Y'.

Tables affected:
  - PCE_CDA
  - PCE_Production
  - Allocation_Factors

PCE_WM is NOT modified – it is used only to identify exception wells.
"""

from db_connection import get_sql_conn


def _purge_by_well_list(wells):
    """Internal helper: delete CDA/Production/AF rows for a list of wells."""
    if not wells:
        return 0, 0, 0

    with get_sql_conn() as conn:
        cursor = conn.cursor()

        total_cda_deleted = 0
        total_prod_deleted = 0
        total_af_deleted = 0

        for well in wells:
            # PCE_CDA
            cursor.execute("DELETE FROM PCE_CDA WHERE [Well Name] = ?", well)
            total_cda_deleted += cursor.rowcount

            # PCE_Production
            cursor.execute("DELETE FROM PCE_Production WHERE [Well Name] = ?", well)
            total_prod_deleted += cursor.rowcount

            # Allocation_Factors
            cursor.execute("DELETE FROM Allocation_Factors WHERE [Well Name] = ?", well)
            total_af_deleted += cursor.rowcount

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
        print("No wells found with Exception = 'Y'. Nothing to purge.")
        return

    print(f"Found {len(wells)} well(s) with Exception = 'Y'.")
    total_cda_deleted, total_prod_deleted, total_af_deleted = _purge_by_well_list(wells)

    print("\nPurge complete.")
    print(f"PCE_CDA rows deleted:            {total_cda_deleted:,}")
    print(f"PCE_Production rows deleted:     {total_prod_deleted:,}")
    print(f"Allocation_Factors rows deleted: {total_af_deleted:,}")


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

    print(f"\nPurged data for {len(cleaned)} well(s) marked as Exception = 'Y'.")
    print(f"PCE_CDA rows deleted:            {total_cda_deleted:,}")
    print(f"PCE_Production rows deleted:     {total_prod_deleted:,}")
    print(f"Allocation_Factors rows deleted: {total_af_deleted:,}")


if __name__ == "__main__":
    purge_exception_wells()

