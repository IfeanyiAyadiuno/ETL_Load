"""
Standalone diagnostic: check whether a specific well is picked up
in the PCE_WM → Snowflake → PCE_CDA pipeline.
"""

import sys
from db_connection import get_sql_conn
from snowflake_connector import SnowflakeConnector

WELL_NAME = "B-G095-H/094-B-08"

def main():
    well = WELL_NAME if len(sys.argv) < 2 else sys.argv[1]
    print(f"\n{'=' * 64}")
    print(f"  Diagnosing well: {well}")
    print(f"{'=' * 64}\n")

    # ------------------------------------------------------------------
    # 1. Check PCE_WM
    # ------------------------------------------------------------------
    print("[1] Checking PCE_WM ...")
    conn = get_sql_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT [Well Name], GasIDREC, PressuresIDREC, [Exception]
        FROM PCE_WM
        WHERE [Well Name] = ?
    """, well)
    wm_rows = cursor.fetchall()

    if not wm_rows:
        print(f"    NOT FOUND in PCE_WM. The well won't appear in CDA at all.")
        print(f"    → Add it via Import New Wells on the Well Master screen.\n")
        conn.close()
        return

    for r in wm_rows:
        wn, gas_id, pres_id, exc = r
        print(f"    Well Name:       {wn}")
        print(f"    GasIDREC:        {gas_id}")
        print(f"    PressuresIDREC:  {pres_id}")
        print(f"    Exception:       {exc!r}")

        if exc and str(exc).strip().upper() not in ('', 'N'):
            print(f"    ⚠  Exception flag is '{exc}' — this well is EXCLUDED.")
        if not gas_id:
            print(f"    ⚠  GasIDREC is NULL — the well will be skipped by the CDA builder.")
        if not pres_id:
            print(f"    ⚠  PressuresIDREC is NULL — pressure/CGR/alloc data won't merge.")

    gas_idrec = str(wm_rows[0][1]).strip() if wm_rows[0][1] else None
    pres_idrec = str(wm_rows[0][2]).strip() if wm_rows[0][2] else None

    # ------------------------------------------------------------------
    # 2. Check PCE_CDA
    # ------------------------------------------------------------------
    print(f"\n[2] Checking PCE_CDA ...")
    cursor.execute("""
        SELECT COUNT(*) AS cnt,
               MIN(ProdDate) AS earliest,
               MAX(ProdDate) AS latest
        FROM PCE_CDA
        WHERE [Well Name] = ?
    """, well)
    cda = cursor.fetchone()
    if cda[0] == 0:
        print(f"    NO rows in PCE_CDA for this well.")
    else:
        print(f"    {cda[0]} rows  |  {cda[1]} to {cda[2]}")

    # ------------------------------------------------------------------
    # 3. Check PCE_Production
    # ------------------------------------------------------------------
    print(f"\n[3] Checking PCE_Production ...")
    cursor.execute("""
        SELECT COUNT(*) AS cnt,
               MIN([Date]) AS earliest,
               MAX([Date]) AS latest
        FROM PCE_Production
        WHERE [Well Name] = ?
    """, well)
    prod = cursor.fetchone()
    if prod[0] == 0:
        # Also try Composite Name mapping
        cursor.execute("""
            SELECT [Composite Name] FROM PCE_WM WHERE [Well Name] = ?
        """, well)
        comp = cursor.fetchone()
        comp_name = comp[0].strip() if comp and comp[0] else None
        if comp_name:
            cursor.execute("""
                SELECT COUNT(*), MIN([Date]), MAX([Date])
                FROM PCE_Production WHERE [Well Name] = ?
            """, comp_name)
            prod2 = cursor.fetchone()
            if prod2[0] > 0:
                print(f"    Found under Composite Name '{comp_name}': {prod2[0]} rows  |  {prod2[1]} to {prod2[2]}")
            else:
                print(f"    NO rows (also checked Composite Name '{comp_name}').")
        else:
            print(f"    NO rows in PCE_Production.")
    else:
        print(f"    {prod[0]} rows  |  {prod[1]} to {prod[2]}")

    conn.close()

    # ------------------------------------------------------------------
    # 4. Check Snowflake for this GasIDREC / PressuresIDREC
    # ------------------------------------------------------------------
    if not gas_idrec:
        print(f"\n[4] Skipping Snowflake check — no GasIDREC.\n")
        return

    print(f"\n[4] Checking Snowflake for GasIDREC={gas_idrec} ...")
    sf = SnowflakeConnector()
    try:
        # GasWH production (the primary data source)
        df = sf.query("""
            SELECT
                IDRECPARENT AS GasIDREC,
                CAST(DTTM AS DATE) AS ProdDate,
                VOLENTERGAS AS GasWH_Production
            FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitMeterOrificeEntry
            WHERE IDRECPARENT = %s
            ORDER BY DTTM DESC
            LIMIT 10
        """, (gas_idrec,))

        if df.empty:
            print(f"    NO rows in pvUnitMeterOrificeEntry for GasIDREC={gas_idrec}")
            print(f"    → The well exists in PCE_WM but Snowflake has no production data for it.")
        else:
            print(f"    Found {len(df)} recent rows (showing up to 10):")
            print(df.to_string(index=False))

        if pres_idrec:
            print(f"\n    Checking allocations for PressuresIDREC={pres_idrec} ...")
            df2 = sf.query("""
                SELECT
                    IDRECCOMP AS PressuresIDREC,
                    CAST(DTTM AS DATE) AS ProdDate,
                    VOLPRODGATHGAS AS Gathered_Gas
                FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvunitallocmonthday
                WHERE IDRECCOMP = %s
                ORDER BY DTTM DESC
                LIMIT 5
            """, (pres_idrec,))
            if df2.empty:
                print(f"    NO allocation rows for PressuresIDREC={pres_idrec}")
            else:
                print(f"    Found {len(df2)} recent allocation rows:")
                print(df2.to_string(index=False))
    finally:
        sf.close()

    print(f"\n{'=' * 64}")
    print("  Diagnosis complete.")
    print(f"{'=' * 64}\n")


if __name__ == "__main__":
    main()
