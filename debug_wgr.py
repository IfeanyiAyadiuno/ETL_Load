# debug_wgr.py
from snowflake_connector import SnowflakeConnector

PRESSURES_ID = "8C9C1F196A85420A85B0B87E1B3E72BA"
START = "2024-10-13"
END_EXCL = "2024-10-16"

def main():
    sf = SnowflakeConnector()

    # 1) How many WGR rows exist for this pressure id (all time)?
    q1 = f"""
    SELECT
      COUNT(*) AS total_rows,
      COUNT(WGR) AS non_null_wgr,
      MIN(DTTM) AS min_dttm,
      MAX(DTTM) AS max_dttm
    FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitCompRatios
    WHERE IDRECPARENT = '{PRESSURES_ID}'
      AND (DELETED = 0 OR DELETED IS NULL);
    """
    print("\n=== WGR SUMMARY (ALL TIME) ===")
    print(sf.query(q1))

    # 2) Do we have ANY rows in your window (even if WGR is null)?
    q2 = f"""
    SELECT
      COUNT(*) AS rows_in_window,
      COUNT(WGR) AS non_null_wgr_in_window,
      MIN(DTTM) AS min_dttm_in_window,
      MAX(DTTM) AS max_dttm_in_window
    FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitCompRatios
    WHERE IDRECPARENT = '{PRESSURES_ID}'
      AND DTTM >= '{START}'
      AND DTTM <  '{END_EXCL}'
      AND (DELETED = 0 OR DELETED IS NULL);
    """
    print("\n=== WINDOW CHECK ===")
    print(sf.query(q2))

    # 3) Show latest 10 rows for this pressure id (so we can see what dates exist)
    q3 = f"""
    SELECT IDRECPARENT, DTTM, WGR, CGR
    FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitCompRatios
    WHERE IDRECPARENT = '{PRESSURES_ID}'
      AND (DELETED = 0 OR DELETED IS NULL)
    ORDER BY DTTM DESC
    LIMIT 10;
    """
    print("\n=== LATEST 10 ROWS FOR THIS PRESSURE ID ===")
    print(sf.query(q3))

    sf.close()

if __name__ == "__main__":
    main()
