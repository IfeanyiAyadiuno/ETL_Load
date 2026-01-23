# cda_build_wgr.py
import pandas as pd

def build_wgr_slice(sf, pressures_id: str, start_date: str, end_date_excl: str) -> pd.DataFrame:
    """
    Pull WGR from pvUnitCompRatios using PressuresIDREC (IDRECPARENT) + date window.

    Returns:
      PressuresIDREC, ProdDateTime, WGR_Ratio
    """
    sql = f"""
        SELECT
            IDRECPARENT AS PressuresIDREC,
            DTTM        AS ProdDateTime,
            WGR         AS WGR_Ratio
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitCompRatios
        WHERE IDRECPARENT = '{pressures_id}'
          AND DTTM >= '{start_date}'
          AND DTTM <  '{end_date_excl}'
          AND (DELETED = 0 OR DELETED IS NULL)
          AND WGR IS NOT NULL
        ORDER BY DTTM;
    """.strip()

    df = sf.query(sql)

    # normalize column names (Snowflake can uppercase)
    df.columns = [c.strip() for c in df.columns]
    rename_map = {}
    for c in df.columns:
        if c.upper() == "PRESSURESIDREC": rename_map[c] = "PressuresIDREC"
        if c.upper() == "PRODDATETIME": rename_map[c] = "ProdDateTime"
        if c.upper() == "WGR_RATIO": rename_map[c] = "WGR_Ratio"
    df = df.rename(columns=rename_map)

    df["PressuresIDREC"] = df["PressuresIDREC"].astype(str).str.strip()
    df["ProdDateTime"] = pd.to_datetime(df["ProdDateTime"], errors="coerce")
    df = df.dropna(subset=["ProdDateTime"])

    # normalize to midnight to match your CDA rows (00:00:00)
    df["ProdDateTime"] = df["ProdDateTime"].dt.normalize()

    return df
