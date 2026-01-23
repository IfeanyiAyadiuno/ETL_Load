# cda_build_step2.py
import pandas as pd

def build_gaswh_hours_slice(sf, gas_id: str, start_date: str, end_date_excl: str) -> pd.DataFrame:
    """
    Pull GasWH_Production (VOLENTERGAS) + OnProdHours (DURONOR)
    from pvUnitMeterOrificeEntry for one GasIDREC and a date window.

    Returns columns:
      GasIDREC, ProdDateTime, GasWH_Production, OnProdHours
    """
    sql = f"""
        SELECT
            IDRECPARENT AS GasIDREC,
            DTTM        AS ProdDateTime,
            VOLENTERGAS AS GasWH_Production,
            DURONOR     AS OnProdHours
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitMeterOrificeEntry
        WHERE IDRECPARENT = '{gas_id}'
          AND DTTM >= '{start_date}'
          AND DTTM <  '{end_date_excl}'
          AND (DELETED = 0 OR DELETED IS NULL)
          AND (VOLENTERGAS IS NOT NULL OR DURONOR IS NOT NULL)
        ORDER BY DTTM;
    """.strip()

    df = sf.query(sql)

    # Snowflake connector often returns uppercase col names
    df.columns = [c.strip() for c in df.columns]
    rename_map = {}
    for c in df.columns:
        if c.upper() == "GASIDREC": rename_map[c] = "GasIDREC"
        if c.upper() == "PRODDATETIME": rename_map[c] = "ProdDateTime"
        if c.upper() == "GASWH_PRODUCTION": rename_map[c] = "GasWH_Production"
        if c.upper() == "ONPRODHOURS": rename_map[c] = "OnProdHours"
    df = df.rename(columns=rename_map)

    # enforce dtypes
    df["GasIDREC"] = df["GasIDREC"].astype(str)
    df["ProdDateTime"] = pd.to_datetime(df["ProdDateTime"], errors="coerce")

    # if Snowflake returns timestamps, keep only the date part (midnight)
    df["ProdDateTime"] = df["ProdDateTime"].dt.normalize()

    return df

