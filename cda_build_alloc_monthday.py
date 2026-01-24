# cda_build_alloc_monthday.py
import pandas as pd

def build_alloc_monthday_slice(sf, pressures_id: str, start_date: str, end_date_excl: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            IDRECCOMP           AS PressuresIDREC,
            DTTM               AS ProdDateTime,
            VOLPRODGATHGAS     AS Gath_Gas_Production,
            VOLNEWPRODALLOCCOND AS New_Prod_Cond_Sales_Production,
            VOLNEWPRODALLOCNGL AS NGL
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitAllocMonthDay
        WHERE
            IDRECCOMP = '{pressures_id}'
            AND DTTM >= '{start_date}'
            AND DTTM <  '{end_date_excl}'
            AND (DELETED = 0 OR DELETED IS NULL)
    """

    df = sf.query(sql)
    if df.empty:
        return df

    # normalize (Snowflake connector can return uppercase headers)
    df.columns = [c.strip() for c in df.columns]
    rename_map = {c: c.title().replace("_", "") for c in df.columns}  # not used, just safety
    # hard rename if uppercase came back
    upper_map = {
        "PRESSURESIDREC": "PressuresIDREC",
        "PRODDATETIME": "ProdDateTime",
        "GATH_GAS_PRODUCTION": "Gath_Gas_Production",
        "NEW_PROD_COND_SALES_PRODUCTION": "New_Prod_Cond_Sales_Production",
        "NGL": "NGL",
    }
    df = df.rename(columns={k: v for k, v in upper_map.items() if k in df.columns})

    df["PressuresIDREC"] = df["PressuresIDREC"].astype(str)
    df["ProdDateTime"] = pd.to_datetime(df["ProdDateTime"], errors="coerce")
    return df.dropna(subset=["ProdDateTime"])
