# cda_build_step4_ecf.py
import pandas as pd

def build_ecf_slice(sf, gas_id: str, start_date: str, end_date_excl: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            IDRECPARENT      AS GasIDREC,
            DTTM            AS ProdDateTime,
            EFFLUENTFACTOR  AS ECF_Ratio
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitMeterOrificeEcf
        WHERE IDRECPARENT = '{gas_id}'
          AND DTTM >= '{start_date}'
          AND DTTM <  '{end_date_excl}'
          AND (DELETED = 0 OR DELETED IS NULL)
          AND EFFLUENTFACTOR IS NOT NULL
        ORDER BY DTTM;
    """

    df = sf.query(sql)
    df.columns = [c.strip() for c in df.columns]

    rename = {}
    for c in df.columns:
        if c.upper() == "GASIDREC": rename[c] = "GasIDREC"
        if c.upper() == "PRODDATETIME": rename[c] = "ProdDateTime"
        if c.upper() == "ECF_RATIO": rename[c] = "ECF_Ratio"
    df = df.rename(columns=rename)

    if "ProdDateTime" in df.columns:
        df["ProdDateTime"] = pd.to_datetime(df["ProdDateTime"], errors="coerce")
    if "ECF_Ratio" in df.columns:
        df["ECF_Ratio"] = pd.to_numeric(df["ECF_Ratio"], errors="coerce")

    df = df.dropna(subset=["GasIDREC", "ProdDateTime", "ECF_Ratio"])
    return df[["GasIDREC", "ProdDateTime", "ECF_Ratio"]]
