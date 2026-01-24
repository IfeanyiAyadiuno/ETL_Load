import pandas as pd

def build_ecf_slice(sf, gas_id: str, start_date: str, end_date_excl: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            IDRECPARENT      AS GasIDREC,
            DTTM             AS ProdDateTime,
            EFFLUENTFACTOR   AS ECF_Ratio
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitMeterOrificeEcf
        WHERE
            IDRECPARENT = '{gas_id}'
            AND DTTM >= '{start_date}'
            AND DTTM <  '{end_date_excl}'
            AND EFFLUENTFACTOR IS NOT NULL
        ORDER BY DTTM
    """

    df = sf.query(sql)

    if df is None or df.empty:
        return pd.DataFrame(columns=["GASIDREC", "PRODDATETIME", "ECF_RATIO"])

    # normalize once
    df.columns = [c.upper() for c in df.columns]

    df["GASIDREC"] = df["GASIDREC"].astype(str)
    df["PRODDATETIME"] = pd.to_datetime(df["PRODDATETIME"], errors="coerce")

    df = df.dropna(subset=["PRODDATETIME", "ECF_RATIO"])

    return df[["GASIDREC", "PRODDATETIME", "ECF_RATIO"]]

