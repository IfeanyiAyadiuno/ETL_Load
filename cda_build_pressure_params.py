# cda_build_pressure_params.py
import pandas as pd

def build_pressure_params_slice(sf, pressures_id: str, start_date: str, end_date_excl: str) -> pd.DataFrame:
    """
    Pull TubingPressure (PRESTUB), CasingPressure (PRESCAS), ChokeSize (SZCHOKE)
    from pvUnitCompParam for one PressuresIDREC (IDRECPARENT) and a date window.

    Returns columns:
      PressuresIDREC, ProdDateTime, TubingPressure, CasingPressure, ChokeSize
    """
    sql = f"""
        SELECT
            IDRECPARENT AS PressuresIDREC,
            DTTM        AS ProdDateTime,
            PRESTUB     AS TubingPressure,
            PRESCAS     AS CasingPressure,
            SZCHOKE     AS ChokeSize
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitCompParam
        WHERE IDRECPARENT = '{pressures_id}'
          AND DTTM >= '{start_date}'
          AND DTTM <  '{end_date_excl}'
          AND (DELETED = 0 OR DELETED IS NULL)
          AND (PRESTUB IS NOT NULL OR PRESCAS IS NOT NULL OR SZCHOKE IS NOT NULL)
        ORDER BY DTTM;
    """.strip()

    df = sf.query(sql)

    # normalize / rename columns
    df.columns = [c.strip() for c in df.columns]
    rename_map = {}
    for c in df.columns:
        if c.upper() == "PRESSURESIDREC": rename_map[c] = "PressuresIDREC"
        if c.upper() == "PRODDATETIME": rename_map[c] = "ProdDateTime"
        if c.upper() == "TUBINGPRESSURE": rename_map[c] = "TubingPressure"
        if c.upper() == "CASINGPRESSURE": rename_map[c] = "CasingPressure"
        if c.upper() == "CHOKESIZE": rename_map[c] = "ChokeSize"
    df = df.rename(columns=rename_map)

    df["PressuresIDREC"] = df["PressuresIDREC"].astype(str)
    df["ProdDateTime"] = pd.to_datetime(df["ProdDateTime"], errors="coerce")
    df["ProdDateTime"] = df["ProdDateTime"].dt.normalize()

    return df
