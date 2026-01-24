# cda_build_cgr.py
import pandas as pd

def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Make column access case-insensitive by mapping all columns to lower-case."""
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    return df

def build_cgr_slice(sf, pressures_id: str, start_date: str, end_date_excl: str) -> pd.DataFrame:
    """
    CGR_Ratio = RATEHCLIQ / RATEGAS
    Source: pvUnitCompGathMonthDayCalc
    Key: IDRECCOMP == PressuresIDREC
    """
    sql = f"""
        SELECT
            IDRECCOMP AS PressuresIDREC,
            DTTM      AS ProdDateTime,
            CASE
                WHEN RATEGAS IS NULL OR RATEGAS = 0 THEN NULL
                ELSE (RATEHCLIQ / RATEGAS)
            END AS CGR_Ratio
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitCompGathMonthDayCalc
        WHERE
            IDRECCOMP = '{pressures_id}'
            AND DTTM >= '{start_date}'
            AND DTTM <  '{end_date_excl}'
            AND (DELETED = 0 OR DELETED IS NULL)
        ORDER BY DTTM
    """

    df = sf.query(sql)
    if df is None or df.empty:
        return df

    # normalize to lower-case so caps never break us
    df = _normalize_cols(df)

    # Rename to your standard names
    # (after normalization, these keys are guaranteed if SELECT aliases worked)
    df = df.rename(columns={
        "pressuresidrec": "PressuresIDREC",
        "proddatetime": "ProdDateTime",
        "cgr_ratio": "CGR_Ratio",
    })

    # If aliases didn't come through (rare), fall back to raw names
    if "PressuresIDREC" not in df.columns and "idreccomp" in df.columns:
        df["PressuresIDREC"] = df["idreccomp"]
    if "ProdDateTime" not in df.columns and "dttm" in df.columns:
        df["ProdDateTime"] = df["dttm"]

    # Clean types
    df["PressuresIDREC"] = df["PressuresIDREC"].astype(str)
    df["ProdDateTime"] = pd.to_datetime(df["ProdDateTime"], errors="coerce")
    df["CGR_Ratio"] = pd.to_numeric(df["CGR_Ratio"], errors="coerce")

    df = df.dropna(subset=["ProdDateTime", "CGR_Ratio"])

    # return only what updater needs
    return df[["PressuresIDREC", "ProdDateTime", "CGR_Ratio"]]
