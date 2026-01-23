# cda_build_step1.py
from __future__ import annotations
import pandas as pd


def build_gaswh_slice(sf, gas_id: str, start_date: str, end_date_excl: str) -> pd.DataFrame:
    """
    Pull GasWH_Production (VOLENTERGAS) for one GasIDREC over a date range.

    - gas_id maps to pvUnitMeterOrificeEntry.IDRECPARENT
    - ProdDateTime maps to pvUnitMeterOrificeEntry.DTTM
    - GasWH_Production maps to pvUnitMeterOrificeEntry.VOLENTERGAS
    """

    sql = f"""
        SELECT
            IDRECPARENT AS GasIDREC,
            DTTM        AS ProdDateTime,
            VOLENTERGAS AS GasWH_Production
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitMeterOrificeEntry
        WHERE IDRECPARENT = '{gas_id}'
          AND DTTM >= '{start_date}'
          AND DTTM <  '{end_date_excl}'
          AND (DELETED = 0 OR DELETED IS NULL)
          AND VOLENTERGAS IS NOT NULL
        ORDER BY DTTM
    """

    df = sf.query(sql)

    if df is None or df.empty:
        print("[build_gaswh_slice] No rows returned.")
        return pd.DataFrame(columns=["GasIDREC", "ProdDateTime", "GasWH_Production"])

    # ---- Normalize column names (case-insensitive) ----
    df.columns = [str(c).strip() for c in df.columns]
    colmap_lower = {c.lower(): c for c in df.columns}  # lower -> actual

    required_lower = {"gasidrec", "proddatetime", "gaswh_production"}
    missing = required_lower - set(colmap_lower.keys())
    if missing:
        print("[build_gaswh_slice] Columns returned:", list(df.columns))
        raise KeyError(f"Missing expected columns from Snowflake result: {missing}")

    # Rename from whatever casing Snowflake gave -> our standard names
    df = df.rename(columns={
        colmap_lower["gasidrec"]: "GasIDREC",
        colmap_lower["proddatetime"]: "ProdDateTime",
        colmap_lower["gaswh_production"]: "GasWH_Production",
    })

    # ---- Type cleanup for merges/inserts later ----
    df["GasIDREC"] = df["GasIDREC"].astype(str)
    df["ProdDateTime"] = pd.to_datetime(df["ProdDateTime"], errors="coerce")
    df["GasWH_Production"] = pd.to_numeric(df["GasWH_Production"], errors="coerce")

    df = df.dropna(subset=["GasIDREC", "ProdDateTime"])

    return df[["GasIDREC", "ProdDateTime", "GasWH_Production"]]
