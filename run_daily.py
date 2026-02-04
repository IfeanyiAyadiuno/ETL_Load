
import pandas as pd
from snowflake_connector import SnowflakeConnector
from dotenv import load_dotenv
import os
import pyodbc

from access_loader import ensure_cda_table, delete_cda_range, insert_cda_rows


load_dotenv()

ACCESS_PATH = os.getenv("ACCESS_PATH")
ACCESS_DRIVER = os.getenv("ACCESS_DRIVER", "{Microsoft Access Driver (*.mdb, *.accdb)}")

def get_access_conn():
    conn_str = f'DRIVER={ACCESS_DRIVER};DBQ={ACCESS_PATH};'
    return pyodbc.connect(conn_str)

def pull_mapping():
    sql = """
    SELECT
        GasIDREC,
        PressuresIDREC,
        [Well Name]
    FROM PCE_WM
    WHERE GasIDREC IS NOT NULL
    """
    with get_access_conn() as cn:
        df = pd.read_sql(sql, cn)

    df["GasIDREC"] = df["GasIDREC"].astype(str).str.strip()
    df["PressuresIDREC"] = df["PressuresIDREC"].astype(str).str.strip()
    df["Well Name"] = df["Well Name"].astype(str).str.strip()
    df = df.drop_duplicates(subset=["GasIDREC"])
    return df

def build_spine(mapping: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    days = pd.date_range(start=start, end=end, freq="D").date
    days_df = pd.DataFrame({"ProdDate": days})

    wells = mapping[["GasIDREC", "PressuresIDREC", "Well Name"]].copy()
    wells["k"] = 1
    days_df["k"] = 1
    return wells.merge(days_df, on="k").drop(columns=["k"])

def pull_ecf(start: str, end: str) -> pd.DataFrame:
    sf = SnowflakeConnector()

    sql = f"""
    SELECT
        IDRECPARENT AS GasIDREC,
        CAST (DTTM AS DATE) AS ProdDate,
        EFFLUENTFACTOR AS ECF_Ratio
    FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitMeterOrificeEcf
    WHERE DTTM >= '{start}'
      AND DTTM < DATEADD(day, 1, '{end}')
    """

    df = sf.query(sql)
    sf.close()

    # normalize types
    df["GasIDREC"] = df["GASIDREC"].astype(str).str.strip() if "GASIDREC" in df.columns else df["GasIDREC"].astype(str).str.strip()
    df["ProdDate"] = pd.to_datetime(df["PRODDATE"] if "PRODDATE" in df.columns else df["ProdDate"]).dt.date
    df["ECF_Ratio"] = pd.to_numeric(df["ECF_RATIO"] if "ECF_RATIO" in df.columns else df["ECF_Ratio"], errors="coerce")

    # keep only expected column names (handle Snowflake uppercasing)
    out = pd.DataFrame({
        "GasIDREC": df["GasIDREC"],
        "ProdDate": df["ProdDate"],
        "ECF_Ratio": df["ECF_Ratio"],
    })

    # if multiple rows per GasIDREC/day, take the last non-null (simple for now)
    out = (
        out.sort_values(["GasIDREC", "ProdDate"])
           .groupby(["GasIDREC", "ProdDate"], as_index=False)
           .agg({"ECF_Ratio": "last"})
    )
    return out

def pull_gaswh(start: str, end: str) -> pd.DataFrame:
    sf = SnowflakeConnector()

    sql = f"""
    SELECT
        IDRECPARENT AS GasIDREC,
        CAST(DTTM AS DATE) AS ProdDate,
        VOLENTERGAS AS GasWH_Production,
        DURONOR AS OnProdHours
    FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitMeterOrificeEntry
    WHERE DTTM >= '{start}'
      AND DTTM < DATEADD(day, 1, '{end}')
    """

    df = sf.query(sql)
    sf.close()

    # normalize column names if Snowflake uppercases
    cols = {c.upper(): c for c in df.columns}

    gas_col   = cols.get("GASIDREC", "GasIDREC")
    date_col  = cols.get("PRODDATE", "ProdDate")
    gaswh_col = cols.get("GASWH_PRODUCTION", "GasWH_Production")
    hrs_col   = cols.get("ONPRODHOURS", "OnProdHours")

    out = pd.DataFrame({
        "GasIDREC": df[gas_col].astype(str).str.strip(),
        "ProdDate": pd.to_datetime(df[date_col]).dt.date,
        "GasWH_Production": pd.to_numeric(df[gaswh_col], errors="coerce"),
        "OnProdHours": pd.to_numeric(df[hrs_col], errors="coerce"),
    })

    # ensure one row per well per day
    out = (
        out.sort_values(["GasIDREC", "ProdDate"])
           .groupby(["GasIDREC", "ProdDate"], as_index=False)
           .agg({
               "GasWH_Production": "last",
               "OnProdHours": "last",
           })
    )

    return out


def pull_cgr(start: str, end: str) -> pd.DataFrame:
    sf = SnowflakeConnector()

    sql = f"""
    SELECT
        IDRECCOMP AS PressuresIDREC,
        CAST(DTTM AS DATE) AS ProdDate,
        CASE
            WHEN RATEGAS IS NULL OR RATEGAS = 0 THEN NULL
            ELSE (RATEHCLIQ / RATEGAS)
        END AS CGR_Ratio
    FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitCompGathMonthDayCalc
    WHERE DTTM >= '{start}'
      AND DTTM < DATEADD(day, 1, '{end}')
    """

    df = sf.query(sql)
    sf.close()

    cols = {c.upper(): c for c in df.columns}

    pid_col  = cols.get("PRESSURESIDREC", "PressuresIDREC")
    date_col = cols.get("PRODDATE", "ProdDate")
    val_col  = cols.get("CGR_RATIO", "CGR_Ratio")

    out = pd.DataFrame({
        "PressuresIDREC": df[pid_col].astype(str).str.strip(),
        "ProdDate": pd.to_datetime(df[date_col]).dt.date,
        "CGR_Ratio": pd.to_numeric(df[val_col], errors="coerce")
    })

    out = (
        out.sort_values(["PressuresIDREC", "ProdDate"])
           .groupby(["PressuresIDREC", "ProdDate"], as_index=False)
           .agg({"CGR_Ratio": "last"})
    )

    return out

def pull_wgr(start: str, end: str) -> pd.DataFrame:
    sf = SnowflakeConnector()

    sql = f"""
    SELECT
        IDRECPARENT AS PressuresIDREC,
        CAST(DTTM AS DATE) AS ProdDate,
        WGR AS WGR_Ratio
    FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitCompRatios
    WHERE DTTM >= '{start}'
      AND DTTM < DATEADD(day, 1, '{end}')
    """

    df = sf.query(sql)
    sf.close()

    cols = {c.upper(): c for c in df.columns}

    pid_col  = cols.get("PRESSURESIDREC", "PressuresIDREC")
    date_col = cols.get("PRODDATE", "ProdDate")
    val_col  = cols.get("WGR_RATIO", "WGR_Ratio")

    out = pd.DataFrame({
        "PressuresIDREC": df[pid_col].astype(str).str.strip(),
        "ProdDate": pd.to_datetime(df[date_col]).dt.date,
        "WGR_Ratio": pd.to_numeric(df[val_col], errors="coerce")
    })

    out = (
        out.sort_values(["PressuresIDREC", "ProdDate"])
           .groupby(["PressuresIDREC", "ProdDate"], as_index=False)
           .agg({"WGR_Ratio": "last"})
    )

    return out

def pull_pressures(start: str, end: str) -> pd.DataFrame:
    sf = SnowflakeConnector()

    sql = f"""
    SELECT
        IDRECPARENT AS PressuresIDREC,
        CAST(DTTM AS DATE) AS ProdDate,
        PRESTUB AS TubingPressure,
        PRESCAS AS CasingPressure,
        SZCHOKE AS ChokeSize
    FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitCompParam
    WHERE DTTM >= '{start}'
      AND DTTM < DATEADD(day, 1, '{end}')
    """

    df = sf.query(sql)
    sf.close()

    cols = {c.upper(): c for c in df.columns}

    pid_col   = cols.get("PRESSURESIDREC", "PressuresIDREC")
    date_col  = cols.get("PRODDATE", "ProdDate")
    tub_col   = cols.get("TUBINGPRESSURE", "TubingPressure")
    cas_col   = cols.get("CASINGPRESSURE", "CasingPressure")
    choke_col = cols.get("CHOKESIZE", "ChokeSize")

    out = pd.DataFrame({
        "PressuresIDREC": df[pid_col].astype(str).str.strip(),
        "ProdDate": pd.to_datetime(df[date_col]).dt.date,
        "TubingPressure": pd.to_numeric(df[tub_col], errors="coerce"),
        "CasingPressure": pd.to_numeric(df[cas_col], errors="coerce"),
        "ChokeSize": pd.to_numeric(df[choke_col], errors="coerce"),
    })

    out = (
        out.sort_values(["PressuresIDREC", "ProdDate"])
           .groupby(["PressuresIDREC", "ProdDate"], as_index=False)
           .agg({
               "TubingPressure": "last",
               "CasingPressure": "last",
               "ChokeSize": "last",
           })
    )

    return out

def pull_allocations(start: str, end: str) -> pd.DataFrame:
    sf = SnowflakeConnector()

    sql = f"""
    SELECT
        IDRECCOMP AS PressuresIDREC,
        CAST(DTTM AS DATE) AS ProdDate,
        VOLPRODGATHGAS AS GatheredGas_Production,
        VOLNEWPRODALLOCCOND AS NewProdCondSales_Production,
        VOLNEWPRODALLOCNGL AS NGL_Production
    FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvunitallocmonthday
    WHERE DTTM >= '{start}'
      AND DTTM < DATEADD(day, 1, '{end}')
    """

    df = sf.query(sql)
    sf.close()

    cols = {c.upper(): c for c in df.columns}

    pid_col   = cols.get("PRESSURESIDREC", "PressuresIDREC")
    date_col  = cols.get("PRODDATE", "ProdDate")
    gas_col   = cols.get("GATHEREDGAS_PRODUCTION", "GatheredGas_Production")
    cond_col  = cols.get("NEWPRODCONDSALES_PRODUCTION", "NewProdCondSales_Production")
    ngl_col   = cols.get("NGL_PRODUCTION", "NGL_Production")

    out = pd.DataFrame({
        "PressuresIDREC": df[pid_col].astype(str).str.strip(),
        "ProdDate": pd.to_datetime(df[date_col]).dt.date,
        "GatheredGas_Production": pd.to_numeric(df[gas_col], errors="coerce"),
        "NewProdCondSales_Production": pd.to_numeric(df[cond_col], errors="coerce"),
        "NGL_Production": pd.to_numeric(df[ngl_col], errors="coerce"),
    })

    out = (
        out.sort_values(["PressuresIDREC", "ProdDate"])
           .groupby(["PressuresIDREC", "ProdDate"], as_index=False)
           .agg({
               "GatheredGas_Production": "last",
               "NewProdCondSales_Production": "last",
               "NGL_Production": "last",
           })
    )

    return out

def pull_alloc_water(start: str, end: str) -> pd.DataFrame:
    sf = SnowflakeConnector()

    sql = f"""
    SELECT
        IDRECCOMP AS PressuresIDREC,
        CAST(DTTM AS DATE) AS ProdDate,
        VOLWATER AS AllocatedWater_Rate
    FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvunitcompgathmonthdaycalc
    WHERE DTTM >= '{start}'
      AND DTTM < DATEADD(day, 1, '{end}')
    """

    df = sf.query(sql)
    sf.close()

    cols = {c.upper(): c for c in df.columns}

    pid_col  = cols.get("PRESSURESIDREC", "PressuresIDREC")
    date_col = cols.get("PRODDATE", "ProdDate")
    val_col  = cols.get("ALLOCATEDWATER_RATE", "AllocatedWater_Rate")

    out = pd.DataFrame({
        "PressuresIDREC": df[pid_col].astype(str).str.strip(),
        "ProdDate": pd.to_datetime(df[date_col]).dt.date,
        "AllocatedWater_Rate": pd.to_numeric(df[val_col], errors="coerce")
    })

    out = (
        out.sort_values(["PressuresIDREC", "ProdDate"])
           .groupby(["PressuresIDREC", "ProdDate"], as_index=False)
           .agg({"AllocatedWater_Rate": "last"})
    )

    return out


if __name__ == "__main__":
    start = "2017-04-01"
    end = "2017-04-30"

    mapping = pull_mapping()
    spine = build_spine(mapping, start, end)

    ecf = pull_ecf(start, end)
    gaswh = pull_gaswh(start, end)
    cgr = pull_cgr(start, end)
    wgr = pull_wgr(start, end)
    pressures = pull_pressures(start, end)
    alloc = pull_allocations(start, end)
    alloc_water = pull_alloc_water(start, end)



    print("Spine rows:", len(spine))


    joined = spine.merge(ecf, on=["GasIDREC", "ProdDate"], how="left")
    joined = joined.merge(gaswh, on=["GasIDREC", "ProdDate"], how="left")
    joined = joined.merge(cgr, on=["PressuresIDREC", "ProdDate"], how="left")
    joined = joined.merge(wgr, on=["PressuresIDREC", "ProdDate"], how="left")
    joined = joined.merge(pressures, on=["PressuresIDREC", "ProdDate"], how="left")

    joined["Condensate_WH_Production"] = (
    joined["GasWH_Production"] * joined["CGR_Ratio"]
)

    joined = joined.merge(alloc, on=["PressuresIDREC", "ProdDate"], how="left")
    joined = joined.merge(alloc_water, on=["PressuresIDREC", "ProdDate"], how="left")

    print("\n--- ABOUT TO LOAD INTO ACCESS ---")
    print("Final dataframe rows:", len(joined))
    print("Final dataframe cols:", len(joined.columns))

    print("\nDF columns:")
    for c in joined.columns:
        print(c)

    ensure_cda_table()
    delete_cda_range(start, end)
    insert_cda_rows(joined)

    
    
    print("\nGasWH rows:", len(gaswh))
    print("Null GasWH count:", joined["GasWH_Production"].isna().sum())

    print("\nJoined sample:")
    print(joined.head(10).to_string(index=False))

    print("\nNull ECF count:", joined["ECF_Ratio"].isna().sum())

