'''

import pandas as pd
from snowflake_connector import SnowflakeConnector
import numpy as np  
from dotenv import load_dotenv
import os
import pyodbc
import warnings
from datetime import datetime
warnings.filterwarnings('ignore', category=FutureWarning)

load_dotenv()

# SQL Server connection settings
SQL_SERVER = os.getenv("SQL_SERVER", "CALVMSQL02")
SQL_DATABASE = os.getenv("SQL_DATABASE", "Re_Main_Production")
SQL_DRIVER = os.getenv("SQL_DRIVER", "{ODBC Driver 17 for SQL Server}")

def get_sql_conn():
    """Create connection to SQL Server"""
    conn_str = (
        f'DRIVER={SQL_DRIVER};'
        f'SERVER={SQL_SERVER};'
        f'DATABASE={SQL_DATABASE};'
        f'Trusted_Connection=yes;'
    )
    return pyodbc.connect(conn_str)

def ensure_pce_cda_table():
    """Check if PCE_CDA table exists"""
    print("  Verifying PCE_CDA table exists in SQL Server...")
    with get_sql_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'PCE_CDA'
        """)
        if cursor.fetchone()[0] == 0:
            print("  ⚠️ PCE_CDA table not found! Please create it first.")
            return False
        print("  ✅ PCE_CDA table exists")
        return True

def delete_pce_cda_range(start_date, end_date):
    """Delete records in date range from SQL Server"""
    print(f"  Deleting records from {start_date} to {end_date}...")
    with get_sql_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM PCE_CDA 
            WHERE ProdDate BETWEEN ? AND ?
        """, start_date, end_date)
        deleted = cursor.rowcount
        conn.commit()
        print(f"  Deleted {deleted:,} records")
        return deleted

def insert_pce_cda_rows(df):
    """
    Insert dataframe into SQL Server PCE_CDA table
    Processes in batches for better performance
    """
    if df.empty:
        print("  No rows to insert")
        return 0
    
    print(f"  Inserting {len(df):,} rows into SQL Server...")
    
    # CLEAN THE DATA - Replace NaN/Inf with None (SQL NULL)
    df_clean = df.copy()
    
    # List of float columns that might have issues
    float_cols = [
        'GasWH_Production', 'Condensate_WH_Production',
        'WGR_Ratio', 'CGR_Ratio', 'ECF_Ratio',
        'OnProdHours', 'TubingPressure', 'CasingPressure', 'ChokeSize',
        'Gathered_Gas_Production', 'Gathered_Condensate_Production',
        'NGL_Production', 'AllocatedWater_Rate', 'Lateral Length'
    ]
    
    # Clean each float column
    for col in float_cols:
        if col in df_clean.columns:
            # Replace NaN, Inf, -Inf with None
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
            df_clean[col] = df_clean[col].replace([np.inf, -np.inf], np.nan)
    
    # Define the insert SQL with the new columns
    insert_sql = """
    INSERT INTO PCE_CDA (
        [GasIDREC], [PressuresIDREC], [Well Name], [ProdDate],
        [GasWH_Production], [Condensate_WH_Production],
        [WGR_Ratio], [CGR_Ratio], [ECF_Ratio],
        [OnProdHours], [TubingPressure], [CasingPressure], [ChokeSize],
        [Gathered_Gas_Production], [Gathered_Condensate_Production],
        [NGL_Production], [AllocatedWater_Rate],
        [Formation Producer], [Layer Producer], [Fault Block], [Pad Name],
        [Lateral Length], [Orient]
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    # Convert dataframe to list of tuples, replacing NaN with None
    rows_to_insert = []
    for _, row in df_clean.iterrows():
        rows_to_insert.append((
            row.get('GasIDREC', None),
            row.get('PressuresIDREC', None),
            row.get('Well Name', None),
            row.get('ProdDate', None),
            None if pd.isna(row.get('GasWH_Production')) else float(row.get('GasWH_Production')),
            None if pd.isna(row.get('Condensate_WH_Production')) else float(row.get('Condensate_WH_Production')),
            None if pd.isna(row.get('WGR_Ratio')) else float(row.get('WGR_Ratio')),
            None if pd.isna(row.get('CGR_Ratio')) else float(row.get('CGR_Ratio')),
            None if pd.isna(row.get('ECF_Ratio')) else float(row.get('ECF_Ratio')),
            None if pd.isna(row.get('OnProdHours')) else float(row.get('OnProdHours')),
            None if pd.isna(row.get('TubingPressure')) else float(row.get('TubingPressure')),
            None if pd.isna(row.get('CasingPressure')) else float(row.get('CasingPressure')),
            None if pd.isna(row.get('ChokeSize')) else float(row.get('ChokeSize')),
            None if pd.isna(row.get('Gathered_Gas_Production')) else float(row.get('Gathered_Gas_Production')),
            None if pd.isna(row.get('Gathered_Condensate_Production')) else float(row.get('Gathered_Condensate_Production')),
            None if pd.isna(row.get('NGL_Production')) else float(row.get('NGL_Production')),
            None if pd.isna(row.get('AllocatedWater_Rate')) else float(row.get('AllocatedWater_Rate')),
            # New columns from PCE_WM
            row.get('Formation Producer', None),
            row.get('Layer Producer', None),
            row.get('Fault Block', None),
            row.get('Pad Name', None),
            None if pd.isna(row.get('Lateral Length')) else float(row.get('Lateral Length')),
            row.get('Orient', None)
        ))
    
    # Insert in batches
    batch_size = 1000
    total_inserted = 0
    
    with get_sql_conn() as conn:
        cursor = conn.cursor()
        
        for i in range(0, len(rows_to_insert), batch_size):
            batch = rows_to_insert[i:i + batch_size]
            try:
                cursor.executemany(insert_sql, batch)
                conn.commit()
                total_inserted += len(batch)
            except Exception as e:
                print(f"    ❌ Error on batch starting at row {i}: {e}")
                # Try one row at a time to find the bad row
                for j, row in enumerate(batch):
                    try:
                        cursor.execute(insert_sql, row)
                        conn.commit()
                        total_inserted += 1
                    except Exception as row_e:
                        print(f"      ❌ Bad row at position {i+j}: {row_e}")
                # Continue with next batch
                continue
            
            if (i + batch_size) % 5000 == 0 or (i + batch_size) >= len(rows_to_insert):
                print(f"    Inserted {min(i + batch_size, len(rows_to_insert)):,} rows...")
    
    print(f"  ✅ Successfully inserted {total_inserted:,} rows")
    return total_inserted

def pull_mapping():
    """Pulls mapping from SQL Server PCE_WM including all needed fields"""
    print("Pulling mapping data from SQL Server...")
    sql = """
    SELECT
        GasIDREC,
        PressuresIDREC,
        [Well Name],
        [Formation Producer],
        [Layer Producer],
        [Fault Block],
        [Pad Name],
        [Lateral Length],
        [Orient]
    FROM PCE_WM
    WHERE GasIDREC IS NOT NULL
    """
    with get_sql_conn() as cn:
        df = pd.read_sql(sql, cn)

    df["GasIDREC"] = df["GasIDREC"].astype(str).str.strip()
    df["PressuresIDREC"] = df["PressuresIDREC"].astype(str).str.strip()
    df["Well Name"] = df["Well Name"].astype(str).str.strip()
    df["Formation Producer"] = df["Formation Producer"].astype(str).str.strip()
    df["Layer Producer"] = df["Layer Producer"].astype(str).str.strip()
    df["Fault Block"] = df["Fault Block"].astype(str).str.strip()
    df["Pad Name"] = df["Pad Name"].astype(str).str.strip()
    df["Lateral Length"] = pd.to_numeric(df["Lateral Length"], errors='coerce')
    df["Orient"] = df["Orient"].astype(str).str.strip()
    
    df = df.drop_duplicates(subset=["GasIDREC"])
    print(f"Found {len(df)} unique wells")
    return df

def pull_ecf(start: str, end: str) -> pd.DataFrame:
    print("Pulling ECF data from Snowflake...")
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
    print(f"ECF data pulled: {len(out):,} rows")
    return out

def pull_gaswh(start: str, end: str) -> pd.DataFrame:
    print("Pulling GasWH data from Snowflake...")
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
    print(f"GasWH data pulled: {len(out):,} rows")
    return out

def pull_cgr(start: str, end: str) -> pd.DataFrame:
    print("Pulling CGR data from Snowflake...")
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
    print(f"CGR data pulled: {len(out):,} rows")
    return out

def pull_wgr(start: str, end: str) -> pd.DataFrame:
    print("Pulling WGR data from Snowflake...")
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
    print(f"WGR data pulled: {len(out):,} rows")
    return out

def pull_pressures(start: str, end: str) -> pd.DataFrame:
    print("Pulling Pressures data from Snowflake...")
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
    print(f"Pressures data pulled: {len(out):,} rows")
    return out

def pull_allocations(start: str, end: str) -> pd.DataFrame:
    print("Pulling Allocation data from Snowflake...")
    sf = SnowflakeConnector()

    sql = f"""
    SELECT
        IDRECCOMP AS PressuresIDREC,
        CAST(DTTM AS DATE) AS ProdDate,
        VOLPRODGATHGAS AS Gathered_Gas_Production,
        VOLPRODGATHHCLIQ AS Gathered_Condensate_Production,
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
    gas_col   = cols.get("GATHERED_GAS_PRODUCTION", "Gathered_Gas_Production")
    cond_col  = cols.get("GATHERED_CONDENSATE_PRODUCTION", "Gathered_Condensate_Production")
    ngl_col   = cols.get("NGL_PRODUCTION", "NGL_Production")

    out = pd.DataFrame({
        "PressuresIDREC": df[pid_col].astype(str).str.strip(),
        "ProdDate": pd.to_datetime(df[date_col]).dt.date,
        "Gathered_Gas_Production": pd.to_numeric(df[gas_col], errors="coerce"),
        "Gathered_Condensate_Production": pd.to_numeric(df[cond_col], errors="coerce"),
        "NGL_Production": pd.to_numeric(df[ngl_col], errors="coerce"),
    })

    out = (
        out.sort_values(["PressuresIDREC", "ProdDate"])
           .groupby(["PressuresIDREC", "ProdDate"], as_index=False)
           .agg({
               "Gathered_Gas_Production": "last",
               "Gathered_Condensate_Production": "last",
               "NGL_Production": "last",
           })
    )
    print(f"Allocation data pulled: {len(out):,} rows")
    return out

def pull_alloc_water(start: str, end: str) -> pd.DataFrame:
    print("Pulling Allocated Water data from Snowflake...")
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
    print(f"Allocated Water data pulled: {len(out):,} rows")
    return out

def get_first_production_dates(mapping, ecf, gaswh, cgr, wgr, pressures, alloc, alloc_water):
    """
    For each well, find the earliest date where ANY data exists
    across all source dataframes
    """
    print("\nFinding first production date for each well...")
    
    first_dates = {}
    wells_with_data = 0
    wells_without_data = 0
    
    for _, row in mapping.iterrows():
        well_name = row["Well Name"]
        gas_id = row["GasIDREC"]
        pressures_id = row["PressuresIDREC"]
        
        # Collect all dates from each data source for this well
        all_dates = []
        
        # ECF data
        if gas_id in ecf["GasIDREC"].values:
            well_ecf = ecf[ecf["GasIDREC"] == gas_id]
            all_dates.extend(well_ecf["ProdDate"].tolist())
        
        # GasWH data
        if gas_id in gaswh["GasIDREC"].values:
            well_gaswh = gaswh[gaswh["GasIDREC"] == gas_id]
            all_dates.extend(well_gaswh["ProdDate"].tolist())
        
        # CGR data
        if pressures_id in cgr["PressuresIDREC"].values:
            well_cgr = cgr[cgr["PressuresIDREC"] == pressures_id]
            all_dates.extend(well_cgr["ProdDate"].tolist())
        
        # WGR data
        if pressures_id in wgr["PressuresIDREC"].values:
            well_wgr = wgr[wgr["PressuresIDREC"] == pressures_id]
            all_dates.extend(well_wgr["ProdDate"].tolist())
        
        # Pressures data
        if pressures_id in pressures["PressuresIDREC"].values:
            well_pressures = pressures[pressures["PressuresIDREC"] == pressures_id]
            all_dates.extend(well_pressures["ProdDate"].tolist())
        
        # Allocations data
        if pressures_id in alloc["PressuresIDREC"].values:
            well_alloc = alloc[alloc["PressuresIDREC"] == pressures_id]
            all_dates.extend(well_alloc["ProdDate"].tolist())
        
        # Allocated Water data
        if pressures_id in alloc_water["PressuresIDREC"].values:
            well_alloc_water = alloc_water[alloc_water["PressuresIDREC"] == pressures_id]
            all_dates.extend(well_alloc_water["ProdDate"].tolist())
        
        if all_dates:
            # Find the earliest date
            first_date = min(all_dates)
            first_dates[well_name] = first_date
            wells_with_data += 1
            if wells_with_data <= 5:  # Show first 5 for sample
                print(f"  {well_name}: first data on {first_date}")
        else:
            # No data at all for this well - use global start date as fallback
            first_dates[well_name] = pd.to_datetime("2009-01-01").date()
            wells_without_data += 1
    
    print(f"Found first production dates for {wells_with_data} wells with data")
    if wells_without_data > 0:
        print(f"  {wells_without_data} wells have NO data (using 2009-01-01 as fallback)")
    return first_dates

def build_optimized_spine(mapping: pd.DataFrame, first_dates: dict, global_end: str) -> pd.DataFrame:
    """
    Build spine that starts at each well's first production date
    """
    print("\nBuilding optimized data spine (starting at first production date)...")
    
    all_spines = []
    global_end_date = pd.to_datetime(global_end).date()
    total_days = 0
    
    for _, row in mapping.iterrows():
        well_name = row["Well Name"]
        gas_id = row["GasIDREC"]
        pressures_id = row["PressuresIDREC"]
        
        # Get this well's first production date
        first_date = first_dates.get(well_name)
        if first_date is None:
            print(f"  ⚠️ No first date for {well_name}, skipping")
            continue
        
        # Create date range from first_date to global_end
        days = pd.date_range(start=first_date, end=global_end_date, freq="D").date
        total_days += len(days)
        
        # Create dataframe for this well
        well_spine = pd.DataFrame({
            "GasIDREC": gas_id,
            "PressuresIDREC": pressures_id,
            "Well Name": well_name,
            "ProdDate": days,
            "Formation Producer": row["Formation Producer"],
            "Layer Producer": row["Layer Producer"],
            "Fault Block": row["Fault Block"],
            "Pad Name": row["Pad Name"],
            "Lateral Length": row["Lateral Length"],
            "Orient": row["Orient"]
        })
        
        all_spines.append(well_spine)
    
    # Combine all well spines
    if all_spines:
        spine = pd.concat(all_spines, ignore_index=True)
        spine = spine.sort_values(["Well Name", "ProdDate"]).reset_index(drop=True)
        
        print(f"Created optimized spine with {len(spine):,} rows")
        print(f"  Average days per well: {total_days/len(mapping):.0f}")
        return spine
    else:
        print("No wells found!")
        return pd.DataFrame()

def process_well_batch(well_data, ecf, gaswh, cgr, wgr, pressures, alloc, alloc_water):
    """
    Process a single well's data and merge all sources.
    This is done per well to maintain well-first order.
    """
    well_name = well_data["Well Name"].iloc[0]
    gas_id = well_data["GasIDREC"].iloc[0]
    pressures_id = well_data["PressuresIDREC"].iloc[0]
    
    # Filter data for this specific well
    well_ecf = ecf[ecf["GasIDREC"] == gas_id].copy() if gas_id in ecf["GasIDREC"].values else pd.DataFrame()
    well_gaswh = gaswh[gaswh["GasIDREC"] == gas_id].copy() if gas_id in gaswh["GasIDREC"].values else pd.DataFrame()
    well_cgr = cgr[cgr["PressuresIDREC"] == pressures_id].copy() if pressures_id in cgr["PressuresIDREC"].values else pd.DataFrame()
    well_wgr = wgr[wgr["PressuresIDREC"] == pressures_id].copy() if pressures_id in wgr["PressuresIDREC"].values else pd.DataFrame()
    well_pressures = pressures[pressures["PressuresIDREC"] == pressures_id].copy() if pressures_id in pressures["PressuresIDREC"].values else pd.DataFrame()
    well_alloc = alloc[alloc["PressuresIDREC"] == pressures_id].copy() if pressures_id in alloc["PressuresIDREC"].values else pd.DataFrame()
    well_alloc_water = alloc_water[alloc_water["PressuresIDREC"] == pressures_id].copy() if pressures_id in alloc_water["PressuresIDREC"].values else pd.DataFrame()
    
    # Start with the well's date spine
    result = well_data.copy()
    
    # Merge each data source (left join to keep all dates)
    if not well_ecf.empty:
        result = result.merge(well_ecf, on=["GasIDREC", "ProdDate"], how="left")
    else:
        result["ECF_Ratio"] = None
    
    if not well_gaswh.empty:
        result = result.merge(well_gaswh, on=["GasIDREC", "ProdDate"], how="left")
    else:
        result["GasWH_Production"] = None
        result["OnProdHours"] = None
    
    if not well_cgr.empty:
        result = result.merge(well_cgr, on=["PressuresIDREC", "ProdDate"], how="left")
    else:
        result["CGR_Ratio"] = None
    
    if not well_wgr.empty:
        result = result.merge(well_wgr, on=["PressuresIDREC", "ProdDate"], how="left")
    else:
        result["WGR_Ratio"] = None
    
    if not well_pressures.empty:
        result = result.merge(well_pressures, on=["PressuresIDREC", "ProdDate"], how="left")
    else:
        result["TubingPressure"] = None
        result["CasingPressure"] = None
        result["ChokeSize"] = None
    
    # Calculate Condensate_WH_Production
    if "GasWH_Production" in result.columns and "CGR_Ratio" in result.columns:
        result["Condensate_WH_Production"] = result["GasWH_Production"] * result["CGR_Ratio"]
    else:
        result["Condensate_WH_Production"] = None
    
    if not well_alloc.empty:
        result = result.merge(well_alloc, on=["PressuresIDREC", "ProdDate"], how="left")
    else:
        result["Gathered_Gas_Production"] = None
        result["Gathered_Condensate_Production"] = None
        result["NGL_Production"] = None
    
    if not well_alloc_water.empty:
        result = result.merge(well_alloc_water, on=["PressuresIDREC", "ProdDate"], how="left")
    else:
        result["AllocatedWater_Rate"] = None
    
    return result


if __name__ == "__main__":
    start = "2009-01-01"
    # Get current date in YYYY-MM-DD format
    end = datetime.now().strftime('%Y-%m-%d')
    
    print("=" * 60)
    print(f"STARTING DATA PIPELINE - OPTIMIZED WELL-BY-WELL ORDER")
    print(f"Date range: {start} to {end} (current date)")
    print("=" * 60)
    
    # Step 1: Check SQL Server table exists and clear data
    print("\n[Step 1/10] Preparing SQL Server database...")
    print("  Verifying PCE_CDA table exists...")
    if not ensure_pce_cda_table():
        print("  ❌ Cannot proceed. Please create PCE_CDA table first.")
        exit(1)
    
    print("  Clearing existing data in range...")
    delete_pce_cda_range(start, end)
    
    # Step 2: Pull mapping data with all PCE_WM fields
    print("\n[Step 2/10] Pulling well mapping data from SQL Server...")
    mapping = pull_mapping()
    
    # Step 3: Pull all data from Snowflake
    print("\n[Step 3/10] Pulling data from Snowflake...")
    print("  Pulling ECF data...")
    ecf = pull_ecf(start, end)
    
    print("  Pulling GasWH data...")
    gaswh = pull_gaswh(start, end)
    
    print("  Pulling CGR data...")
    cgr = pull_cgr(start, end)
    
    print("  Pulling WGR data...")
    wgr = pull_wgr(start, end)
    
    print("  Pulling Pressures data...")
    pressures = pull_pressures(start, end)
    
    print("  Pulling Allocation data...")
    alloc = pull_allocations(start, end)
    
    print("  Pulling Allocated Water data...")
    alloc_water = pull_alloc_water(start, end)
    
    print("  Snowflake data pull complete")
    
    # Step 4: Find first production dates for each well
    print("\n[Step 4/10] Finding first production dates for each well...")
    first_dates = get_first_production_dates(mapping, ecf, gaswh, cgr, wgr, pressures, alloc, alloc_water)
    
    # Step 5: Build optimized data spine
    print("\n[Step 5/10] Building optimized data spine...")
    spine = build_optimized_spine(mapping, first_dates, end)
    
    # Step 6: Process wells one by one in order
    print("\n[Step 6/10] Processing wells in order (well-by-well, date-by-date)...")
    
    # Get unique wells in sorted order
    unique_wells = mapping["Well Name"].sort_values().unique()
    total_wells = len(unique_wells)
    
    # Initialize empty list to store results
    all_results = []
    
    # Process each well sequentially
    for idx, well_name in enumerate(unique_wells, 1):
        print(f"  Processing well {idx}/{total_wells}: {well_name}")
        
        # Get this well's spine data (already includes PCE_WM fields)
        well_spine = spine[spine["Well Name"] == well_name].copy()
        
        if well_spine.empty:
            print(f"    ⚠️ No spine data for {well_name}, skipping")
            continue
        
        # Process this well
        well_result = process_well_batch(
            well_spine, ecf, gaswh, cgr, wgr, pressures, alloc, alloc_water
        )
        
        # Append to results (only if not empty)
        if not well_result.empty:
            all_results.append(well_result)
        
        if idx % 10 == 0:
            print(f"    ✓ Completed {idx} wells so far...")
    
    # Step 7: Combine all well results
    print("\n[Step 7/10] Combining all well data...")
    if all_results:
        joined = pd.concat(all_results, ignore_index=True)
        print(f"  Combined dataframe rows: {len(joined):,}")
    else:
        joined = pd.DataFrame()
        print("  No data to combine")
    
    # Step 8: Data validation
    print("\n[Step 8/10] Validating data...")
    if not joined.empty:
        print(f"  Final dataframe rows: {len(joined):,}")
        print(f"  Final dataframe columns: {len(joined.columns)}")
        
        print("\n  Null value counts:")
        important_cols = ['GasWH_Production', 'ECF_Ratio', 'CGR_Ratio', 'WGR_Ratio', 
                          'TubingPressure', 'CasingPressure', 'Gathered_Gas_Production']
        for col in important_cols:
            if col in joined.columns:
                null_count = joined[col].isna().sum()
                pct = (null_count / len(joined)) * 100 if len(joined) > 0 else 0
                print(f"    {col}: {null_count:,} nulls ({pct:.1f}%)")
    else:
        print("  No data to validate")
    
    # Step 9: Load into SQL Server
    print("\n[Step 9/10] Loading data into SQL Server...")
    if not joined.empty:
        print(f"  Inserting {len(joined):,} rows in well-first order...")
        insert_pce_cda_rows(joined)
    else:
        print("  No data to insert")
    
    # Step 10: Final summary
    print("\n[Step 10/10] Pipeline completed successfully!")
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Date range: {start} to {end} (current date)")
    print(f"Number of wells: {len(mapping)}")
    
    if not joined.empty:
        print(f"Total records loaded into SQL Server: {len(joined):,}")
        # Calculate estimated savings
        full_range_days = len(pd.date_range(start=start, end=end, freq='D'))
        estimated_full_rows = len(mapping) * full_range_days
        savings = estimated_full_rows - len(joined)
        savings_pct = (savings / estimated_full_rows * 100) if estimated_full_rows > 0 else 0
        print(f"Estimated rows if using full range: {estimated_full_rows:,}")
        print(f"Rows saved by optimization: {savings:,} ({savings_pct:.1f}%)")
    else:
        print("Total records loaded into SQL Server: 0")
    
    print(f"Destination: {SQL_SERVER}.{SQL_DATABASE}.PCE_CDA")
    print(f"Columns loaded: {len(joined.columns) if not joined.empty else 0}")
    print(f"Insert order: Well-by-well, date-by-date (optimized)")
    print(f"PCE_WM fields added: Formation Producer, Layer Producer, Fault Block, Pad Name, Lateral Length, Orient")
    
    print("\nData source row counts:")
    print(f"  ECF: {len(ecf):,}")
    print(f"  GasWH: {len(gaswh):,}")
    print(f"  CGR: {len(cgr):,}")
    print(f"  WGR: {len(wgr):,}")
    print(f"  Pressures: {len(pressures):,}")
    print(f"  Allocations: {len(alloc):,}")
    print(f"  Allocated Water: {len(alloc_water):,}")
    
    # Show sample of final data
    if not joined.empty:
        print("\nFirst 3 rows of loaded data:")
        print("-" * 100)
        sample_cols = ['Well Name', 'ProdDate', 'GasWH_Production', 'CGR_Ratio', 
                       'TubingPressure', 'Gathered_Gas_Production', 'Formation Producer', 'Pad Name']
        display_cols = [col for col in sample_cols if col in joined.columns]
        if display_cols:
            print(joined[display_cols].head(3).to_string(index=False))
        print("-" * 100)
        
        # Show well order sample
        print("\nSample of well order (first 5 wells, first date each):")
        print("-" * 100)
        first_wells = joined.drop_duplicates(subset=["Well Name"]).head(5)
        for _, row in first_wells.iterrows():
            print(f"  {row['Well Name']} - First date: {row['ProdDate']}")
        print("-" * 100)
    else:
        print("\nNo data loaded to display")
    
    print("=" * 60)
'''

import pandas as pd
from snowflake_connector import SnowflakeConnector
import numpy as np  
from dotenv import load_dotenv
import os
import pyodbc
import warnings
from datetime import datetime
warnings.filterwarnings('ignore', category=FutureWarning)

load_dotenv()

# SQL Server connection settings
SQL_SERVER = os.getenv("SQL_SERVER", "CALVMSQL02")
SQL_DATABASE = os.getenv("SQL_DATABASE", "Re_Main_Production")
SQL_DRIVER = os.getenv("SQL_DRIVER", "{ODBC Driver 17 for SQL Server}")

def get_sql_conn():
    """Create connection to SQL Server"""
    conn_str = (
        f'DRIVER={SQL_DRIVER};'
        f'SERVER={SQL_SERVER};'
        f'DATABASE={SQL_DATABASE};'
        f'Trusted_Connection=yes;'
    )
    return pyodbc.connect(conn_str)

def ensure_pce_cda_table():
    """Check if PCE_CDA table exists"""
    print("  Verifying PCE_CDA table exists in SQL Server...")
    with get_sql_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'PCE_CDA'
        """)
        if cursor.fetchone()[0] == 0:
            print("  ⚠️ PCE_CDA table not found! Please create it first.")
            return False
        print("  ✅ PCE_CDA table exists")
        return True

def delete_pce_cda_range(start_date, end_date):
    """Delete records in date range from SQL Server"""
    print(f"  Deleting records from {start_date} to {end_date}...")
    with get_sql_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM PCE_CDA 
            WHERE ProdDate BETWEEN ? AND ?
        """, start_date, end_date)
        deleted = cursor.rowcount
        conn.commit()
        print(f"  Deleted {deleted:,} records")
        return deleted

def insert_pce_cda_rows(df):
    """
    Insert dataframe into SQL Server PCE_CDA table
    Processes in batches for better performance
    """
    if df.empty:
        print("  No rows to insert")
        return 0
    
    print(f"  Inserting {len(df):,} rows into SQL Server...")
    
    # CLEAN THE DATA - Replace NaN/Inf with None (SQL NULL)
    df_clean = df.copy()
    
    # List of float columns that might have issues
    float_cols = [
        'GasWH_Production', 'Condensate_WH_Production',
        'WGR_Ratio', 'CGR_Ratio', 'ECF_Ratio',
        'OnProdHours', 'TubingPressure', 'CasingPressure', 'ChokeSize',
        'Gathered_Gas_Production', 'Gathered_Condensate_Production',
        'NGL_Production', 'AllocatedWater_Rate', 'Lateral Length'
    ]
    
    # Clean each float column
    for col in float_cols:
        if col in df_clean.columns:
            # Replace NaN, Inf, -Inf with None
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
            df_clean[col] = df_clean[col].replace([np.inf, -np.inf], np.nan)
    
    # Define the insert SQL with the new columns
    insert_sql = """
    INSERT INTO PCE_CDA (
        [GasIDREC], [PressuresIDREC], [Well Name], [ProdDate],
        [GasWH_Production], [Condensate_WH_Production],
        [WGR_Ratio], [CGR_Ratio], [ECF_Ratio],
        [OnProdHours], [TubingPressure], [CasingPressure], [ChokeSize],
        [Gathered_Gas_Production], [Gathered_Condensate_Production],
        [NGL_Production], [AllocatedWater_Rate],
        [Formation Producer], [Layer Producer], [Fault Block], [Pad Name],
        [Lateral Length], [Orient]
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    # Convert dataframe to list of tuples, replacing NaN with None
    rows_to_insert = []
    for _, row in df_clean.iterrows():
        rows_to_insert.append((
            row.get('GasIDREC', None),
            row.get('PressuresIDREC', None),
            row.get('Well Name', None),
            row.get('ProdDate', None),
            None if pd.isna(row.get('GasWH_Production')) else float(row.get('GasWH_Production')),
            None if pd.isna(row.get('Condensate_WH_Production')) else float(row.get('Condensate_WH_Production')),
            None if pd.isna(row.get('WGR_Ratio')) else float(row.get('WGR_Ratio')),
            None if pd.isna(row.get('CGR_Ratio')) else float(row.get('CGR_Ratio')),
            None if pd.isna(row.get('ECF_Ratio')) else float(row.get('ECF_Ratio')),
            None if pd.isna(row.get('OnProdHours')) else float(row.get('OnProdHours')),
            None if pd.isna(row.get('TubingPressure')) else float(row.get('TubingPressure')),
            None if pd.isna(row.get('CasingPressure')) else float(row.get('CasingPressure')),
            None if pd.isna(row.get('ChokeSize')) else float(row.get('ChokeSize')),
            None if pd.isna(row.get('Gathered_Gas_Production')) else float(row.get('Gathered_Gas_Production')),
            None if pd.isna(row.get('Gathered_Condensate_Production')) else float(row.get('Gathered_Condensate_Production')),
            None if pd.isna(row.get('NGL_Production')) else float(row.get('NGL_Production')),
            None if pd.isna(row.get('AllocatedWater_Rate')) else float(row.get('AllocatedWater_Rate')),
            # New columns from PCE_WM
            row.get('Formation Producer', None),
            row.get('Layer Producer', None),
            row.get('Fault Block', None),
            row.get('Pad Name', None),
            None if pd.isna(row.get('Lateral Length')) else float(row.get('Lateral Length')),
            row.get('Orient', None)
        ))
    
    # Insert in batches
    batch_size = 1000
    total_inserted = 0
    
    with get_sql_conn() as conn:
        cursor = conn.cursor()
        
        for i in range(0, len(rows_to_insert), batch_size):
            batch = rows_to_insert[i:i + batch_size]
            try:
                cursor.executemany(insert_sql, batch)
                conn.commit()
                total_inserted += len(batch)
            except Exception as e:
                print(f"    ❌ Error on batch starting at row {i}: {e}")
                # Try one row at a time to find the bad row
                for j, row in enumerate(batch):
                    try:
                        cursor.execute(insert_sql, row)
                        conn.commit()
                        total_inserted += 1
                    except Exception as row_e:
                        print(f"      ❌ Bad row at position {i+j}: {row_e}")
                # Continue with next batch
                continue
            
            if (i + batch_size) % 5000 == 0 or (i + batch_size) >= len(rows_to_insert):
                print(f"    Inserted {min(i + batch_size, len(rows_to_insert)):,} rows...")
    
    print(f"  ✅ Successfully inserted {total_inserted:,} rows")
    return total_inserted

def pull_mapping():
    """Pulls mapping from SQL Server PCE_WM including all needed fields"""
    print("Pulling mapping data from SQL Server...")
    sql = """
    SELECT
        GasIDREC,
        PressuresIDREC,
        [Well Name],
        [Formation Producer],
        [Layer Producer],
        [Fault Block],
        [Pad Name],
        [Lateral Length],
        [Orient]
    FROM PCE_WM
    WHERE GasIDREC IS NOT NULL
    """
    with get_sql_conn() as cn:
        df = pd.read_sql(sql, cn)

    df["GasIDREC"] = df["GasIDREC"].astype(str).str.strip()
    df["PressuresIDREC"] = df["PressuresIDREC"].astype(str).str.strip()
    df["Well Name"] = df["Well Name"].astype(str).str.strip()
    df["Formation Producer"] = df["Formation Producer"].astype(str).str.strip()
    df["Layer Producer"] = df["Layer Producer"].astype(str).str.strip()
    df["Fault Block"] = df["Fault Block"].astype(str).str.strip()
    df["Pad Name"] = df["Pad Name"].astype(str).str.strip()
    df["Lateral Length"] = pd.to_numeric(df["Lateral Length"], errors='coerce')
    df["Orient"] = df["Orient"].astype(str).str.strip()
    
    df = df.drop_duplicates(subset=["GasIDREC"])
    print(f"Found {len(df)} unique wells")
    return df

def pull_ecf(start: str, end: str) -> pd.DataFrame:
    print("  Pulling ECF data from Snowflake...")
    sf = SnowflakeConnector()

    sql = f"""
    SELECT
        IDRECPARENT AS GasIDREC,
        CAST (DTTM AS DATE) AS ProdDate,
        EFFLUENTFACTOR AS ECF_Ratio
    FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitMeterOrificeEcf
    WHERE DTTM >= '{start}'
      AND DTTM <= '{end}'
    """

    df = sf.query(sql)
    sf.close()

    # normalize types
    df["GasIDREC"] = df["GASIDREC"].astype(str).str.strip() if "GASIDREC" in df.columns else df["GasIDREC"].astype(str).str.strip()
    df["ProdDate"] = pd.to_datetime(df["PRODDATE"] if "PRODDATE" in df.columns else df["ProdDate"]).dt.date
    df["ECF_Ratio"] = pd.to_numeric(df["ECF_RATIO"] if "ECF_RATIO" in df.columns else df["ECF_Ratio"], errors="coerce")

    out = pd.DataFrame({
        "GasIDREC": df["GasIDREC"],
        "ProdDate": df["ProdDate"],
        "ECF_Ratio": df["ECF_Ratio"],
    })

    print(f"    ECF data pulled: {len(out):,} rows (range: {out['ProdDate'].min()} to {out['ProdDate'].max()})")
    return out

def pull_gaswh(start: str, end: str) -> pd.DataFrame:
    print("  Pulling GasWH data from Snowflake...")
    sf = SnowflakeConnector()

    sql = f"""
    SELECT
        IDRECPARENT AS GasIDREC,
        CAST(DTTM AS DATE) AS ProdDate,
        VOLENTERGAS AS GasWH_Production,
        DURONOR AS OnProdHours
    FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitMeterOrificeEntry
    WHERE DTTM >= '{start}'
      AND DTTM <= '{end}'
    """

    df = sf.query(sql)
    sf.close()

    cols = {c.upper(): c for c in df.columns}
    gas_col = cols.get("GASIDREC", "GasIDREC")
    date_col = cols.get("PRODDATE", "ProdDate")
    gaswh_col = cols.get("GASWH_PRODUCTION", "GasWH_Production")
    hrs_col = cols.get("ONPRODHOURS", "OnProdHours")

    out = pd.DataFrame({
        "GasIDREC": df[gas_col].astype(str).str.strip(),
        "ProdDate": pd.to_datetime(df[date_col]).dt.date,
        "GasWH_Production": pd.to_numeric(df[gaswh_col], errors="coerce"),
        "OnProdHours": pd.to_numeric(df[hrs_col], errors="coerce"),
    })

    print(f"    GasWH data pulled: {len(out):,} rows (range: {out['ProdDate'].min()} to {out['ProdDate'].max()})")
    return out

def pull_cgr(start: str, end: str) -> pd.DataFrame:
    print("  Pulling CGR data from Snowflake...")
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
      AND DTTM <= '{end}'
    """

    df = sf.query(sql)
    sf.close()

    cols = {c.upper(): c for c in df.columns}
    pid_col = cols.get("PRESSURESIDREC", "PressuresIDREC")
    date_col = cols.get("PRODDATE", "ProdDate")
    val_col = cols.get("CGR_RATIO", "CGR_Ratio")

    out = pd.DataFrame({
        "PressuresIDREC": df[pid_col].astype(str).str.strip(),
        "ProdDate": pd.to_datetime(df[date_col]).dt.date,
        "CGR_Ratio": pd.to_numeric(df[val_col], errors="coerce")
    })

    print(f"    CGR data pulled: {len(out):,} rows (range: {out['ProdDate'].min()} to {out['ProdDate'].max()})")
    return out

def pull_wgr(start: str, end: str) -> pd.DataFrame:
    print("  Pulling WGR data from Snowflake...")
    sf = SnowflakeConnector()

    sql = f"""
    SELECT
        IDRECPARENT AS PressuresIDREC,
        CAST(DTTM AS DATE) AS ProdDate,
        WGR AS WGR_Ratio
    FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitCompRatios
    WHERE DTTM >= '{start}'
      AND DTTM <= '{end}'
    """

    df = sf.query(sql)
    sf.close()

    cols = {c.upper(): c for c in df.columns}
    pid_col = cols.get("PRESSURESIDREC", "PressuresIDREC")
    date_col = cols.get("PRODDATE", "ProdDate")
    val_col = cols.get("WGR_RATIO", "WGR_Ratio")

    out = pd.DataFrame({
        "PressuresIDREC": df[pid_col].astype(str).str.strip(),
        "ProdDate": pd.to_datetime(df[date_col]).dt.date,
        "WGR_Ratio": pd.to_numeric(df[val_col], errors="coerce")
    })

    print(f"    WGR data pulled: {len(out):,} rows (range: {out['ProdDate'].min()} to {out['ProdDate'].max()})")
    return out

def pull_pressures(start: str, end: str) -> pd.DataFrame:
    print("  Pulling Pressures data from Snowflake...")
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
      AND DTTM <= '{end}'
    """

    df = sf.query(sql)
    sf.close()

    cols = {c.upper(): c for c in df.columns}
    pid_col = cols.get("PRESSURESIDREC", "PressuresIDREC")
    date_col = cols.get("PRODDATE", "ProdDate")
    tub_col = cols.get("TUBINGPRESSURE", "TubingPressure")
    cas_col = cols.get("CASINGPRESSURE", "CasingPressure")
    choke_col = cols.get("CHOKESIZE", "ChokeSize")

    out = pd.DataFrame({
        "PressuresIDREC": df[pid_col].astype(str).str.strip(),
        "ProdDate": pd.to_datetime(df[date_col]).dt.date,
        "TubingPressure": pd.to_numeric(df[tub_col], errors="coerce"),
        "CasingPressure": pd.to_numeric(df[cas_col], errors="coerce"),
        "ChokeSize": pd.to_numeric(df[choke_col], errors="coerce"),
    })

    print(f"    Pressures data pulled: {len(out):,} rows (range: {out['ProdDate'].min()} to {out['ProdDate'].max()})")
    return out

def pull_allocations(start: str, end: str) -> pd.DataFrame:
    print("  Pulling Allocation data from Snowflake...")
    sf = SnowflakeConnector()

    sql = f"""
    SELECT
        IDRECCOMP AS PressuresIDREC,
        CAST(DTTM AS DATE) AS ProdDate,
        VOLPRODGATHGAS AS Gathered_Gas_Production,
        VOLPRODGATHHCLIQ AS Gathered_Condensate_Production,
        VOLNEWPRODALLOCNGL AS NGL_Production
    FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvunitallocmonthday
    WHERE DTTM >= '{start}'
      AND DTTM <= '{end}'
    """

    df = sf.query(sql)
    sf.close()

    cols = {c.upper(): c for c in df.columns}
    pid_col = cols.get("PRESSURESIDREC", "PressuresIDREC")
    date_col = cols.get("PRODDATE", "ProdDate")
    gas_col = cols.get("GATHERED_GAS_PRODUCTION", "Gathered_Gas_Production")
    cond_col = cols.get("GATHERED_CONDENSATE_PRODUCTION", "Gathered_Condensate_Production")
    ngl_col = cols.get("NGL_PRODUCTION", "NGL_Production")

    out = pd.DataFrame({
        "PressuresIDREC": df[pid_col].astype(str).str.strip(),
        "ProdDate": pd.to_datetime(df[date_col]).dt.date,
        "Gathered_Gas_Production": pd.to_numeric(df[gas_col], errors="coerce"),
        "Gathered_Condensate_Production": pd.to_numeric(df[cond_col], errors="coerce"),
        "NGL_Production": pd.to_numeric(df[ngl_col], errors="coerce"),
    })

    print(f"    Allocation data pulled: {len(out):,} rows (range: {out['ProdDate'].min()} to {out['ProdDate'].max()})")
    return out

def pull_alloc_water(start: str, end: str) -> pd.DataFrame:
    print("  Pulling Allocated Water data from Snowflake...")
    sf = SnowflakeConnector()

    sql = f"""
    SELECT
        IDRECCOMP AS PressuresIDREC,
        CAST(DTTM AS DATE) AS ProdDate,
        VOLWATER AS AllocatedWater_Rate
    FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvunitcompgathmonthdaycalc
    WHERE DTTM >= '{start}'
      AND DTTM <= '{end}'
    """

    df = sf.query(sql)
    sf.close()

    cols = {c.upper(): c for c in df.columns}
    pid_col = cols.get("PRESSURESIDREC", "PressuresIDREC")
    date_col = cols.get("PRODDATE", "ProdDate")
    val_col = cols.get("ALLOCATEDWATER_RATE", "AllocatedWater_Rate")

    out = pd.DataFrame({
        "PressuresIDREC": df[pid_col].astype(str).str.strip(),
        "ProdDate": pd.to_datetime(df[date_col]).dt.date,
        "AllocatedWater_Rate": pd.to_numeric(df[val_col], errors="coerce")
    })

    print(f"    Allocated Water data pulled: {len(out):,} rows (range: {out['ProdDate'].min()} to {out['ProdDate'].max()})")
    return out

def get_first_production_dates(mapping, ecf, gaswh, cgr, wgr, pressures, alloc, alloc_water):
    """
    For each well, find the absolute earliest date where ANY data exists
    """
    print("\nFinding first production date for each well...")
    
    first_dates = {}
    wells_with_data = 0
    wells_without_data = 0
    problem_wells = []
    
    for idx, row in mapping.iterrows():
        well_name = row["Well Name"]
        gas_id = row["GasIDREC"]
        pressures_id = row["PressuresIDREC"]
        
        # Collect ALL dates from ALL sources
        all_dates = []
        source_counts = {}
        
        # Check each source and add EVERY date
        if gas_id in ecf["GasIDREC"].values:
            dates = ecf[ecf["GasIDREC"] == gas_id]["ProdDate"].tolist()
            all_dates.extend(dates)
            source_counts['ECF'] = len(dates)
        
        if gas_id in gaswh["GasIDREC"].values:
            dates = gaswh[gaswh["GasIDREC"] == gas_id]["ProdDate"].tolist()
            all_dates.extend(dates)
            source_counts['GasWH'] = len(dates)
        
        if pressures_id in cgr["PressuresIDREC"].values:
            dates = cgr[cgr["PressuresIDREC"] == pressures_id]["ProdDate"].tolist()
            all_dates.extend(dates)
            source_counts['CGR'] = len(dates)
        
        if pressures_id in wgr["PressuresIDREC"].values:
            dates = wgr[wgr["PressuresIDREC"] == pressures_id]["ProdDate"].tolist()
            all_dates.extend(dates)
            source_counts['WGR'] = len(dates)
        
        if pressures_id in pressures["PressuresIDREC"].values:
            dates = pressures[pressures["PressuresIDREC"] == pressures_id]["ProdDate"].tolist()
            all_dates.extend(dates)
            source_counts['Pressures'] = len(dates)
        
        if pressures_id in alloc["PressuresIDREC"].values:
            dates = alloc[alloc["PressuresIDREC"] == pressures_id]["ProdDate"].tolist()
            all_dates.extend(dates)
            source_counts['Alloc'] = len(dates)
        
        if pressures_id in alloc_water["PressuresIDREC"].values:
            dates = alloc_water[alloc_water["PressuresIDREC"] == pressures_id]["ProdDate"].tolist()
            all_dates.extend(dates)
            source_counts['AllocWater'] = len(dates)
        
        if all_dates:
            # Find the absolute earliest date
            first_date = min(all_dates)
            first_dates[well_name] = first_date
            wells_with_data += 1
            
            # Check if this well might have earlier data than we think
            if first_date.year > 2012:  # Adjust this threshold as needed
                problem_wells.append(well_name)
                print(f"\n  ⚠️ {well_name}: first data on {first_date}")
                print(f"     Source breakdown: {source_counts}")
                
                # Double-check each source individually
                if gas_id in gaswh["GasIDREC"].values:
                    wh_min = gaswh[gaswh["GasIDREC"] == gas_id]["ProdDate"].min()
                    print(f"       GasWH min: {wh_min}")
                if gas_id in ecf["GasIDREC"].values:
                    ecf_min = ecf[ecf["GasIDREC"] == gas_id]["ProdDate"].min()
                    print(f"       ECF min: {ecf_min}")
        else:
            first_dates[well_name] = pd.to_datetime("2009-01-01").date()
            wells_without_data += 1
            if wells_without_data <= 5:
                print(f"  {well_name}: NO DATA (using 2009-01-01)")
    
    print(f"\nFound first production dates for {wells_with_data} wells with data")
    if wells_without_data > 0:
        print(f"  {wells_without_data} wells have NO data (using 2009-01-01)")
    if problem_wells:
        print(f"\n⚠️ {len(problem_wells)} wells have first date after 2012 - check these:")
        for well in problem_wells[:10]:
            print(f"  - {well}")
        if len(problem_wells) > 10:
            print(f"  ... and {len(problem_wells) - 10} more")
    
    return first_dates

def build_optimized_spine(mapping: pd.DataFrame, first_dates: dict, global_end: str) -> pd.DataFrame:
    """
    Build spine that starts at each well's first production date
    """
    print("\nBuilding optimized data spine (starting at first production date)...")
    
    all_spines = []
    global_end_date = pd.to_datetime(global_end).date()
    total_days = 0
    
    for _, row in mapping.iterrows():
        well_name = row["Well Name"]
        gas_id = row["GasIDREC"]
        pressures_id = row["PressuresIDREC"]
        
        # Get this well's first production date
        first_date = first_dates.get(well_name)
        if first_date is None:
            print(f"  ⚠️ No first date for {well_name}, using 2009-01-01")
            first_date = pd.to_datetime("2009-01-01").date()
        
        # Ensure first_date is a date object
        if isinstance(first_date, pd.Timestamp):
            first_date = first_date.date()
        
        # Create date range from first_date to global_end
        days = pd.date_range(start=first_date, end=global_end_date, freq="D").date
        total_days += len(days)
        
        # Create dataframe for this well
        well_spine = pd.DataFrame({
            "GasIDREC": gas_id,
            "PressuresIDREC": pressures_id,
            "Well Name": well_name,
            "ProdDate": days,
            "Formation Producer": row["Formation Producer"],
            "Layer Producer": row["Layer Producer"],
            "Fault Block": row["Fault Block"],
            "Pad Name": row["Pad Name"],
            "Lateral Length": row["Lateral Length"],
            "Orient": row["Orient"]
        })
        
        all_spines.append(well_spine)
    
    # Combine all well spines
    if all_spines:
        spine = pd.concat(all_spines, ignore_index=True)
        spine = spine.sort_values(["Well Name", "ProdDate"]).reset_index(drop=True)
        
        print(f"Created optimized spine with {len(spine):,} rows")
        print(f"  Average days per well: {total_days/len(mapping):.0f}")
        return spine
    else:
        print("No wells found!")
        return pd.DataFrame()

def process_well_batch(well_data, ecf, gaswh, cgr, wgr, pressures, alloc, alloc_water):
    """
    Process a single well's data and merge all sources.
    This is done per well to maintain well-first order.
    """
    well_name = well_data["Well Name"].iloc[0]
    gas_id = well_data["GasIDREC"].iloc[0]
    pressures_id = well_data["PressuresIDREC"].iloc[0]
    
    # Filter data for this specific well
    well_ecf = ecf[ecf["GasIDREC"] == gas_id].copy() if gas_id in ecf["GasIDREC"].values else pd.DataFrame()
    well_gaswh = gaswh[gaswh["GasIDREC"] == gas_id].copy() if gas_id in gaswh["GasIDREC"].values else pd.DataFrame()
    well_cgr = cgr[cgr["PressuresIDREC"] == pressures_id].copy() if pressures_id in cgr["PressuresIDREC"].values else pd.DataFrame()
    well_wgr = wgr[wgr["PressuresIDREC"] == pressures_id].copy() if pressures_id in wgr["PressuresIDREC"].values else pd.DataFrame()
    well_pressures = pressures[pressures["PressuresIDREC"] == pressures_id].copy() if pressures_id in pressures["PressuresIDREC"].values else pd.DataFrame()
    well_alloc = alloc[alloc["PressuresIDREC"] == pressures_id].copy() if pressures_id in alloc["PressuresIDREC"].values else pd.DataFrame()
    well_alloc_water = alloc_water[alloc_water["PressuresIDREC"] == pressures_id].copy() if pressures_id in alloc_water["PressuresIDREC"].values else pd.DataFrame()
    
    # Start with the well's date spine
    result = well_data.copy()
    
    # Merge each data source (left join to keep all dates)
    if not well_ecf.empty:
        result = result.merge(well_ecf, on=["GasIDREC", "ProdDate"], how="left")
    else:
        result["ECF_Ratio"] = None
    
    if not well_gaswh.empty:
        result = result.merge(well_gaswh, on=["GasIDREC", "ProdDate"], how="left")
    else:
        result["GasWH_Production"] = None
        result["OnProdHours"] = None
    
    if not well_cgr.empty:
        result = result.merge(well_cgr, on=["PressuresIDREC", "ProdDate"], how="left")
    else:
        result["CGR_Ratio"] = None
    
    if not well_wgr.empty:
        result = result.merge(well_wgr, on=["PressuresIDREC", "ProdDate"], how="left")
    else:
        result["WGR_Ratio"] = None
    
    if not well_pressures.empty:
        result = result.merge(well_pressures, on=["PressuresIDREC", "ProdDate"], how="left")
    else:
        result["TubingPressure"] = None
        result["CasingPressure"] = None
        result["ChokeSize"] = None
    
    # Calculate Condensate_WH_Production
    if "GasWH_Production" in result.columns and "CGR_Ratio" in result.columns:
        result["Condensate_WH_Production"] = result["GasWH_Production"] * result["CGR_Ratio"]
    else:
        result["Condensate_WH_Production"] = None
    
    if not well_alloc.empty:
        result = result.merge(well_alloc, on=["PressuresIDREC", "ProdDate"], how="left")
    else:
        result["Gathered_Gas_Production"] = None
        result["Gathered_Condensate_Production"] = None
        result["NGL_Production"] = None
    
    if not well_alloc_water.empty:
        result = result.merge(well_alloc_water, on=["PressuresIDREC", "ProdDate"], how="left")
    else:
        result["AllocatedWater_Rate"] = None
    
    return result

def validate_first_dates_with_sql(mapping, first_dates):
    """
    Validate first dates by checking actual SQL data
    """
    print("\nValidating first dates against SQL Server...")
    
    with get_sql_conn() as conn:
        for well_name in list(first_dates.keys())[:5]:  # Check first 5 wells
            cursor = conn.cursor()
            cursor.execute("""
                SELECT MIN(ProdDate) 
                FROM PCE_CDA 
                WHERE [Well Name] = ?
            """, well_name)
            result = cursor.fetchone()
            if result and result[0]:
                sql_min = result[0]
                our_min = first_dates[well_name]
                if isinstance(our_min, pd.Timestamp):
                    our_min = our_min.date()
                
                if sql_min != our_min:
                    print(f"  ⚠️ {well_name}: SQL says {sql_min}, we have {our_min}")

if __name__ == "__main__":
    start = "2009-01-01"
    # Get current date in YYYY-MM-DD format
    end = datetime.now().strftime('%Y-%m-%d')
    
    print("=" * 60)
    print(f"STARTING DATA PIPELINE - OPTIMIZED WELL-BY-WELL ORDER")
    print(f"Date range: {start} to {end} (current date)")
    print("=" * 60)
    
    # Step 1: Check SQL Server table exists and clear data
    print("\n[Step 1/10] Preparing SQL Server database...")
    print("  Verifying PCE_CDA table exists...")
    if not ensure_pce_cda_table():
        print("  ❌ Cannot proceed. Please create PCE_CDA table first.")
        exit(1)
    
    print("  Clearing existing data in range...")
    delete_pce_cda_range(start, end)
    
    # Step 2: Pull mapping data with all PCE_WM fields
    print("\n[Step 2/10] Pulling well mapping data from SQL Server...")
    mapping = pull_mapping()
    
    # Step 3: Pull all data from Snowflake
    print("\n[Step 3/10] Pulling data from Snowflake...")
    ecf = pull_ecf(start, end)
    gaswh = pull_gaswh(start, end)
    cgr = pull_cgr(start, end)
    wgr = pull_wgr(start, end)
    pressures = pull_pressures(start, end)
    alloc = pull_allocations(start, end)
    alloc_water = pull_alloc_water(start, end)
    print("  Snowflake data pull complete")
    
    # Step 4: Find first production dates for each well
    print("\n[Step 4/10] Finding first production dates for each well...")
    first_dates = get_first_production_dates(mapping, ecf, gaswh, cgr, wgr, pressures, alloc, alloc_water)
    
    # Step 5: Build optimized data spine
    print("\n[Step 5/10] Building optimized data spine...")
    spine = build_optimized_spine(mapping, first_dates, end)
    
    # Step 6: Process wells one by one in order
    print("\n[Step 6/10] Processing wells in order (well-by-well, date-by-date)...")
    
    # Get unique wells in sorted order
    unique_wells = mapping["Well Name"].sort_values().unique()
    total_wells = len(unique_wells)
    
    # Initialize empty list to store results
    all_results = []
    
    # Process each well sequentially
    for idx, well_name in enumerate(unique_wells, 1):
        print(f"  Processing well {idx}/{total_wells}: {well_name}")
        
        # Get this well's spine data
        well_spine = spine[spine["Well Name"] == well_name].copy()
        
        if well_spine.empty:
            print(f"    ⚠️ No spine data for {well_name}, skipping")
            continue
        
        # Process this well
        well_result = process_well_batch(
            well_spine, ecf, gaswh, cgr, wgr, pressures, alloc, alloc_water
        )
        
        # Append to results
        if not well_result.empty:
            all_results.append(well_result)
        
        if idx % 10 == 0:
            print(f"    ✓ Completed {idx} wells so far...")
    
    # Step 7: Combine all well results
    print("\n[Step 7/10] Combining all well data...")
    if all_results:
        joined = pd.concat(all_results, ignore_index=True)
        print(f"  Combined dataframe rows: {len(joined):,}")
    else:
        joined = pd.DataFrame()
        print("  No data to combine")
    
    # Step 8: Data validation
    print("\n[Step 8/10] Validating data...")
    if not joined.empty:
        print(f"  Final dataframe rows: {len(joined):,}")
        print(f"  Final dataframe columns: {len(joined.columns)}")
        
        print("\n  Null value counts:")
        important_cols = ['GasWH_Production', 'ECF_Ratio', 'CGR_Ratio', 'WGR_Ratio', 
                          'TubingPressure', 'CasingPressure', 'Gathered_Gas_Production']
        for col in important_cols:
            if col in joined.columns:
                null_count = joined[col].isna().sum()
                pct = (null_count / len(joined)) * 100 if len(joined) > 0 else 0
                print(f"    {col}: {null_count:,} nulls ({pct:.1f}%)")
    else:
        print("  No data to validate")
    
    # Step 9: Load into SQL Server
    print("\n[Step 9/10] Loading data into SQL Server...")
    if not joined.empty:
        print(f"  Inserting {len(joined):,} rows in well-first order...")
        insert_pce_cda_rows(joined)
    else:
        print("  No data to insert")
    
    # Step 10: Final summary
    print("\n[Step 10/10] Pipeline completed successfully!")
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Date range: {start} to {end} (current date)")
    print(f"Number of wells: {len(mapping)}")
    
    if not joined.empty:
        print(f"Total records loaded into SQL Server: {len(joined):,}")
        full_range_days = len(pd.date_range(start=start, end=end, freq='D'))
        estimated_full_rows = len(mapping) * full_range_days
        savings = estimated_full_rows - len(joined)
        savings_pct = (savings / estimated_full_rows * 100) if estimated_full_rows > 0 else 0
        print(f"Estimated rows if using full range: {estimated_full_rows:,}")
        print(f"Rows saved by optimization: {savings:,} ({savings_pct:.1f}%)")
    else:
        print("Total records loaded into SQL Server: 0")
    
    print(f"Destination: {SQL_SERVER}.{SQL_DATABASE}.PCE_CDA")
    print(f"Columns loaded: {len(joined.columns) if not joined.empty else 0}")
    print(f"Insert order: Well-by-well, date-by-date (optimized)")
    print(f"PCE_WM fields added: Formation Producer, Layer Producer, Fault Block, Pad Name, Lateral Length, Orient")
    
    print("\nData source row counts:")
    print(f"  ECF: {len(ecf):,}")
    print(f"  GasWH: {len(gaswh):,}")
    print(f"  CGR: {len(cgr):,}")
    print(f"  WGR: {len(wgr):,}")
    print(f"  Pressures: {len(pressures):,}")
    print(f"  Allocations: {len(alloc):,}")
    print(f"  Allocated Water: {len(alloc_water):,}")
    
    # Show sample of final data
    if not joined.empty:
        print("\nFirst 3 rows of loaded data:")
        print("-" * 100)
        sample_cols = ['Well Name', 'ProdDate', 'GasWH_Production', 'CGR_Ratio', 
                       'TubingPressure', 'Gathered_Gas_Production', 'Formation Producer', 'Pad Name']
        display_cols = [col for col in sample_cols if col in joined.columns]
        if display_cols:
            print(joined[display_cols].head(3).to_string(index=False))
        print("-" * 100)
        
        # Show well order sample
        print("\nSample of well order (first 5 wells, first date each):")
        print("-" * 100)
        first_wells = joined.drop_duplicates(subset=["Well Name"]).head(5)
        for _, row in first_wells.iterrows():
            print(f"  {row['Well Name']} - First date: {row['ProdDate']}")
        print("-" * 100)
    else:
        print("\nNo data loaded to display")
    
    print("=" * 60)