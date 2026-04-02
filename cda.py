
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
        cursor.fast_executemany = True
        
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
      AND ([Exception] IS NULL OR [Exception] = '' OR [Exception] = 'N')
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

def build_complete_spine(mapping: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    """
    Build complete spine with all wells × all days (no optimization yet)
    """
    print("\nBuilding complete data spine (all wells × all days)...")
    days = pd.date_range(start=start, end=end, freq="D").date
    days_df = pd.DataFrame({"ProdDate": days})
    
    # Create cross join between wells and days
    wells = mapping[["GasIDREC", "PressuresIDREC", "Well Name", 
                     "Formation Producer", "Layer Producer", "Fault Block", 
                     "Pad Name", "Lateral Length", "Orient"]].copy()
    wells["key"] = 1
    days_df["key"] = 1
    spine = wells.merge(days_df, on="key").drop(columns=["key"])
    
    # Sort by Well Name first, then ProdDate
    spine = spine.sort_values(["Well Name", "ProdDate"]).reset_index(drop=True)
    
    print(f"Created complete spine with {len(spine):,} rows ({len(mapping)} wells × {len(days)} days)")
    return spine

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

def filter_to_first_production(df):
    """
    For each well, keep only rows from the first non-zero production data onward
    Uses Gas WH if available, otherwise falls back to Gathered Gas
    Matches VBA logic: If Gas WH <= 2, use Gathered Gas
    """
    print("\nFiltering to first production date for each well...")
    
    original_count = len(df)
    wells = df['Well Name'].unique()
    total_wells = len(wells)
    
    filtered_dfs = []
    wells_with_data = 0
    wells_without_data = 0
    
    for well_idx, well_name in enumerate(wells, 1):
        well_mask = df['Well Name'] == well_name
        well_data = df[well_mask].copy()
        
        # Sort by date to ensure chronological order
        well_data = well_data.sort_values('ProdDate')
        
        # Create effective Gas WH using VBA logic
        # If Gas WH <= 2, use Gathered Gas instead
        gas_wh = well_data['GasWH_Production'].fillna(0)
        gathered_gas = well_data['Gathered_Gas_Production'].fillna(0)
        
        # Apply VBA logic: whgtmp <= 2 -> use gathered gas
        effective_gas = gas_wh.copy()
        low_prod_mask = (gas_wh <= 2) & (gas_wh > 0)  # Gas WH between 0 and 2
        zero_mask = gas_wh == 0  # No Gas WH at all
        
        # For low production, use gathered gas
        effective_gas[low_prod_mask] = gathered_gas[low_prod_mask]
        # For zero production, use gathered gas if available
        effective_gas[zero_mask] = gathered_gas[zero_mask]
        
        # Find first row with non-zero effective production
        non_zero_indices = effective_gas[effective_gas > 0].index
        
        if len(non_zero_indices) > 0:
            # Get the first non-zero date
            first_production_idx = non_zero_indices[0]
            first_production_date = well_data.loc[first_production_idx, 'ProdDate']
            
            # Keep rows from that date onward
            well_filtered = well_data[well_data['ProdDate'] >= first_production_date].copy()
            
            # Apply the Gas WH replacement for all rows (VBA logic)
            for idx in well_filtered.index:
                gas_val = well_filtered.loc[idx, 'GasWH_Production']
                gathered_val = well_filtered.loc[idx, 'Gathered_Gas_Production']
                
                # VBA logic: If Gas WH <= 2, use Gathered Gas
                if pd.notna(gas_val) and gas_val <= 2 and gas_val > 0:
                    well_filtered.loc[idx, 'GasWH_Production'] = gathered_val
                elif pd.isna(gas_val) or gas_val == 0:
                    # If no Gas WH, use Gathered Gas
                    well_filtered.loc[idx, 'GasWH_Production'] = gathered_val
            
            filtered_dfs.append(well_filtered)
            wells_with_data += 1
            
            if well_idx % 50 == 0:
                print(f"    Processed {well_idx}/{total_wells} wells...")
        else:
            wells_without_data += 1
    
    if filtered_dfs:
        df_filtered = pd.concat(filtered_dfs, ignore_index=True)
        print(f"  Wells with production data: {wells_with_data}")
        print(f"  Wells with NO production data: {wells_without_data}")
        print(f"  Rows before filtering: {original_count:,}")
        print(f"  Rows after filtering: {len(df_filtered):,}")
        print(f"  Rows removed: {original_count - len(df_filtered):,} ({((original_count - len(df_filtered))/original_count*100):.1f}%)")
        return df_filtered
    else:
        print("  No wells with production data found!")
        return pd.DataFrame()

if __name__ == "__main__":
    start = "2009-01-01"
    # NEW: Fixed end date - January 31, 2026
    end = "2026-01-31"
    
    print("=" * 60)
    print(f"STARTING DATA PIPELINE - WITH VBA-STYLE FIRST PRODUCTION FILTER")
    print(f"Date range: {start} to {end}")
    print("=" * 60)
    
    # Step 1: Check SQL Server table exists and clear data
    print("\n[Step 1/9] Preparing SQL Server database...")
    print("  Verifying PCE_CDA table exists...")
    if not ensure_pce_cda_table():
        print("  ❌ Cannot proceed. Please create PCE_CDA table first.")
        exit(1)
    
    print("  Clearing existing data in range...")
    delete_pce_cda_range(start, end)
    
    # Step 2: Pull mapping data with all PCE_WM fields
    print("\n[Step 2/9] Pulling well mapping data from SQL Server...")
    mapping = pull_mapping()
    
    # Step 3: Pull all data from Snowflake
    print("\n[Step 3/9] Pulling data from Snowflake...")
    ecf = pull_ecf(start, end)
    gaswh = pull_gaswh(start, end)
    cgr = pull_cgr(start, end)
    wgr = pull_wgr(start, end)
    pressures = pull_pressures(start, end)
    alloc = pull_allocations(start, end)
    alloc_water = pull_alloc_water(start, end)
    print("  Snowflake data pull complete")
    
    # Step 4: Build complete spine (all wells × all days)
    print("\n[Step 4/9] Building complete data spine...")
    spine = build_complete_spine(mapping, start, end)
    
    # Step 5: Process wells one by one in order
    print("\n[Step 5/9] Processing wells in order (well-by-well, date-by-date)...")
    
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
    
    # Step 6: Combine all well results
    print("\n[Step 6/9] Combining all well data...")
    if all_results:
        joined = pd.concat(all_results, ignore_index=True)
        print(f"  Combined dataframe rows: {len(joined):,}")
    else:
        joined = pd.DataFrame()
        print("  No data to combine")
    
    # Step 7: Apply VBA-style first production filter
    if not joined.empty:
        joined = filter_to_first_production(joined)
    else:
        print("\nNo data to filter")
    
    # Step 8: Data validation
    print("\n[Step 8/9] Validating data...")
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
    print("\n[Step 9/9] Loading data into SQL Server...")
    if not joined.empty:
        print(f"  Inserting {len(joined):,} rows in well-first order...")
        insert_pce_cda_rows(joined)
    else:
        print("  No data to insert")
    
    # Final summary
    print("\n" + "=" * 60)
    print("PIPELINE COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Date range: {start} to {end}")
    print(f"Number of wells: {len(mapping)}")
    
    if not joined.empty:
        print(f"Total records loaded into SQL Server: {len(joined):,}")
        full_range_days = len(pd.date_range(start=start, end=end, freq='D'))
        estimated_full_rows = len(mapping) * full_range_days
        savings = estimated_full_rows - len(joined)
        savings_pct = (savings / estimated_full_rows * 100) if estimated_full_rows > 0 else 0
        print(f"Estimated rows if using full range: {estimated_full_rows:,}")
        print(f"Rows removed by first-production filter: {savings:,} ({savings_pct:.1f}%)")
    else:
        print("Total records loaded into SQL Server: 0")
    
    print(f"Destination: {SQL_SERVER}.{SQL_DATABASE}.PCE_CDA")
    print(f"Columns loaded: {len(joined.columns) if not joined.empty else 0}")
    print(f"Insert order: Well-by-well, date-by-date")
    print(f"PCE_WM fields added: Formation Producer, Layer Producer, Fault Block, Pad Name, Lateral Length, Orient")
    print(f"Filter method: VBA-style first production date (Gas WH ≤ 2 → Gathered Gas)")
    
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