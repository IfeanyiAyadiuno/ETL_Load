
import pandas as pd
import pyodbc
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
import os
import warnings
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

def clear_pce_production():
    """Clear all data from PCE_Production table"""
    print("\nClearing PCE_Production table...")
    with get_sql_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM PCE_Production")
        deleted = cursor.rowcount
        conn.commit()
        print(f"  Deleted {deleted:,} records")
        return deleted

def fetch_cda_data():
    """Fetch all daily production data from PCE_CDA ordered by well and date"""
    print("\nFetching data from PCE_CDA...")
    
    query = """
    SELECT 
        [Well Name] as [Source_Well_Name],
        ProdDate as [Date],
        [GasWH_Production] as [Gas WH Production (10³m³)],
        [Condensate_WH_Production] as [Condensate WH (m³/d)],
        [Gas - S2 Production] as [Gas S2 Production (10³m³)],
        [Gas - Sales Production] as [Gas Sales Production (10³m³)],
        [Condensate - Sales Production] as [Condensate Sales (m³/d)],
        [Gathered_Gas_Production] as [Gathered Gas (e³m³/d)],
        [Gathered_Condensate_Production] as [Gathered Condensate (m³/d)],
        [Sales CGR Ratio] as [Sales CGR (m³/e³m³)],
        [CGR_Ratio] as [CGR (m³/e³m³)],
        [WGR_Ratio] as [WGR (m³/e³m³)],
        [ECF_Ratio] as [ECF],
        [OnProdHours] as [Hours On],
        [TubingPressure] as [Tubing Pressure (kPa)],
        [CasingPressure] as [Casing Pressure (kPa)],
        [ChokeSize] as [Choke Size],
        [AllocatedWater_Rate] as [Alloc. Water Rate (m³)],
        [NGL_Production] as [NGL (m³)],
        [Formation Producer],
        [Layer Producer],
        [Fault Block],
        [Pad Name],
        [Lateral Length],
        [Orient] as [Orientation]
    FROM PCE_CDA
    ORDER BY [Well Name], ProdDate
    """
    
    with get_sql_conn() as conn:
        df = pd.read_sql(query, conn)
    
    print(f"  Fetched {len(df):,} rows from PCE_CDA")
    return df

def fetch_well_mapping():
    """Fetch well name mappings from PCE_WM (Composite Name and Well Name)"""
    print("\nFetching well name mappings from PCE_WM...")
    
    query = """
    SELECT 
        [Well Name] as SourceWell,
        [Composite Name],
        [Well Name] as FallbackWell
    FROM PCE_WM
    WHERE [Well Name] IS NOT NULL
    """
    
    with get_sql_conn() as conn:
        df = pd.read_sql(query, conn)
    
    # Create mapping dictionaries
    composite_map = {}
    fallback_map = {}
    
    for _, row in df.iterrows():
        # FIX: Add .strip() to remove trailing spaces from source well names
        source = str(row['SourceWell']).strip()
        composite = row['Composite Name']
        fallback = row['FallbackWell']
        
        # Store composite if it exists and is not empty
        if pd.notna(composite) and str(composite).strip():
            composite_map[source] = str(composite).strip()
        
        # Always store fallback (Well Name itself)
        fallback_map[source] = str(fallback).strip()
    
    print(f"  Loaded {len(composite_map)} composite name mappings")
    print(f"  Loaded {len(fallback_map)} fallback name mappings")
    
    return composite_map, fallback_map

def apply_well_names(df, composite_map, fallback_map):
    """
    Apply well name mapping: use Composite Name if available, otherwise use Well Name
    """
    print("\nApplying well name mappings...")
    
    original_count = len(df)
    
    # Track unmapped wells
    unmapped_sources = set()
    
    # Apply mapping
    def map_well_name(source_well):
        if pd.isna(source_well) or not str(source_well).strip():
            return None
        
        source = str(source_well).strip()
        
        # Try composite name first
        if source in composite_map:
            return composite_map[source]
        
        # Fall back to well name
        if source in fallback_map:
            return fallback_map[source]
        
        # No mapping found
        unmapped_sources.add(source)
        return None
    
    df['Well Name'] = df['Source_Well_Name'].apply(map_well_name)
    
    # Remove rows with no well name mapping
    unmapped_count = df['Well Name'].isna().sum()
    if unmapped_count > 0:
        print(f"  ⚠️ {unmapped_count:,} rows ({unmapped_count/original_count*100:.1f}%) have no well name mapping")
        
        # Show sample of unmapped wells
        if len(unmapped_sources) > 0:
            print(f"  Unmapped source wells: {', '.join(list(unmapped_sources)[:10])}")
            if len(unmapped_sources) > 10:
                print(f"    ... and {len(unmapped_sources) - 10} more")
        
        # Drop unmapped rows
        df = df.dropna(subset=['Well Name'])
        print(f"  → Remaining rows: {len(df):,}")
    
    # Drop the source column
    df = df.drop(columns=['Source_Well_Name'])
    
    print(f"  Well name mapping complete")
    return df

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
        
        # Create effective Gas WH using VBA logic
        # If Gas WH <= 2, use Gathered Gas instead
        gas_wh = well_data['Gas WH Production (10³m³)'].fillna(0)
        gathered_gas = well_data['Gathered Gas (e³m³/d)'].fillna(0)
        
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
            first_production_date = well_data.loc[first_production_idx, 'Date']
            
            # Keep rows from that date onward
            well_filtered = well_data[well_data['Date'] >= first_production_date].copy()
            
            # Apply the Gas WH replacement for all rows (VBA logic)
            for idx in well_filtered.index:
                gas_val = well_filtered.loc[idx, 'Gas WH Production (10³m³)']
                gathered_val = well_filtered.loc[idx, 'Gathered Gas (e³m³/d)']
                
                # VBA logic: If Gas WH <= 2, use Gathered Gas
                if pd.notna(gas_val) and gas_val <= 2 and gas_val > 0:
                    well_filtered.loc[idx, 'Gas WH Production (10³m³)'] = gathered_val
                elif pd.isna(gas_val) or gas_val == 0:
                    # If no Gas WH, use Gathered Gas
                    well_filtered.loc[idx, 'Gas WH Production (10³m³)'] = gathered_val
            
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

def calculate_sequences(df):
    """
    Calculate Days Seq and Day Seq UPRT for each well
    Matches VBA logic:
    - Days Seq: simple counter that resets per well
    - Day Seq UPRT: stays same when production <= 0, increments otherwise
    """
    print("\nCalculating sequence columns...")
    
    df['Days Seq'] = 0
    df['Day Seq UPRT'] = 0
    
    wells = df['Well Name'].unique()
    total_wells = len(wells)
    
    for well_idx, well_name in enumerate(wells, 1):
        well_mask = df['Well Name'] == well_name
        well_indices = df[well_mask].index
        
        # Days Seq: simple counter starting at 1 (VBA: seq = seq + 1)
        df.loc[well_indices, 'Days Seq'] = range(1, len(well_indices) + 1)
        
        # Day Seq UPRT: VBA logic - nozeroseq = IIf(vdpr <= 0, nozeroseq, nozeroseq + 1)
        gas_wh = df.loc[well_indices, 'Gas WH Production (10³m³)'].fillna(0).values
        seq_uprt = []
        counter = 1
        
        for val in gas_wh:
            if val > 0:
                seq_uprt.append(counter)
                counter += 1
            else:
                seq_uprt.append(counter - 1 if counter > 1 else 1)
        
        df.loc[well_indices, 'Day Seq UPRT'] = seq_uprt
        
        if well_idx % 50 == 0:
            print(f"    Processed {well_idx}/{total_wells} wells...")
    
    return df

def calculate_cumulatives(df):
    """
    Calculate cumulative totals for each well (optimized)
    """
    print("\nCalculating cumulative totals...")
    
    # List of source columns and their cumulative target columns
    cumulatives = [
        ('Gas WH Production (10³m³)', 'Gas WH Cumulative Production (10³m³)'),
        ('Gas S2 Production (10³m³)', 'Gas S2 Cumulative Production (10³m³)'),
        ('Gas Sales Production (10³m³)', 'Gas Sales Cumulative Production (10³m³)'),
        ('Condensate Sales (m³/d)', 'Condensate Sales Cumulative Production (m³)'),
        ('Condensate WH (m³/d)', 'Condensate WH Cumulative Production (m³)'),
        ('Gathered Gas (e³m³/d)', 'Gas Gathered Cumulative (e³m³)'),
        ('Gathered Condensate (m³/d)', 'Condensate Gathered Cumulative (m³)')
    ]
    
    # Initialize cumulative columns
    for _, cum_col in cumulatives:
        df[cum_col] = 0.0
    
    # Process each well
    wells = df['Well Name'].unique()
    total_wells = len(wells)
    
    for well_idx, well_name in enumerate(wells, 1):
        well_mask = df['Well Name'] == well_name
        well_indices = df[well_mask].index
        
        # Process all cumulative columns at once for this well
        for source_col, target_col in cumulatives:
            # Get values, convert to float, fill NA with 0
            values = pd.to_numeric(df.loc[well_indices, source_col], errors='coerce').fillna(0)
            df.loc[well_indices, target_col] = values.cumsum().values
        
        # Show progress every 50 wells
        if well_idx % 50 == 0:
            print(f"    Processed {well_idx}/{total_wells} wells for cumulatives...")
    
    print("  Cumulative calculations complete")
    return df

def calculate_monthly_averages(df):
    """
    Calculate monthly averages for each well with progress tracking
    """
    print("\nCalculating monthly averages...")
    
    # Create year-month column for grouping
    df['YearMonth'] = pd.to_datetime(df['Date']).dt.to_period('M')
    
    # List of columns to calculate monthly averages for
    monthly_avgs = [
        ('Gas WH Production (10³m³)', 'Gas WH Avg (10³m³)'),
        ('Gas S2 Production (10³m³)', 'Gas S2 Avg (10³m³)'),
        ('Gathered Gas (e³m³/d)', 'Gas Gathered Avg (e³m³/d)'),
        ('Gathered Condensate (m³/d)', 'Condensate Gathered Avg (m³/d)')
    ]
    
    # Initialize average columns
    for _, avg_col in monthly_avgs:
        df[avg_col] = 0.0
    
    # Get unique wells
    wells = df['Well Name'].unique()
    total_wells = len(wells)
    print(f"  Processing {total_wells} wells...")
    
    # Track progress
    total_months_processed = 0
    
    # Calculate monthly averages per well
    for well_idx, well_name in enumerate(wells, 1):
        well_mask = df['Well Name'] == well_name
        well_data = df[well_mask].copy()
        
        for month in well_data['YearMonth'].unique():
            month_mask = (df['Well Name'] == well_name) & (df['YearMonth'] == month)
            month_indices = df[month_mask].index
            
            for source_col, avg_col in monthly_avgs:
                month_values = df.loc[month_indices, source_col].fillna(0)
                if len(month_values) > 0:
                    monthly_avg = month_values.mean()
                    df.loc[month_indices, avg_col] = monthly_avg
            
            total_months_processed += 1
        
        # Show progress every 25 wells
        if well_idx % 25 == 0 or well_idx == total_wells:
            pct = (well_idx / total_wells) * 100
            print(f"    Progress: {well_idx}/{total_wells} wells ({pct:.1f}%) - {total_months_processed:,} months")
    
    # Drop the temporary YearMonth column
    df = df.drop(columns=['YearMonth'])
    
    print(f"  ✅ Monthly average calculations complete")
    return df

def add_on_production_year(df):
    """
    Add On Production Year column (year of first production date for each well)
    """
    print("\nAdding On Production Year...")
    
    df['On Production Year'] = 0
    
    wells = df['Well Name'].unique()
    total_wells = len(wells)
    
    for well_idx, well_name in enumerate(wells, 1):
        well_mask = df['Well Name'] == well_name
        first_date = pd.to_datetime(df.loc[well_mask, 'Date'].min())
        df.loc[well_mask, 'On Production Year'] = first_date.year
        
        # Show progress every 50 wells
        if well_idx % 50 == 0:
            print(f"    Processed {well_idx}/{total_wells} wells...")
    
    print("  On Production Year added")
    return df

def insert_pce_production(df):
    """
    Insert dataframe into PCE_Production table
    """
    if df.empty:
        print("  No rows to insert")
        return 0
    
    print(f"\nInserting {len(df):,} rows into PCE_Production...")
    
    # Define the insert SQL with 39 parameters
    insert_sql = """
    INSERT INTO PCE_Production (
        [Date], [Days Seq], [Day Seq UPRT], [Well Name],
        [Gas WH Production (10³m³)], [Condensate WH (m³/d)],
        [Gas S2 Production (10³m³)], [Gas Sales Production (10³m³)],
        [Condensate Sales (m³/d)], [Gathered Gas (e³m³/d)],
        [Gathered Condensate (m³/d)], [Sales CGR (m³/e³m³)],
        [CGR (m³/e³m³)], [WGR (m³/e³m³)], [ECF],
        [Hours On], [Tubing Pressure (kPa)], [Casing Pressure (kPa)],
        [Choke Size], [Gas WH Cumulative Production (10³m³)],
        [Gas S2 Cumulative Production (10³m³)],
        [Gas Sales Cumulative Production (10³m³)],
        [Condensate Sales Cumulative Production (m³)],
        [Condensate WH Cumulative Production (m³)],
        [Gas Gathered Cumulative (e³m³)],
        [Condensate Gathered Cumulative (m³)],
        [Formation Producer], [Layer Producer], [Fault Block],
        [Pad Name], [Lateral Length], [Orientation],
        [On Production Year], [Alloc. Water Rate (m³)], [NGL (m³)],
        [Gas WH Avg (10³m³)], [Gas S2 Avg (10³m³)],
        [Gas Gathered Avg (e³m³/d)], [Condensate Gathered Avg (m³/d)]
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    # Convert dataframe to list of tuples, replacing NaN with None
    rows_to_insert = []
    for _, row in df.iterrows():
        rows_to_insert.append((
            row['Date'],
            int(row['Days Seq']),
            int(row['Day Seq UPRT']),
            row['Well Name'],
            None if pd.isna(row['Gas WH Production (10³m³)']) else float(row['Gas WH Production (10³m³)']),
            None if pd.isna(row['Condensate WH (m³/d)']) else float(row['Condensate WH (m³/d)']),
            None if pd.isna(row['Gas S2 Production (10³m³)']) else float(row['Gas S2 Production (10³m³)']),
            None if pd.isna(row['Gas Sales Production (10³m³)']) else float(row['Gas Sales Production (10³m³)']),
            None if pd.isna(row['Condensate Sales (m³/d)']) else float(row['Condensate Sales (m³/d)']),
            None if pd.isna(row['Gathered Gas (e³m³/d)']) else float(row['Gathered Gas (e³m³/d)']),
            None if pd.isna(row['Gathered Condensate (m³/d)']) else float(row['Gathered Condensate (m³/d)']),
            None if pd.isna(row['Sales CGR (m³/e³m³)']) else float(row['Sales CGR (m³/e³m³)']),
            None if pd.isna(row['CGR (m³/e³m³)']) else float(row['CGR (m³/e³m³)']),
            None if pd.isna(row['WGR (m³/e³m³)']) else float(row['WGR (m³/e³m³)']),
            None if pd.isna(row['ECF']) else float(row['ECF']),
            None if pd.isna(row['Hours On']) else float(row['Hours On']),
            None if pd.isna(row['Tubing Pressure (kPa)']) else float(row['Tubing Pressure (kPa)']),
            None if pd.isna(row['Casing Pressure (kPa)']) else float(row['Casing Pressure (kPa)']),
            None if pd.isna(row['Choke Size']) else float(row['Choke Size']),
            None if pd.isna(row['Gas WH Cumulative Production (10³m³)']) else float(row['Gas WH Cumulative Production (10³m³)']),
            None if pd.isna(row['Gas S2 Cumulative Production (10³m³)']) else float(row['Gas S2 Cumulative Production (10³m³)']),
            None if pd.isna(row['Gas Sales Cumulative Production (10³m³)']) else float(row['Gas Sales Cumulative Production (10³m³)']),
            None if pd.isna(row['Condensate Sales Cumulative Production (m³)']) else float(row['Condensate Sales Cumulative Production (m³)']),
            None if pd.isna(row['Condensate WH Cumulative Production (m³)']) else float(row['Condensate WH Cumulative Production (m³)']),
            None if pd.isna(row['Gas Gathered Cumulative (e³m³)']) else float(row['Gas Gathered Cumulative (e³m³)']),
            None if pd.isna(row['Condensate Gathered Cumulative (m³)']) else float(row['Condensate Gathered Cumulative (m³)']),
            row['Formation Producer'],
            row['Layer Producer'],
            row['Fault Block'],
            row['Pad Name'],
            None if pd.isna(row['Lateral Length']) else float(row['Lateral Length']),
            row['Orientation'],
            int(row['On Production Year']) if pd.notna(row['On Production Year']) else None,
            None if pd.isna(row['Alloc. Water Rate (m³)']) else float(row['Alloc. Water Rate (m³)']),
            None if pd.isna(row['NGL (m³)']) else float(row['NGL (m³)']),
            None if pd.isna(row['Gas WH Avg (10³m³)']) else float(row['Gas WH Avg (10³m³)']),
            None if pd.isna(row['Gas S2 Avg (10³m³)']) else float(row['Gas S2 Avg (10³m³)']),
            None if pd.isna(row['Gas Gathered Avg (e³m³/d)']) else float(row['Gas Gathered Avg (e³m³/d)']),
            None if pd.isna(row['Condensate Gathered Avg (m³/d)']) else float(row['Condensate Gathered Avg (m³/d)'])
        ))
    
    # Insert in batches
    batch_size = 1000
    total_inserted = 0
    duplicate_skipped = 0
    
    with get_sql_conn() as conn:
        cursor = conn.cursor()
        
        for i in range(0, len(rows_to_insert), batch_size):
            batch = rows_to_insert[i:i + batch_size]
            
            for j, row in enumerate(batch):
                try:
                    cursor.execute(insert_sql, row)
                    conn.commit()
                    total_inserted += 1
                except Exception as row_e:
                    if "Violation of UNIQUE KEY" in str(row_e):
                        duplicate_skipped += 1
                        if duplicate_skipped <= 5:  # Show first 5 duplicates
                            print(f"      ⚠️ Duplicate skipped at position {i+j}")
                    else:
                        print(f"      ❌ Error at position {i+j}: {row_e}")
            
            if (i + batch_size) % 5000 == 0 or (i + batch_size) >= len(rows_to_insert):
                print(f"    Progress: {min(i + batch_size, len(rows_to_insert)):,} rows processed...")
    
    print(f"  ✅ Successfully inserted {total_inserted:,} rows")
    if duplicate_skipped > 0:
        print(f"  ⚠️ Skipped {duplicate_skipped:,} duplicate rows")
    
    return total_inserted

def main():
    print("=" * 80)
    print("PCE_PRODUCTION POPULATION SCRIPT")
    print("=" * 80)
    
    # Step 1: Clear existing data
    clear_pce_production()
    
    # Step 2: Fetch well name mappings
    composite_map, fallback_map = fetch_well_mapping()
    
    # Step 3: Fetch CDA data
    df = fetch_cda_data()
    
    if df.empty:
        print("No data to process. Exiting.")
        return
    
    # Step 4: Apply well name mappings (composite name with fallback to well name)
    df = apply_well_names(df, composite_map, fallback_map)
    
    if df.empty:
        print("No data after well name mapping. Exiting.")
        return
    
    # Step 5: Filter to first production date for each well
    df = filter_to_first_production(df)
    
    if df.empty:
        print("No data after filtering. Exiting.")
        return
    
    # Step 6: Calculate sequences with corrected Day Seq UPRT logic
    df = calculate_sequences(df)
    
    # Step 7: Calculate cumulatives
    df = calculate_cumulatives(df)
    
    # Step 8: Calculate monthly averages
    df = calculate_monthly_averages(df)
    
    # Step 9: Add On Production Year
    df = add_on_production_year(df)
    
    # Step 10: Insert into PCE_Production
    rows_inserted = insert_pce_production(df)
    
    # Step 11: Final summary
    print("\n" + "=" * 80)
    print("POPULATION SUMMARY")
    print("=" * 80)
    print(f"Wells processed: {len(df['Well Name'].unique()):,}")
    print(f"Total records: {len(df):,}")
    print(f"Records inserted: {rows_inserted:,}")
    print(f"Columns populated: {len(df.columns)}")
    print("=" * 80)

if __name__ == "__main__":
    main()