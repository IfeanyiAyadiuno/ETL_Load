import pandas as pd
import pyodbc
import numpy as np
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

def fetch_existing_data():
    """Fetch only the data needed for Day Seq UPRT recalculation"""
    print("\nFetching existing data from PCE_Production...")
    
    query = """
    SELECT 
        [Well Name],
        [Date],
        [Gas WH Production (10³m³)],
        [Days Seq]  -- We'll keep this for ordering, but not modify it
    FROM PCE_Production
    ORDER BY [Well Name], [Date]
    """
    
    with get_sql_conn() as conn:
        df = pd.read_sql(query, conn)
    
    print(f"  Fetched {len(df):,} rows from PCE_Production")
    return df

def recalculate_day_seq_uprt(df):
    """
    Recalculate Day Seq UPRT with corrected logic
    """
    print("\nRecalculating Day Seq UPRT...")
    
    # Initialize the column with zeros
    df['New_Day_Seq_UPRT'] = 0
    
    wells = df['Well Name'].unique()
    total_wells = len(wells)
    print(f"  Processing {total_wells} wells...")
    
    for well_idx, well_name in enumerate(wells, 1):
        well_mask = df['Well Name'] == well_name
        well_indices = df[well_mask].index
        
        # Get gas WH values
        gas_wh = df.loc[well_indices, 'Gas WH Production (10³m³)'].fillna(0).values
        new_seq = []
        
        counter = 1
        i = 0
        
        while i < len(gas_wh):
            if gas_wh[i] >= 1:
                # Normal day - use current counter and increment for next
                new_seq.append(counter)
                counter += 1
                i += 1
            else:
                # First low day - use the last good number (counter-1)
                last_good = counter - 1
                new_seq.append(last_good)
                i += 1
                
                # Continue with same number for consecutive low days
                while i < len(gas_wh) and gas_wh[i] < 1:
                    new_seq.append(last_good)
                    i += 1
                
                # After streak ends, counter stays where it was
                # No increment here - next good day will use current counter
        
        df.loc[well_indices, 'New_Day_Seq_UPRT'] = new_seq
        
        # Show progress every 25 wells
        if well_idx % 25 == 0 or well_idx == total_wells:
            print(f"    Processed {well_idx}/{total_wells} wells...")
    
    # Verify the changes with a sample
    print("\n  Sample of recalculated values (first 10 rows of first well):")
    first_well = wells[0]
    sample = df[df['Well Name'] == first_well].head(10)
    for _, row in sample.iterrows():
        print(f"    Date: {row['Date']}, Gas WH: {row['Gas WH Production (10³m³)']:.1f}, Old Seq: {row['Days Seq']}, New Seq: {row['New_Day_Seq_UPRT']}")
    
    return df

def update_pce_production(df):
    """
    Update only the Day Seq UPRT column in PCE_Production
    """
    print("\nUpdating Day Seq UPRT in PCE_Production...")
    
    # Update SQL
    update_sql = """
    UPDATE PCE_Production 
    SET [Day Seq UPRT] = ?
    WHERE [Well Name] = ? AND [Date] = ?
    """
    
    # Convert to list of tuples for batch update
    updates = []
    for _, row in df.iterrows():
        updates.append((
            int(row['New_Day_Seq_UPRT']),
            row['Well Name'],
            row['Date']
        ))
    
    print(f"  Preparing to update {len(updates):,} rows...")
    
    # Update in batches
    batch_size = 1000
    total_updated = 0
    
    with get_sql_conn() as conn:
        cursor = conn.cursor()
        
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i + batch_size]
            
            for j, (new_seq, well_name, date) in enumerate(batch):
                try:
                    cursor.execute(update_sql, new_seq, well_name, date)
                    conn.commit()
                    total_updated += 1
                except Exception as e:
                    print(f"      ❌ Error updating row {i+j}: {e}")
            
            if (i + batch_size) % 5000 == 0 or (i + batch_size) >= len(updates):
                print(f"    Progress: {min(i + batch_size, len(updates)):,} rows updated...")
    
    print(f"  ✅ Successfully updated {total_updated:,} rows")
    return total_updated

def verify_update():
    """Verify the update by showing sample before/after"""
    print("\nVerifying update with sample data...")
    
    query = """
    SELECT TOP 20
        [Well Name],
        [Date],
        [Gas WH Production (10³m³)],
        [Days Seq],
        [Day Seq UPRT]
    FROM PCE_Production
    WHERE [Well Name] = (SELECT TOP 1 [Well Name] FROM PCE_Production)
    ORDER BY [Date]
    """
    
    with get_sql_conn() as conn:
        df = pd.read_sql(query, conn)
    
    print("\nSample of updated data (first well, first 20 days):")
    print("-" * 80)
    for _, row in df.iterrows():
        gas_wh = row['Gas WH Production (10³m³)']
        status = "🔴 LOW" if gas_wh < 1 else "✓"
        print(f"  {row['Date']} | Gas: {gas_wh:6.1f} | Days Seq: {row['Days Seq']:2} | Day Seq UPRT: {row['Day Seq UPRT']:2} {status}")
    print("-" * 80)

def main():
    print("=" * 80)
    print("DAY SEQ UPRT CORRECTION SCRIPT")
    print("=" * 80)
    print("This script will ONLY update the Day Seq UPRT column")
    print("All other data in PCE_Production remains unchanged")
    print("=" * 80)
    
    confirm = input("\nProceed with update? (Type 'YES' to continue): ")
    if confirm != 'YES':
        print("Update cancelled.")
        return
    
    # Step 1: Fetch existing data
    df = fetch_existing_data()
    
    if df.empty:
        print("No data found. Exiting.")
        return
    
    # Step 2: Recalculate Day Seq UPRT
    df = recalculate_day_seq_uprt(df)
    
    # Step 3: Update the database
    rows_updated = update_pce_production(df)
    
    # Step 4: Verify
    verify_update()
    
    # Step 5: Summary
    print("\n" + "=" * 80)
    print("UPDATE SUMMARY")
    print("=" * 80)
    print(f"Wells processed: {len(df['Well Name'].unique()):,}")
    print(f"Total rows updated: {rows_updated:,}")
    print(f"Columns modified: Day Seq UPRT only")
    print("=" * 80)

if __name__ == "__main__":
    main()