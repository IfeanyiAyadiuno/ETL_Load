'''
import pandas as pd
import pyodbc
import time
import sys
from datetime import datetime, timedelta
import traceback
import os
import re

def get_well_name_mapping(db_path):
    """
    Connect to Access database and create mapping from Well Name_AF to Well Name
    Returns: dictionary {well_name_af: well_name}
    """
    print("\nLoading well name mapping from PCE_WM table...")
    
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        r'DBQ=' + db_path + ';'
    )
    
    try:
        conn = pyodbc.connect(conn_str)
        query = "SELECT [Well Name], [Well Name_AF] FROM PCE_WM WHERE [Well Name_AF] IS NOT NULL"
        df_mapping = pd.read_sql(query, conn)
        conn.close()
        
        # Create mapping dictionary
        mapping = {}
        for _, row in df_mapping.iterrows():
            well_name = row['Well Name']
            well_name_af = row['Well Name_AF']
            if pd.notna(well_name_af) and well_name_af.strip():
                mapping[well_name_af.strip()] = well_name.strip()
        
        print(f"Loaded {len(mapping)} well name mappings")
        
        # Show sample mappings
        sample_count = min(3, len(mapping))
        if sample_count > 0:
            print("\nSample mappings:")
            for i, (af_name, actual_name) in enumerate(list(mapping.items())[:sample_count]):
                print(f"  '{af_name}' → '{actual_name}'")
        
        return mapping
        
    except Exception as e:
        print(f"ERROR loading well name mapping: {e}")
        traceback.print_exc()
        return None

def transform_well_name_for_mapping(excel_name):
    """
    Transform Excel well names to try different formats for matching
    Returns a list of possible name variations to try
    """
    if not isinstance(excel_name, str):
        return [excel_name]
    
    variations = [excel_name]  # Start with original
    clean_name = excel_name.strip()
    
    # Pattern for wells with /94-B-8 that should be /94-A-5
    if '/94-B-8' in clean_name:
        # Replace with /94-A-5 as suggested
        var1 = clean_name.replace('/94-B-8', '/94-A-5')
        variations.append(var1)
        
        # Also try uppercase first letter with the replacement
        if clean_name[0].islower():
            var2 = clean_name[0].upper() + clean_name[1:]
            variations.append(var2)
            # Apply the replacement to uppercase version
            if '/94-B-8' in var2:
                variations.append(var2.replace('/94-B-8', '/94-A-5'))
    
    # Pattern for wells that might need leading zeros (e.g., c-D98-D/94-B-8)
    # This handles cases like c-D98-D/94-B-8 → C-D098-D/094-A-5
    pattern = r'^([a-zA-Z])-([A-Z])(\d+)-([A-Z])/(\d+)-([A-Z])-(\d+)$'
    match = re.match(pattern, clean_name)
    if match:
        prefix = match.group(1).upper()
        letter1 = match.group(2)
        num1 = match.group(3).zfill(3)  # Add leading zeros to 3 digits
        letter2 = match.group(4)
        num2 = match.group(5).zfill(3)   # Add leading zeros to 3 digits
        letter3 = match.group(6)
        num3 = match.group(7)  # Don't zfill this one if we want '5' not '05'
        
        # If the last part is '8', try with '5' for A-5 pattern
        if num3 == '8':
            # Create standardized format with A-5
            standardized = f"{prefix}-{letter1}{num1}-{letter2}/{num2}-A-5"
            variations.append(standardized)
            
            # Also try with original letter but 5
            if letter3 != 'A':
                variations.append(f"{prefix}-{letter1}{num1}-{letter2}/{num2}-{letter3}-5")
        else:
            # Standard format with original ending
            standardized = f"{prefix}-{letter1}{num1}-{letter2}/{num2}-{letter3}-{num3}"
            variations.append(standardized)
    
    # Also try direct replacement of B-8 with A-5 without other changes
    if clean_name.endswith('B-8'):
        base_name = clean_name[:-3]  # Remove 'B-8'
        variations.append(base_name + 'A-5')
    
    # Also try adding the 0 back in various combinations (since Access might have either)
    if '/94-A-5' in clean_name or '/94-A-5' in str(variations):
        # Also try with leading zero
        for i, var in enumerate(variations[:]):  # Use slice copy to avoid modifying while iterating
            if '/94-A-5' in var:
                variations.append(var.replace('/94-A-5', '/94-A-05'))
    
    # Remove duplicates while preserving order
    seen = set()
    unique_variations = []
    for var in variations:
        if var not in seen:
            seen.add(var)
            unique_variations.append(var)
    
    return unique_variations

def get_month_end(month_start):
    """Calculate month end date from month start"""
    if month_start.month == 12:
        return datetime(month_start.year + 1, 1, 1) - timedelta(days=1)
    else:
        return datetime(month_start.year, month_start.month + 1, 1) - timedelta(days=1)

def allocation_factors_loader():
    """
    Load allocation factors from Excel to Access database.
    Each well has 9 columns: 5 gas columns, 3 condensate columns, 1 empty column.
    Loads data ONLY UNTIL END OF AUGUST 2025.
    Maps well names using PCE_WM table (Well Name_AF → Well Name)
    Also populates:
    - Gathered_Gas_Production and Gathered_Condensate_Production from CDA_Table
    - Gathered_to_S2_Gas, Gathered_to_Sales, Gathered_to_Sales_Condensate (calculated ratios)
    """
    
    print("\n" + "="*70)
    print("ALLOCATION FACTORS LOADER (UNTIL AUGUST 2025)")
    print("="*70)
    
    # File paths
    excel_path = r"I:\ResEng\Tools\Programmers Paradise\mvp_cda_load\Book1.xlsx"
    db_path = r"I:\ResEng\Tools\Programmers Paradise\GUI_WM\PCE_WM1.accdb"
    
    print(f"Excel: {excel_path}")
    print(f"Database: {db_path}")
    
    # First, load the well name mapping from PCE_WM table
    well_name_mapping = get_well_name_mapping(db_path)
    if well_name_mapping is None:
        print("Failed to load well name mapping. Exiting.")
        return False
    
    print("\nIMPORTANT: This will DELETE ALL DATA and reload ONLY UNTIL END OF AUGUST 2025")
    print("IMPORTANT: Close Microsoft Access before continuing!")
    
    confirm = input("\nStart load? (Type 'GO' to confirm): ")
    if confirm.upper() != 'GO':
        print("Load cancelled.")
        return
    
    print("\n" + "="*70)
    print("STARTING LOAD...")
    print("="*70)
    
    total_start = time.time()
    
    try:
        # Read Excel file
        print("\nReading Excel...")
        start_time = time.time()
        df = pd.read_excel(excel_path, header=None)
        read_time = time.time() - start_time
        print(f"Read: {df.shape[0]} rows, {df.shape[1]} columns")
        print(f"Time: {read_time:.1f}s")
        
        # Connect to database and clear ALL existing data
        print("\nConnecting to database to clear ALL existing data...")
        conn_str = (
            r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
            r'DBQ=' + db_path + ';'
        )
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Get count of existing data before deletion
        cursor.execute("SELECT COUNT(*) FROM Allocation_Factors")
        existing_count = cursor.fetchone()[0]
        print(f"Found {existing_count:,} existing records in table")
        
        # Delete ALL data from table
        print("Deleting ALL data from Allocation_Factors table...")
        cursor.execute("DELETE FROM Allocation_Factors")
        conn.commit()
        print("All data deleted from table.")
        
        # Find all wells in the Excel file
        print("\nFinding all wells...")
        wells = []
        mapped_wells_count = 0
        transformed_mapped_count = 0
        unmapped_wells = []  # Track wells not found in mapping
        
        # Scan through columns to find well names in row 3 (Excel row 4)
        col = 0
        while col < df.shape[1]:
            excel_well_name = df.iloc[2, col]  # Row 3 has well names (Well Name_AF values)
            
            # Check if this cell contains a well name
            if pd.notna(excel_well_name) and isinstance(excel_well_name, str) and excel_well_name.strip():
                clean_excel_name = excel_well_name.strip()
                
                # Try to map to actual well name using PCE_WM lookup
                actual_well_name = None
                mapping_source = None
                
                # First try direct lookup
                if clean_excel_name in well_name_mapping:
                    actual_well_name = well_name_mapping[clean_excel_name]
                    mapping_source = "direct"
                    mapped_wells_count += 1
                else:
                    # Try transformed variations
                    variations = transform_well_name_for_mapping(clean_excel_name)
                    for var in variations[1:]:  # Skip first (original) as we already tried it
                        if var in well_name_mapping:
                            actual_well_name = well_name_mapping[var]
                            mapping_source = f"transformed: {var}"
                            transformed_mapped_count += 1
                            print(f"  Mapped '{clean_excel_name}' → '{actual_well_name}' (via transformation: {var})")
                            break
                
                if actual_well_name is None:
                    actual_well_name = clean_excel_name  # Fallback to Excel name if not found
                    unmapped_wells.append(clean_excel_name)
                    print(f"  WARNING: No mapping found for '{clean_excel_name}' - using Excel name")
                
                # Validate the structure - check if next columns match expected pattern
                if col + 8 < df.shape[1]:
                    # Check column at offset 4 should be 'WH to Sales' with 'Allocation Factor'
                    col4_type = df.iloc[3, col + 4]
                    col4_cat = df.iloc[4, col + 4]
                    
                    # Check column at offset 5 should be 'Prodview WH' with 'Condensate'
                    col5_type = df.iloc[3, col + 5]
                    col5_cat = df.iloc[4, col + 5]
                    
                    # Validate the pattern matches expected well structure
                    if (str(col4_type) == 'WH to Sales' and str(col4_cat) == 'Allocation Factor' and
                        str(col5_type) == 'Prodview WH' and str(col5_cat) == 'Condensate'):
                        
                        # Add well with all column mappings
                        wells.append({
                            'excel_name': clean_excel_name,  # Original name from Excel (for debugging)
                            'name': actual_well_name,        # Actual well name to store in DB
                            # Gas columns (first 5 columns)
                            'col_gas_prodview': col,      # Prodview WH Gas
                            'col_gas_s1': col + 1,        # S2 Gas
                            'col_gas_wh_to_s1': col + 2,  # WH to S2 Allocation Factor
                            'col_gas_sales': col + 3,     # Sales Gas
                            'col_gas_wh_to_sales': col + 4, # WH to Sales Allocation Factor
                            # Condensate columns (next 3 columns)
                            'col_cond_prodview': col + 5, # Prodview WH Condensate
                            'col_cond_sales': col + 6,    # Sales Condensate
                            'col_cond_wh_to_sales': col + 7 # WH to Sales Condensate Allocation Factor
                        })
                        
                        # Skip 9 columns (5 gas + 3 condensate + 1 empty) for next well
                        col += 9
                        continue
            
            # Move to next column if no well found here
            col += 1
        
        print(f"\nFound {len(wells)} wells in Excel")
        print(f"  - Directly mapped: {mapped_wells_count}")
        print(f"  - Mapped via transformation: {transformed_mapped_count}")
        
        if unmapped_wells:
            print(f"\nWARNING: {len(unmapped_wells)} wells had no mapping in PCE_WM table:")
            for w in unmapped_wells[:10]:  # Show first 10 unmapped wells
                print(f"  - '{w}'")
            if len(unmapped_wells) > 10:
                print(f"  ... and {len(unmapped_wells) - 10} more")
        
        # Get months data starting from row 5 (Excel row 6)
        print("\nProcessing months...")
        months = []
        data_start_row = 5  # Excel row 6
        
        # Define the cutoff date - end of AUGUST 2025
        cutoff_date = datetime(2025, 8, 31)
        print(f"Cutoff date: {cutoff_date.strftime('%B %d, %Y')}")
        
        months_loaded = 0
        months_skipped = 0
        
        for row in range(data_start_row, df.shape[0]):
            month_value = df.iloc[row, 0]  # Column A has dates
            if pd.isna(month_value):
                break
            
            # Convert to datetime
            if isinstance(month_value, datetime):
                month_date = month_value
            else:
                try:
                    month_date = pd.to_datetime(month_value)
                except:
                    continue
            
            # Check if month is BEFORE OR EQUAL to AUGUST 2025
            month_year = month_date.year
            month_month = month_date.month
            
            if month_year < 2025 or (month_year == 2025 and month_month <= 8):
                months.append({
                    'row': row,
                    'date': month_date
                })
                months_loaded += 1
            else:
                months_skipped += 1
                if months_skipped <= 5:
                    print(f"  Skipping {month_date.strftime('%B %Y')} - after cutoff")
        
        print(f"\nLoaded {months_loaded} months (up to August 2025)")
        if months_skipped > 0:
            print(f"Skipped {months_skipped} months (after August 2025)")
        
        # Prepare for data insertion
        print("\nProcessing and inserting data...")
        total_rows = len(wells) * len(months)
        print(f"Processing {len(wells)} wells × {len(months)} months = {total_rows:,} rows")
        
        total_inserted = 0
        errors = 0
        wells_with_cda_data = 0
        wells_without_cda_data = 0
        
        # Get metadata for database
        source_file = os.path.basename(excel_path)
        loaded_at = datetime.now()
        
        # SQL INSERT statement matching Access table structure with all fields
        INSERT_SQL = """
            INSERT INTO Allocation_Factors (
                MonthStartDate, WellName,
                Prodview_WH_Gas, S2_Gas, WH_to_S2_AllocFactor, 
                Sales_Gas, WH_to_Sales_AllocFactor,
                Prodview_WH_Cond, Sales_Condensate, WH_to_Sales_Cond_AllocFactor,
                Gathered_Gas_Production, Gathered_Condensate_Production,
                Gathered_to_S2_Gas, Gathered_to_Sales, Gathered_to_Sales_Condensate,
                SourceFile, LoadedAt
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Process each well - all months for one well, then next well
        for well_idx, well in enumerate(wells):
            well_name = well['name']
            excel_name = well['excel_name']
            
            # Show progress for every well
            if excel_name != well_name:
                print(f"\nProcessing Well {well_idx+1}/{len(wells)}: '{excel_name}' → '{well_name}'")
            else:
                print(f"\nProcessing Well {well_idx+1}/{len(wells)}: {well_name}")
            
            well_data_points = 0
            well_cda_count = 0
            
            # Process all months for this well
            for month_idx, month in enumerate(months):
                month_date = month['date']
                row_num = month['row']
                
                # Double-check cutoff (safety check)
                month_year = month_date.year
                month_month = month_date.month
                
                if month_year > 2025 or (month_year == 2025 and month_month > 8):
                    continue
                
                try:
                    # Get gas values from Excel
                    prodview_wh_gas = df.iloc[row_num, well['col_gas_prodview']]
                    s2_gas = df.iloc[row_num, well['col_gas_s1']]  # S2_Gas
                    wh_to_s2 = df.iloc[row_num, well['col_gas_wh_to_s1']]
                    sales_gas = df.iloc[row_num, well['col_gas_sales']]
                    wh_to_sales_gas = df.iloc[row_num, well['col_gas_wh_to_sales']]
                    
                    # Get condensate values from Excel
                    prodview_wh_cond = df.iloc[row_num, well['col_cond_prodview']]
                    sales_cond = df.iloc[row_num, well['col_cond_sales']]
                    wh_to_sales_cond = df.iloc[row_num, well['col_cond_wh_to_sales']]
                    
                    # Convert to float with proper null handling
                    prodview_wh_gas_val = float(prodview_wh_gas) if pd.notna(prodview_wh_gas) else 0
                    s2_gas_val = float(s2_gas) if pd.notna(s2_gas) else 0
                    wh_to_s2_val = float(wh_to_s2) if pd.notna(wh_to_s2) else 0
                    sales_gas_val = float(sales_gas) if pd.notna(sales_gas) else 0
                    wh_to_sales_gas_val = float(wh_to_sales_gas) if pd.notna(wh_to_sales_gas) else 0
                    prodview_wh_cond_val = float(prodview_wh_cond) if pd.notna(prodview_wh_cond) else 0
                    sales_cond_val = float(sales_cond) if pd.notna(sales_cond) else 0
                    wh_to_sales_cond_val = float(wh_to_sales_cond) if pd.notna(wh_to_sales_cond) else 0
                    
                    # -----------------------------------------------------------------
                    # QUERY CDA_TABLE FOR GATHERED PRODUCTION VALUES
                    # -----------------------------------------------------------------
                    # Calculate month end date
                    month_end = get_month_end(month_date)
                    
                    # Query CDA_Table for this well and month
                    cursor.execute("""
                        SELECT SUM(Gathered_Gas_Production) as TotalGatheredGas,
                               SUM(Gathered_Condensate_Production) as TotalGatheredCond
                        FROM CDA_Table 
                        WHERE [Well Name] = ? AND ProdDate BETWEEN ? AND ?
                    """, well_name, month_date.date(), month_end.date())
                    
                    result = cursor.fetchone()
                    gathered_gas_val = float(result[0]) if result and result[0] is not None else 0
                    gathered_cond_val = float(result[1]) if result and result[1] is not None else 0
                    
                    if gathered_gas_val > 0 or gathered_cond_val > 0:
                        well_cda_count += 1
                    
                    # -----------------------------------------------------------------
                    # CALCULATE GATHERED TO RATIOS
                    # -----------------------------------------------------------------
                    # Gathered_to_S2_Gas = S2_Gas / Gathered_Gas_Production
                    if gathered_gas_val > 0:
                        gathered_to_s2 = s2_gas_val / gathered_gas_val
                        gathered_to_s2_str = str(gathered_to_s2)
                    else:
                        gathered_to_s2_str = "1"  # When no gathered gas, ratio is 1 (100%)

                    # Gathered_to_Sales = Sales_Gas / Gathered_Gas_Production
                    if gathered_gas_val > 0:
                        gathered_to_sales = sales_gas_val / gathered_gas_val
                        gathered_to_sales_str = str(gathered_to_sales)
                    else:
                        gathered_to_sales_str = "1"  # When no gathered gas, ratio is 1 (100%)

                    # Gathered_to_Sales_Condensate = Sales_Condensate / Gathered_Condensate_Production
                    if gathered_cond_val > 0:
                        gathered_to_sales_cond = sales_cond_val / gathered_cond_val
                        gathered_to_sales_cond_str = str(gathered_to_sales_cond)
                    else:
                        gathered_to_sales_cond_str = "1"  # When no gathered condensate, ratio is 1 (100%)
                    
                    # Insert into database with all fields
                    cursor.execute(INSERT_SQL,
                        month_date,                    # MonthStartDate
                        well_name,                     # WellName (mapped from PCE_WM)
                        # Gas columns
                        prodview_wh_gas_val,
                        s2_gas_val,
                        wh_to_s2_val,
                        sales_gas_val,
                        wh_to_sales_gas_val,
                        # Condensate columns
                        prodview_wh_cond_val,
                        sales_cond_val,
                        wh_to_sales_cond_val,
                        # Gathered production from CDA_Table
                        gathered_gas_val,               # Gathered_Gas_Production
                        gathered_cond_val,               # Gathered_Condensate_Production
                        # Gathered to ratio text fields
                        gathered_to_s2_str,              # Gathered_to_S2_Gas
                        gathered_to_sales_str,           # Gathered_to_Sales
                        gathered_to_sales_cond_str,      # Gathered_to_Sales_Condensate
                        # Metadata
                        source_file,
                        loaded_at
                    )
                    
                    total_inserted += 1
                    well_data_points += 1
                    
                    # Commit every 5000 rows to avoid memory issues
                    if total_inserted % 5000 == 0:
                        conn.commit()
                        print(f"      Committed {total_inserted:,} rows...")
                    
                except Exception as e:
                    errors += 1
                    if errors <= 3:
                        print(f"      Error inserting {well_name} - {month_date}: {str(e)[:100]}")
            
            # Update counters
            if well_cda_count > 0:
                wells_with_cda_data += 1
            else:
                wells_without_cda_data += 1
            
            # Show completion for this well
            print(f"      Inserted {well_data_points} months (up to Aug 2025) for well '{well_name}'")
            if well_cda_count > 0:
                print(f"      Found CDA data for {well_cda_count} months")
        
        # Final commit and close connection
        conn.commit()
        conn.close()
        
        total_time = time.time() - total_start
        
        print("\n" + "="*70)
        print("LOAD SUMMARY")
        print("="*70)
        print(f"   Total wells in Excel: {len(wells)}")
        print(f"   Wells directly mapped: {mapped_wells_count}")
        print(f"   Wells mapped via transformation: {transformed_mapped_count}")
        print(f"   Wells without mapping: {len(unmapped_wells)}")
        print(f"   Wells with CDA data (at least one month): {wells_with_cda_data}")
        print(f"   Wells without any CDA data: {wells_without_cda_data}")
        print(f"   Months loaded: {len(months)} (up to August 2025)")
        print(f"   Months skipped: {months_skipped} (after August 2025)")
        print(f"   Previous records deleted: {existing_count:,}")
        print(f"   New records inserted: {total_inserted:,}")
        print(f"   Errors: {errors}")
        print(f"   Total time: {total_time:.1f} seconds")
        
        if unmapped_wells:
            print(f"\nWARNING: {len(unmapped_wells)} wells had no mapping and used Excel names.")
            print("         Check PCE_WM table for missing Well Name_AF entries.")
            print("\nUnmapped wells:")
            for w in sorted(unmapped_wells)[:20]:
                print(f"  - {w}")
            if len(unmapped_wells) > 20:
                print(f"  ... and {len(unmapped_wells) - 20} more")
        
        if months_skipped > 0:
            print(f"\nNOTE: {months_skipped} months after August 2025 were NOT loaded.")
            print("      Use the monthly update script for months after August 2025.")
        
        if errors > 0:
            print(f"\nWarning: {errors} errors occurred")
        
        print("\nLOAD COMPLETE!")
        print("="*70)
        
    except Exception as e:
        print(f"\nERROR: {e}")
        print("Full traceback:")
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    allocation_factors_loader()
    
    print("\nPress Enter to exit...")
    input()
'''

import pandas as pd
import pyodbc
import time
import sys
from datetime import datetime, timedelta
import traceback
import os
import re
from dotenv import load_dotenv

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

def get_well_name_mapping():
    """
    Connect to SQL Server and create mapping from Well Name_AF to Well Name
    Returns: dictionary {well_name_af: well_name}
    """
    print("\nLoading well name mapping from PCE_WM table...")
    
    try:
        conn = get_sql_conn()
        query = "SELECT [Well Name], [Well Name_AF] FROM PCE_WM WHERE [Well Name_AF] IS NOT NULL"
        df_mapping = pd.read_sql(query, conn)
        conn.close()
        
        # Create mapping dictionary
        mapping = {}
        for _, row in df_mapping.iterrows():
            well_name = row['Well Name']
            well_name_af = row['Well Name_AF']
            if pd.notna(well_name_af) and well_name_af.strip():
                mapping[well_name_af.strip()] = well_name.strip()
        
        print(f"Loaded {len(mapping)} well name mappings")
        
        # Show sample mappings
        sample_count = min(3, len(mapping))
        if sample_count > 0:
            print("\nSample mappings:")
            for i, (af_name, actual_name) in enumerate(list(mapping.items())[:sample_count]):
                print(f"  '{af_name}' → '{actual_name}'")
        
        return mapping
        
    except Exception as e:
        print(f"ERROR loading well name mapping: {e}")
        traceback.print_exc()
        return None

def transform_well_name_for_mapping(excel_name):
    """
    Transform Excel well names to try different formats for matching
    Returns a list of possible name variations to try
    """
    if not isinstance(excel_name, str):
        return [excel_name]
    
    variations = [excel_name]  # Start with original
    clean_name = excel_name.strip()
    
    # Pattern for wells with /94-B-8 that should be /94-A-5
    if '/94-B-8' in clean_name:
        # Replace with /94-A-5 as suggested
        var1 = clean_name.replace('/94-B-8', '/94-A-5')
        variations.append(var1)
        
        # Also try uppercase first letter with the replacement
        if clean_name[0].islower():
            var2 = clean_name[0].upper() + clean_name[1:]
            variations.append(var2)
            # Apply the replacement to uppercase version
            if '/94-B-8' in var2:
                variations.append(var2.replace('/94-B-8', '/94-A-5'))
    
    # Pattern for wells that might need leading zeros (e.g., c-D98-D/94-B-8)
    # This handles cases like c-D98-D/94-B-8 → C-D098-D/094-A-5
    pattern = r'^([a-zA-Z])-([A-Z])(\d+)-([A-Z])/(\d+)-([A-Z])-(\d+)$'
    match = re.match(pattern, clean_name)
    if match:
        prefix = match.group(1).upper()
        letter1 = match.group(2)
        num1 = match.group(3).zfill(3)  # Add leading zeros to 3 digits
        letter2 = match.group(4)
        num2 = match.group(5).zfill(3)   # Add leading zeros to 3 digits
        letter3 = match.group(6)
        num3 = match.group(7)  # Don't zfill this one if we want '5' not '05'
        
        # If the last part is '8', try with '5' for A-5 pattern
        if num3 == '8':
            # Create standardized format with A-5
            standardized = f"{prefix}-{letter1}{num1}-{letter2}/{num2}-A-5"
            variations.append(standardized)
            
            # Also try with original letter but 5
            if letter3 != 'A':
                variations.append(f"{prefix}-{letter1}{num1}-{letter2}/{num2}-{letter3}-5")
        else:
            # Standard format with original ending
            standardized = f"{prefix}-{letter1}{num1}-{letter2}/{num2}-{letter3}-{num3}"
            variations.append(standardized)
    
    # Also try direct replacement of B-8 with A-5 without other changes
    if clean_name.endswith('B-8'):
        base_name = clean_name[:-3]  # Remove 'B-8'
        variations.append(base_name + 'A-5')
    
    # Also try adding the 0 back in various combinations (since Access might have either)
    if '/94-A-5' in clean_name or '/94-A-5' in str(variations):
        # Also try with leading zero
        for i, var in enumerate(variations[:]):  # Use slice copy to avoid modifying while iterating
            if '/94-A-5' in var:
                variations.append(var.replace('/94-A-5', '/94-A-05'))
    
    # Remove duplicates while preserving order
    seen = set()
    unique_variations = []
    for var in variations:
        if var not in seen:
            seen.add(var)
            unique_variations.append(var)
    
    return unique_variations

def get_month_end(month_start):
    """Calculate month end date from month start"""
    if month_start.month == 12:
        return datetime(month_start.year + 1, 1, 1) - timedelta(days=1)
    else:
        return datetime(month_start.year, month_start.month + 1, 1) - timedelta(days=1)

def get_cda_gathered_data(cursor, well_name, month_start, month_end):
    """Query SQL Server PCE_CDA for gathered production values"""
    cursor.execute("""
        SELECT SUM(Gathered_Gas_Production) as TotalGatheredGas,
               SUM(Gathered_Condensate_Production) as TotalGatheredCond
        FROM PCE_CDA 
        WHERE [Well Name] = ? AND ProdDate BETWEEN ? AND ?
    """, well_name, month_start.date(), month_end.date())
    
    result = cursor.fetchone()
    gathered_gas_val = float(result[0]) if result and result[0] is not None else 0
    gathered_cond_val = float(result[1]) if result and result[1] is not None else 0
    
    return gathered_gas_val, gathered_cond_val

def allocation_factors_loader():
    """
    Load allocation factors from Excel to SQL Server database.
    Each well has 9 columns: 5 gas columns, 3 condensate columns, 1 empty column.
    Loads data ONLY UNTIL END OF AUGUST 2025.
    Maps well names using PCE_WM table (Well Name_AF → Well Name)
    Also populates:
    - Gathered_Gas_Production and Gathered_Condensate_Production from PCE_CDA
    - Gathered_to_S2_Gas, Gathered_to_Sales, Gathered_to_Sales_Condensate (calculated ratios)
    """
    
    print("\n" + "="*70)
    print("ALLOCATION FACTORS LOADER - SQL SERVER VERSION")
    print("="*70)
    print("(UNTIL AUGUST 2025)")
    
    # File paths
    excel_path = r"I:\ResEng\Tools\Programmers Paradise\mvp_cda_load\Book1.xlsx"
    
    print(f"Excel: {excel_path}")
    print(f"SQL Server: {SQL_SERVER}.{SQL_DATABASE}")
    
    # First, load the well name mapping from PCE_WM table
    well_name_mapping = get_well_name_mapping()
    if well_name_mapping is None:
        print("Failed to load well name mapping. Exiting.")
        return False
    
    print("\nIMPORTANT: This will DELETE ALL DATA and reload ONLY UNTIL END OF AUGUST 2025")
    print("IMPORTANT: Close any applications connected to SQL Server before continuing!")
    
    confirm = input("\nStart load? (Type 'GO' to confirm): ")
    if confirm.upper() != 'GO':
        print("Load cancelled.")
        return
    
    print("\n" + "="*70)
    print("STARTING LOAD...")
    print("="*70)
    
    total_start = time.time()
    
    try:
        # Read Excel file
        print("\nReading Excel...")
        start_time = time.time()
        df = pd.read_excel(excel_path, header=None)
        read_time = time.time() - start_time
        print(f"Read: {df.shape[0]} rows, {df.shape[1]} columns")
        print(f"Time: {read_time:.1f}s")
        
        # Connect to SQL Server
        print("\nConnecting to SQL Server...")
        conn = get_sql_conn()
        cursor = conn.cursor()
        print("   Database connected successfully.")
        
        # Get count of existing data before deletion
        cursor.execute("SELECT COUNT(*) FROM Allocation_Factors")
        existing_count = cursor.fetchone()[0]
        print(f"Found {existing_count:,} existing records in table")
        
        # Delete ALL data from table
        print("Deleting ALL data from Allocation_Factors table...")
        cursor.execute("DELETE FROM Allocation_Factors")
        conn.commit()
        print("All data deleted from table.")
        
        # Find all wells in the Excel file
        print("\nFinding all wells...")
        wells = []
        mapped_wells_count = 0
        transformed_mapped_count = 0
        unmapped_wells = []  # Track wells not found in mapping
        
        # Scan through columns to find well names in row 3 (Excel row 4)
        col = 0
        while col < df.shape[1]:
            excel_well_name = df.iloc[2, col]  # Row 3 has well names (Well Name_AF values)
            
            # Check if this cell contains a well name
            if pd.notna(excel_well_name) and isinstance(excel_well_name, str) and excel_well_name.strip():
                clean_excel_name = excel_well_name.strip()
                
                # Try to map to actual well name using PCE_WM lookup
                actual_well_name = None
                mapping_source = None
                
                # First try direct lookup
                if clean_excel_name in well_name_mapping:
                    actual_well_name = well_name_mapping[clean_excel_name]
                    mapping_source = "direct"
                    mapped_wells_count += 1
                else:
                    # Try transformed variations
                    variations = transform_well_name_for_mapping(clean_excel_name)
                    for var in variations[1:]:  # Skip first (original) as we already tried it
                        if var in well_name_mapping:
                            actual_well_name = well_name_mapping[var]
                            mapping_source = f"transformed: {var}"
                            transformed_mapped_count += 1
                            print(f"  Mapped '{clean_excel_name}' → '{actual_well_name}' (via transformation: {var})")
                            break
                
                if actual_well_name is None:
                    actual_well_name = clean_excel_name  # Fallback to Excel name if not found
                    unmapped_wells.append(clean_excel_name)
                    print(f"  WARNING: No mapping found for '{clean_excel_name}' - using Excel name")
                
                # Validate the structure - check if next columns match expected pattern
                if col + 8 < df.shape[1]:
                    # Check column at offset 4 should be 'WH to Sales' with 'Allocation Factor'
                    col4_type = df.iloc[3, col + 4]
                    col4_cat = df.iloc[4, col + 4]
                    
                    # Check column at offset 5 should be 'Prodview WH' with 'Condensate'
                    col5_type = df.iloc[3, col + 5]
                    col5_cat = df.iloc[4, col + 5]
                    
                    # Validate the pattern matches expected well structure
                    if (str(col4_type) == 'WH to Sales' and str(col4_cat) == 'Allocation Factor' and
                        str(col5_type) == 'Prodview WH' and str(col5_cat) == 'Condensate'):
                        
                        # Add well with all column mappings
                        wells.append({
                            'excel_name': clean_excel_name,  # Original name from Excel (for debugging)
                            'name': actual_well_name,        # Actual well name to store in DB
                            # Gas columns (first 5 columns)
                            'col_gas_prodview': col,      # Prodview WH Gas
                            'col_gas_s1': col + 1,        # S2 Gas
                            'col_gas_wh_to_s1': col + 2,  # WH to S2 Allocation Factor
                            'col_gas_sales': col + 3,     # Sales Gas
                            'col_gas_wh_to_sales': col + 4, # WH to Sales Allocation Factor
                            # Condensate columns (next 3 columns)
                            'col_cond_prodview': col + 5, # Prodview WH Condensate
                            'col_cond_sales': col + 6,    # Sales Condensate
                            'col_cond_wh_to_sales': col + 7 # WH to Sales Condensate Allocation Factor
                        })
                        
                        # Skip 9 columns (5 gas + 3 condensate + 1 empty) for next well
                        col += 9
                        continue
            
            # Move to next column if no well found here
            col += 1
        
        print(f"\nFound {len(wells)} wells in Excel")
        print(f"  - Directly mapped: {mapped_wells_count}")
        print(f"  - Mapped via transformation: {transformed_mapped_count}")
        
        if unmapped_wells:
            print(f"\nWARNING: {len(unmapped_wells)} wells had no mapping in PCE_WM table:")
            for w in unmapped_wells[:10]:  # Show first 10 unmapped wells
                print(f"  - '{w}'")
            if len(unmapped_wells) > 10:
                print(f"  ... and {len(unmapped_wells) - 10} more")
        
        # Get months data starting from row 5 (Excel row 6)
        print("\nProcessing months...")
        months = []
        data_start_row = 5  # Excel row 6
        
        # Define the cutoff date - end of AUGUST 2025
        cutoff_date = datetime(2025, 8, 31)
        print(f"Cutoff date: {cutoff_date.strftime('%B %d, %Y')}")
        
        months_loaded = 0
        months_skipped = 0
        
        for row in range(data_start_row, df.shape[0]):
            month_value = df.iloc[row, 0]  # Column A has dates
            if pd.isna(month_value):
                break
            
            # Convert to datetime
            if isinstance(month_value, datetime):
                month_date = month_value
            else:
                try:
                    month_date = pd.to_datetime(month_value)
                except:
                    continue
            
            # Check if month is BEFORE OR EQUAL to AUGUST 2025
            month_year = month_date.year
            month_month = month_date.month
            
            if month_year < 2025 or (month_year == 2025 and month_month <= 8):
                months.append({
                    'row': row,
                    'date': month_date
                })
                months_loaded += 1
            else:
                months_skipped += 1
                if months_skipped <= 5:
                    print(f"  Skipping {month_date.strftime('%B %Y')} - after cutoff")
        
        print(f"\nLoaded {months_loaded} months (up to August 2025)")
        if months_skipped > 0:
            print(f"Skipped {months_skipped} months (after August 2025)")
        
        # Prepare for data insertion
        print("\nProcessing and inserting data...")
        total_rows = len(wells) * len(months)
        print(f"Processing {len(wells)} wells × {len(months)} months = {total_rows:,} rows")
        
        total_inserted = 0
        errors = 0
        wells_with_cda_data = 0
        wells_without_cda_data = 0
        
        # Get metadata for database
        source_file = os.path.basename(excel_path)
        loaded_at = datetime.now()
        
        # SQL INSERT statement matching SQL Server table structure
        INSERT_SQL = """
            INSERT INTO Allocation_Factors (
                MonthStartDate, [Well Name],
                Prodview_WH_Gas, S2_Gas, WH_to_S2_AllocFactor, 
                Sales_Gas, WH_to_Sales_AllocFactor,
                Prodview_WH_Cond, Sales_Condensate, WH_to_Sales_Cond_AllocFactor,
                Gathered_Gas_Production, Gathered_Condensate_Production,
                Gathered_to_S2_Gas, Gathered_to_Sales, Gathered_to_Sales_Condensate,
                SourceFile, LoadedAt
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Process each well - all months for one well, then next well
        for well_idx, well in enumerate(wells):
            well_name = well['name']
            excel_name = well['excel_name']
            
            # Show progress for every well
            if excel_name != well_name:
                print(f"\nProcessing Well {well_idx+1}/{len(wells)}: '{excel_name}' → '{well_name}'")
            else:
                print(f"\nProcessing Well {well_idx+1}/{len(wells)}: {well_name}")
            
            well_data_points = 0
            well_cda_count = 0
            
            # Process all months for this well
            for month_idx, month in enumerate(months):
                month_date = month['date']
                row_num = month['row']
                
                # Double-check cutoff (safety check)
                month_year = month_date.year
                month_month = month_date.month
                
                if month_year > 2025 or (month_year == 2025 and month_month > 8):
                    continue
                
                try:
                    # Get gas values from Excel
                    prodview_wh_gas = df.iloc[row_num, well['col_gas_prodview']]
                    s2_gas = df.iloc[row_num, well['col_gas_s1']]
                    wh_to_s2 = df.iloc[row_num, well['col_gas_wh_to_s1']]
                    sales_gas = df.iloc[row_num, well['col_gas_sales']]
                    wh_to_sales_gas = df.iloc[row_num, well['col_gas_wh_to_sales']]
                    
                    # Get condensate values from Excel
                    prodview_wh_cond = df.iloc[row_num, well['col_cond_prodview']]
                    sales_cond = df.iloc[row_num, well['col_cond_sales']]
                    wh_to_sales_cond = df.iloc[row_num, well['col_cond_wh_to_sales']]
                    
                    # Convert to float with proper null handling
                    prodview_wh_gas_val = float(prodview_wh_gas) if pd.notna(prodview_wh_gas) else 0
                    s2_gas_val = float(s2_gas) if pd.notna(s2_gas) else 0
                    wh_to_s2_val = float(wh_to_s2) if pd.notna(wh_to_s2) else 0
                    sales_gas_val = float(sales_gas) if pd.notna(sales_gas) else 0
                    wh_to_sales_gas_val = float(wh_to_sales_gas) if pd.notna(wh_to_sales_gas) else 0
                    prodview_wh_cond_val = float(prodview_wh_cond) if pd.notna(prodview_wh_cond) else 0
                    sales_cond_val = float(sales_cond) if pd.notna(sales_cond) else 0
                    wh_to_sales_cond_val = float(wh_to_sales_cond) if pd.notna(wh_to_sales_cond) else 0
                    
                    # -----------------------------------------------------------------
                    # QUERY PCE_CDA FOR GATHERED PRODUCTION VALUES
                    # -----------------------------------------------------------------
                    # Calculate month end date
                    month_end = get_month_end(month_date)
                    
                    # Query PCE_CDA for this well and month
                    gathered_gas_val, gathered_cond_val = get_cda_gathered_data(
                        cursor, well_name, month_date, month_end
                    )
                    
                    if gathered_gas_val > 0 or gathered_cond_val > 0:
                        well_cda_count += 1
                    
                    # -----------------------------------------------------------------
                    # CALCULATE GATHERED TO RATIOS
                    # -----------------------------------------------------------------
                    # Gathered_to_S2_Gas = S2_Gas / Gathered_Gas_Production
                    if gathered_gas_val > 0:
                        gathered_to_s2 = s2_gas_val / gathered_gas_val
                        gathered_to_s2_str = str(gathered_to_s2)
                    else:
                        gathered_to_s2_str = "1"  # When no gathered gas, ratio is 1 (100%)

                    # Gathered_to_Sales = Sales_Gas / Gathered_Gas_Production
                    if gathered_gas_val > 0:
                        gathered_to_sales = sales_gas_val / gathered_gas_val
                        gathered_to_sales_str = str(gathered_to_sales)
                    else:
                        gathered_to_sales_str = "1"

                    # Gathered_to_Sales_Condensate = Sales_Condensate / Gathered_Condensate_Production
                    if gathered_cond_val > 0:
                        gathered_to_sales_cond = sales_cond_val / gathered_cond_val
                        gathered_to_sales_cond_str = str(gathered_to_sales_cond)
                    else:
                        gathered_to_sales_cond_str = "1"
                    
                    # Insert into database with all fields
                    cursor.execute(INSERT_SQL,
                        month_date,                    # MonthStartDate
                        well_name,                     # Well Name
                        # Gas columns
                        prodview_wh_gas_val,
                        s2_gas_val,
                        wh_to_s2_val,
                        sales_gas_val,
                        wh_to_sales_gas_val,
                        # Condensate columns
                        prodview_wh_cond_val,
                        sales_cond_val,
                        wh_to_sales_cond_val,
                        # Gathered production from PCE_CDA
                        gathered_gas_val,
                        gathered_cond_val,
                        # Gathered to ratio text fields
                        gathered_to_s2_str,
                        gathered_to_sales_str,
                        gathered_to_sales_cond_str,
                        # Metadata
                        source_file,
                        loaded_at
                    )
                    
                    total_inserted += 1
                    well_data_points += 1
                    
                    # Commit every 5000 rows to avoid memory issues
                    if total_inserted % 5000 == 0:
                        conn.commit()
                        print(f"      Committed {total_inserted:,} rows...")
                    
                except Exception as e:
                    errors += 1
                    if errors <= 3:
                        print(f"      Error inserting {well_name} - {month_date}: {str(e)[:100]}")
            
            # Update counters
            if well_cda_count > 0:
                wells_with_cda_data += 1
            else:
                wells_without_cda_data += 1
            
            # Show completion for this well
            print(f"      Inserted {well_data_points} months (up to Aug 2025) for well '{well_name}'")
            if well_cda_count > 0:
                print(f"      Found CDA data for {well_cda_count} months")
        
        # Final commit and close connection
        conn.commit()
        conn.close()
        
        total_time = time.time() - total_start
        
        print("\n" + "="*70)
        print("LOAD SUMMARY")
        print("="*70)
        print(f"   Total wells in Excel: {len(wells)}")
        print(f"   Wells directly mapped: {mapped_wells_count}")
        print(f"   Wells mapped via transformation: {transformed_mapped_count}")
        print(f"   Wells without mapping: {len(unmapped_wells)}")
        print(f"   Wells with CDA data (at least one month): {wells_with_cda_data}")
        print(f"   Wells without any CDA data: {wells_without_cda_data}")
        print(f"   Months loaded: {len(months)} (up to August 2025)")
        print(f"   Months skipped: {months_skipped} (after August 2025)")
        print(f"   Previous records deleted: {existing_count:,}")
        print(f"   New records inserted: {total_inserted:,}")
        print(f"   Errors: {errors}")
        print(f"   Total time: {total_time:.1f} seconds")
        print(f"   Destination: {SQL_SERVER}.{SQL_DATABASE}.Allocation_Factors")
        
        if unmapped_wells:
            print(f"\nWARNING: {len(unmapped_wells)} wells had no mapping and used Excel names.")
            print("         Check PCE_WM table for missing Well Name_AF entries.")
            print("\nUnmapped wells:")
            for w in sorted(unmapped_wells)[:20]:
                print(f"  - {w}")
            if len(unmapped_wells) > 20:
                print(f"  ... and {len(unmapped_wells) - 20} more")
        
        if months_skipped > 0:
            print(f"\nNOTE: {months_skipped} months after August 2025 were NOT loaded.")
            print("      Use the monthly update script for months after August 2025.")
        
        if errors > 0:
            print(f"\nWarning: {errors} errors occurred")
        
        print("\nLOAD COMPLETE!")
        print("="*70)
        
    except Exception as e:
        print(f"\nERROR: {e}")
        print("Full traceback:")
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    allocation_factors_loader()
    
    print("\nPress Enter to exit...")
    input()