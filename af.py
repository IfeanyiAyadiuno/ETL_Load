import pandas as pd
import pyodbc
import time
import sys
from datetime import datetime
import traceback
import os

def allocation_factors_loader():
    """
    Load allocation factors from Excel to Access database.
    Each well has 9 columns: 5 gas columns, 3 condensate columns, 1 empty column.
    """
    
    print("\n" + "="*70)
    print("ALLOCATION FACTORS LOADER")
    print("="*70)
    
    # File paths
    excel_path = r"I:\ResEng\Tools\Programmers Paradise\mvp_cda_load\Book1.xlsx"
    db_path = r"I:\ResEng\Tools\Programmers Paradise\GUI_WM\PCE_WM1.accdb"
    
    print(f"Excel: {excel_path}")
    print(f"Database: {db_path}")
    
    print("\nIMPORTANT: Close Microsoft Access before continuing!")
    
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
        
        # Connect to database and clear existing data
        print("\nConnecting to database to clear existing data...")
        conn_str = (
            r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
            r'DBQ=' + db_path + ';'
        )
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        print("Clearing table...")
        cursor.execute("DELETE FROM Allocation_Factors")
        conn.commit()
        print("Table cleared.")
        
        # Find all wells in the Excel file
        print("\nFinding all wells...")
        wells = []
        
        # Scan through columns to find well names in row 3 (Excel row 4)
        col = 0
        while col < df.shape[1]:
            well_name = df.iloc[2, col]  # Row 3 has well names
            
            # Check if this cell contains a well name
            if pd.notna(well_name) and isinstance(well_name, str) and well_name.strip():
                clean_name = well_name.strip()
                
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
                            'name': clean_name,
                            # Gas columns (first 5 columns)
                            'col_gas_prodview': col,      # Prodview WH Gas
                            'col_gas_s1': col + 1,        # S1 Gas
                            'col_gas_wh_to_s1': col + 2,  # WH to S1 Allocation Factor
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
        
        print(f"\nFound {len(wells)} wells")
        
        # Get months data starting from row 5 (Excel row 6)
        print("\nProcessing months...")
        months = []
        data_start_row = 5  # Excel row 6
        
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
            
            months.append({
                'row': row,
                'date': month_date
            })
        
        print(f"Found {len(months)} months")
        
        # Prepare for data insertion
        print("\nProcessing and inserting data...")
        total_rows = len(wells) * len(months)
        print(f"Processing {len(wells)} wells × {len(months)} months = {total_rows:,} rows")
        
        total_inserted = 0
        errors = 0
        
        # Get metadata for database
        source_file = os.path.basename(excel_path)
        loaded_at = datetime.now()
        
        # SQL INSERT statement matching Access table structure
        INSERT_SQL = """
            INSERT INTO Allocation_Factors (
                MonthStartDate, WellName,
                Prodview_WH_Gas, S1_Gas, WH_to_S1_AllocFactor, 
                Sales_Gas, WH_to_Sales_AllocFactor,
                Prodview_WH_Cond, Sales_Cond, WH_to_Sales_Cond_AllocFactor,
                SourceFile, LoadedAt
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Process each well - all months for one well, then next well
        for well_idx, well in enumerate(wells):
            well_name = well['name']
            
            # Show progress for every well
            print(f"\nProcessing Well {well_idx+1}/{len(wells)}: {well_name}")
            
            well_data_points = 0
            
            # Process all months for this well
            for month_idx, month in enumerate(months):
                month_date = month['date']
                row_num = month['row']
                
                try:
                    # Get gas values from Excel
                    prodview_wh_gas = df.iloc[row_num, well['col_gas_prodview']]
                    s1_gas = df.iloc[row_num, well['col_gas_s1']]
                    wh_to_s1_gas = df.iloc[row_num, well['col_gas_wh_to_s1']]
                    sales_gas = df.iloc[row_num, well['col_gas_sales']]
                    wh_to_sales_gas = df.iloc[row_num, well['col_gas_wh_to_sales']]
                    
                    # Get condensate values from Excel
                    prodview_wh_cond = df.iloc[row_num, well['col_cond_prodview']]
                    sales_cond = df.iloc[row_num, well['col_cond_sales']]
                    wh_to_sales_cond = df.iloc[row_num, well['col_cond_wh_to_sales']]
                    
                    # Insert into database
                    cursor.execute(INSERT_SQL,
                        month_date,                    # MonthStartDate
                        well_name,                     # WellName
                        # Gas columns
                        float(prodview_wh_gas) if pd.notna(prodview_wh_gas) else 0,
                        float(s1_gas) if pd.notna(s1_gas) else 0,
                        float(wh_to_s1_gas) if pd.notna(wh_to_s1_gas) else 0,
                        float(sales_gas) if pd.notna(sales_gas) else 0,
                        float(wh_to_sales_gas) if pd.notna(wh_to_sales_gas) else 0,
                        # Condensate columns
                        float(prodview_wh_cond) if pd.notna(prodview_wh_cond) else 0,
                        float(sales_cond) if pd.notna(sales_cond) else 0,
                        float(wh_to_sales_cond) if pd.notna(wh_to_sales_cond) else 0,
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
            
            # Show completion for this well
            print(f"      Inserted {well_data_points} months for well '{well_name}'")
        
        # Final commit and close connection
        conn.commit()
        conn.close()
        
        total_time = time.time() - total_start
        
        print("\n" + "="*70)
        print("LOAD SUMMARY")
        print("="*70)
        print(f"   Total wells: {len(wells)}")
        print(f"   Total months: {len(months)}")
        print(f"   Successfully inserted: {total_inserted:,}")
        print(f"   Errors: {errors}")
        print(f"   Total time: {total_time:.1f} seconds")
        
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