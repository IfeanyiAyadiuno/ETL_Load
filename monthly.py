
import pandas as pd
import pyodbc
import time
import sys
from datetime import datetime, date, timedelta
import os
import re
import traceback
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

def combined_monthly_loader():
    """
    Combined loader for ValNav and Public Data Accumap data for the same month.
    Loads all source fields and calculates allocation factors:
    - WH_to_S2_AllocFactor = S2_Gas / Prodview_WH_Gas (1 if denominator=0)
    - WH_to_Sales_AllocFactor = Sales_Gas / Prodview_WH_Gas (1 if denominator=0)
    - WH_to_Sales_Cond_AllocFactor = Sales_Cond / Prodview_WH_Cond (1 if denominator=0)
    
    Also calculates gathered to ratios:
    - Gathered_to_S2_Gas = S2_Gas / Gathered_Gas_Production (1 if denominator=0)
    - Gathered_to_Sales = Sales_Gas / Gathered_Gas_Production (1 if denominator=0)
    - Gathered_to_Sales_Condensate = Sales_Condensate / Gathered_Condensate_Production (1 if denominator=0)
    
    Then updates PCE_CDA with all calculated fields following VBA logic:
    - Gas - S2 Production = WH_to_S2_AllocFactor × (WH Gas or Gathered Gas)
    - Gas - Sales Production = IF monthly sales >0 THEN WH_to_Sales_AllocFactor × (WH Gas or Gathered Gas) ELSE monthly sales/days
    - Condensate - Sales Production = WH_to_Sales_Cond_AllocFactor × (WH Cond or Gathered Cond)
    - Sales CGR Ratio = Condensate_Sales / Gas_Sales (0 if gas_sales=0)
    """
    
    print("\n" + "="*80)
    print("COMBINED MONTHLY LOADER - SQL SERVER VERSION")
    print("="*80)
    
    print(f"\nSQL Server: {SQL_SERVER}.{SQL_DATABASE}")
    
    # -----------------------------------------------------------------
    # 1. SELECT MONTH TO LOAD
    # -----------------------------------------------------------------
    print("\n1. Select month to load:")
    print("   Enter month in 'Dec 2025' format")
    
    while True:
        month_input = input("   Month to load (e.g., 'Dec 2025'): ").strip()
        try:
            month_date = datetime.strptime(month_input, "%b %Y")
            month_start = month_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            print(f"   Month selected: {month_start.strftime('%B %Y')}")
            break
        except:
            print("   ERROR: Invalid month format. Use 'Dec 2025' format.")
    
    # -----------------------------------------------------------------
    # 2. GET VALNAV FILE
    # -----------------------------------------------------------------
    print("\n" + "-"*40)
    print("VALNAV FILE")
    print("-"*40)
    
    print("Enter the path to your ValNav monthly Excel file:")
    print(r"Example: I:\ResEng\Production\PA Monthly Actuals\Production-Acounting-Vnv.xlsx")
    
    valnav_path = input("ValNav file path: ").strip()
    
    # Validate file exists
    if not os.path.exists(valnav_path):
        print(f"\nERROR: ValNav file not found: {valnav_path}")
        return
    
    # -----------------------------------------------------------------
    # 3. GET PUBLIC DATA ACCUMAP FILE
    # -----------------------------------------------------------------
    print("\n" + "-"*40)
    print("PUBLIC DATA ACCUMAP FILE")
    print("-"*40)
    
    print("Enter the path to your Public Data Accumap Excel file:")
    print(r"Example: I:\ResEng\Production\Prod Macros\Macro 3\Public-Data-Accumap.xlsx")
    
    accumap_path = input("Public Data Accumap file path: ").strip()
    
    # Validate file exists
    if not os.path.exists(accumap_path):
        print(f"\nERROR: Public Data Accumap file not found: {accumap_path}")
        return
    
    print("\nIMPORTANT: Close any applications connected to SQL Server before continuing!")
    
    confirm = input("\nStart combined monthly load? (Type 'GO' to confirm): ")
    if confirm.upper() != 'GO':
        print("Load cancelled.")
        return
    
    total_start = time.time()
    
    # Initialize variables to avoid UnboundLocalError
    valnav_data = {}
    accumap_data = {}
    valnav_uwis = set()
    accumap_uwis = set()
    existing_count = 0
    report_filename = None
    missing_report = None
    
    try:
        # -----------------------------------------------------------------
        # 4. READ VALNAV DATA
        # -----------------------------------------------------------------
        print("\n" + "="*80)
        print("PROCESSING VALNAV DATA")
        print("="*80)
        
        # Read ValNav Excel file
        xl_file = pd.ExcelFile(valnav_path)
        sheet_names = xl_file.sheet_names
        
        # Try to find sheet with month name
        target_valnav_sheet = None
        month_search_full = month_start.strftime("%B %Y").lower()  # Full month name (e.g., "july 2025")
        month_search_abbr = month_start.strftime("%b %Y").lower()  # Abbreviated (e.g., "jul 2025")

        print(f"   Looking for sheets containing: '{month_search_full}' or '{month_search_abbr}'")

        for sheet in sheet_names:
            sheet_lower = sheet.lower()
            if month_search_full in sheet_lower or month_search_abbr in sheet_lower:
                target_valnav_sheet = sheet
                print(f"   Found matching sheet: '{sheet}'")
                break

        if target_valnav_sheet is None:
            print(f"\nWARNING: Could not find sheet for {month_start.strftime('%B %Y')} in ValNav file.")
            print(f"Available sheets: {', '.join(sheet_names)}")
            target_valnav_sheet = input(f"Enter sheet name to use (default is first sheet '{sheet_names[0]}'): ").strip()
            if not target_valnav_sheet:
                target_valnav_sheet = sheet_names[0]
        
        print(f"   Reading ValNav sheet: '{target_valnav_sheet}'")
        
        # Read ValNav data
        df_valnav = pd.read_excel(valnav_path, sheet_name=target_valnav_sheet)
        print(f"   Successfully read {len(df_valnav)} rows from ValNav")
        
        # Clean ValNav UWI values
        df_valnav['UWI_clean_valnav'] = df_valnav['McDaniel database'].astype(str).str.strip()
        
        # Prepare ValNav data dictionary (UWI -> (S2_Gas, Sales_Cond))
        valnav_data = {}
        valnav_uwis = set()
        
        for idx, row in df_valnav.iterrows():
            uwi = row['UWI_clean_valnav']
            if pd.isna(uwi):
                continue
            
            uwi_str = str(uwi).strip()
            valnav_uwis.add(uwi_str)
            
            # Get gas and condensate values
            gas_volume = row['Gas Actual Volume'] if pd.notna(row['Gas Actual Volume']) else 0
            cond_volume = row['Allocation Disp Condensate Volume (m³)'] if pd.notna(row['Allocation Disp Condensate Volume (m³)']) else 0
            
            valnav_data[uwi_str] = {
                'S2_Gas': float(gas_volume),
                'Sales_Cond': float(cond_volume)
            }
        
        print(f"   Processed {len(valnav_data)} ValNav records with data")
        
        # -----------------------------------------------------------------
        # 5. READ PUBLIC DATA ACCUMAP FILE
        # -----------------------------------------------------------------
        print("\n" + "="*80)
        print("PROCESSING PUBLIC DATA ACCUMAP DATA")
        print("="*80)
        
        # Read Public Data Accumap Excel file
        accumap_xl = pd.ExcelFile(accumap_path)
        accumap_sheets = accumap_xl.sheet_names
        
        # Try to find the correct sheet
        target_accumap_sheet = 'Sales Gas - to PRW'
        if target_accumap_sheet not in accumap_sheets:
            print(f"   WARNING: '{target_accumap_sheet}' sheet not found.")
            print(f"   Available sheets: {', '.join(accumap_sheets)}")
            target_accumap_sheet = input(f"   Enter sheet name to use (default is first sheet '{accumap_sheets[0]}'): ").strip()
            if not target_accumap_sheet:
                target_accumap_sheet = accumap_sheets[0]
        
        print(f"   Reading sheet: '{target_accumap_sheet}'")
        
        # Read Public Data Accumap data
        df_accumap = pd.read_excel(accumap_path, sheet_name=target_accumap_sheet)
        print(f"   Successfully read {len(df_accumap)} rows from Public Data Accumap")
        
        # Clean UWI values
        df_accumap['UWI_clean_accumap'] = df_accumap['Unique Well ID'].astype(str).str.strip()
        
        # Remove trailing '0' from UWI
        df_accumap['UWI_clean_accumap'] = df_accumap['UWI_clean_accumap'].apply(
            lambda x: x[:-1] if isinstance(x, str) and x.endswith('0') and len(x) > 1 else x
        )
        
        # Convert date column and filter for target month
        df_accumap['Date_parsed'] = pd.to_datetime(df_accumap['Date'], errors='coerce')
        df_accumap_filtered = df_accumap[
            (df_accumap['Date_parsed'].dt.year == month_start.year) & 
            (df_accumap['Date_parsed'].dt.month == month_start.month)
        ].copy()
        
        print(f"   Found {len(df_accumap_filtered)} Public Data Accumap records for {month_start.strftime('%B %Y')}")
        
        # Prepare data dictionary (UWI -> Sales_Gas)
        accumap_data = {}
        accumap_uwis = set()
        
        for idx, row in df_accumap_filtered.iterrows():
            uwi = row['UWI_clean_accumap']
            if pd.isna(uwi):
                continue
            
            uwi_str = str(uwi).strip()
            accumap_uwis.add(uwi_str)
            
            # Get sales gas value
            sales_gas = row['PRD Monthly Mktbl GAS e3m3'] if pd.notna(row['PRD Monthly Mktbl GAS e3m3']) else 0
            
            accumap_data[uwi_str] = {
                'Sales_Gas': float(sales_gas)
            }
        
        print(f"   Processed {len(accumap_data)} Public Data Accumap records with data")
        
        # -----------------------------------------------------------------
        # 6. CONNECT TO SQL SERVER
        # -----------------------------------------------------------------
        print("\n" + "="*80)
        print("SQL SERVER OPERATIONS")
        print("="*80)
        
        print("\nConnecting to SQL Server...")
        try:
            conn = get_sql_conn()
            cursor = conn.cursor()
            print("   Database connected successfully.")
        except Exception as e:
            print(f"\nERROR connecting to database: {e}")
            return
        
        # -----------------------------------------------------------------
        # 7. DELETE EXISTING DATA FOR THE MONTH
        # -----------------------------------------------------------------
        print("\nClearing existing data for the month...")
        
        # Count existing rows for this month
        cursor.execute("""
            SELECT COUNT(*) FROM Allocation_Factors 
            WHERE MonthStartDate = ?
        """, month_start)
        
        existing_count = cursor.fetchone()[0]
        
        # Delete all rows for this month
        if existing_count > 0:
            cursor.execute("""
                DELETE FROM Allocation_Factors 
                WHERE MonthStartDate = ?
            """, month_start)
            conn.commit()
            print(f"   Deleted {existing_count} existing records for {month_start.strftime('%B %Y')}")
        else:
            print(f"   No existing records found for {month_start.strftime('%B %Y')}")
        
        # -----------------------------------------------------------------
        # 8. FETCH WELL MAPPINGS FROM PCE_WM
        # -----------------------------------------------------------------
        print("\nFetching well mappings from PCE_WM...")
        
        # Fetch ALL UWIs from PCE_WM
        cursor.execute("SELECT [Value Navigator UWI], [Well Name] FROM PCE_WM WHERE [Value Navigator UWI] IS NOT NULL")
        all_pce_uwis = cursor.fetchall()
        
        # Create lookup dictionaries
        pce_uwi_dict = {}  # UWI variations -> WellName
        pce_original_to_wellname = {}  # Original UWI -> WellName
        
        for pce_uwi, well_name in all_pce_uwis:
            if pce_uwi:
                pce_uwi_str = str(pce_uwi).strip()
                # Store original mapping
                pce_original_to_wellname[pce_uwi_str] = well_name
                
                # Create variations for matching
                variations = [pce_uwi_str.lower()]
                
                # Variation 1: Remove first digit if it's a digit
                if len(pce_uwi_str) > 1 and pce_uwi_str[0].isdigit():
                    variations.append(pce_uwi_str[1:].lower())
                
                # Variation 2: Handle /2 vs /02 differences
                if '/' in pce_uwi_str:
                    parts = pce_uwi_str.split('/')
                    if len(parts) > 0:
                        last_part = parts[-1]
                        if last_part.isdigit():
                            # Remove leading zeros
                            clean_last = str(int(last_part))
                            new_uwi = '/'.join(parts[:-1] + [clean_last])
                            variations.append(new_uwi.lower())
                        
                        # Add leading zero for single digit
                        if last_part.isdigit() and len(last_part) == 1:
                            padded_last = last_part.zfill(2)
                            new_uwi = '/'.join(parts[:-1] + [padded_last])
                            variations.append(new_uwi.lower())
                
                # Add all variations to dictionary
                for variation in variations:
                    pce_uwi_dict[variation] = well_name
        
        print(f"   Loaded {len(pce_original_to_wellname)} UWIs from PCE_WM table")
        
        # -----------------------------------------------------------------
        # 9. LOAD ALL PCE_CDA DATA FOR THE MONTH (ONE PASS)
        # -----------------------------------------------------------------
        print("\n" + "="*80)
        print("LOADING ALL PCE_CDA DATA FOR MONTH")
        print("="*80)
        
        # Calculate month end date
        if month_start.month == 12:
            month_end = datetime(month_start.year + 1, 1, 1) - timedelta(days=1)
        else:
            month_end = datetime(month_start.year, month_start.month + 1, 1) - timedelta(days=1)
        
        # Convert to date objects for queries
        month_start_date = month_start.date()
        month_end_date = month_end.date()
        
        print(f"   Aggregating PCE_CDA data for {month_start.strftime('%B %Y')}...")
        
        # Query PCE_CDA for the target month, aggregated by well
        cursor.execute("""
            SELECT [Well Name], 
                   SUM(GasWH_Production) as TotalGasWH,
                   SUM(Condensate_WH_Production) as TotalCondWH,
                   SUM(Gathered_Gas_Production) as TotalGatheredGas,
                   SUM(Gathered_Condensate_Production) as TotalGatheredCond
            FROM PCE_CDA 
            WHERE ProdDate BETWEEN ? AND ?
            GROUP BY [Well Name]
        """, month_start_date, month_end_date)
        
        cda_results = cursor.fetchall()
        
        # Create lookup dictionary keyed by Well Name with all four fields
        cda_lookup = {}
        for well_name, gas_wh, cond_wh, gathered_gas, gathered_cond in cda_results:
            if well_name:  # Skip null well names
                cda_lookup[well_name] = {
                    'prodview_wh_gas': float(gas_wh) if gas_wh is not None else 0,
                    'prodview_wh_cond': float(cond_wh) if cond_wh is not None else 0,
                    'gathered_gas': float(gathered_gas) if gathered_gas is not None else 0,
                    'gathered_cond': float(gathered_cond) if gathered_cond is not None else 0
                }
        
        print(f"   Found CDA data for {len(cda_lookup)} wells")
        
        # -----------------------------------------------------------------
        # 10. MATCH UWIS AND PREPARE COMBINED DATA
        # -----------------------------------------------------------------
        print("\n" + "="*80)
        print("MATCHING UWIS AND PREPARING COMBINED DATA")
        print("="*80)
        
        # Combine all unique UWIs from both sources
        all_source_uwis = valnav_uwis.union(accumap_uwis)
        print(f"   Total unique UWIs from both sources: {len(all_source_uwis)}")
        
        # Track matches and unmatched
        matched_wells = {}  # WellName -> combined data
        unmatched_valnav = []
        unmatched_accumap = []
        unmatched_both = []
        
        # Helper function to normalize UWI for matching
        def normalize_uwi_for_matching(uwi_str):
            """Normalize UWI string for matching."""
            normalized = uwi_str.lower()
            # Handle /02 to /2 conversion
            if normalized.endswith('/02'):
                normalized = normalized[:-3] + '/2'
            return normalized
        
        # Match each source UWI to PCE_WM
        for uwi in all_source_uwis:
            uwi_str = str(uwi)
            matched = False
            
            # Normalize UWI
            normalized_uwi = normalize_uwi_for_matching(uwi_str)
            
            # Try exact match
            if normalized_uwi in pce_uwi_dict:
                well_name = pce_uwi_dict[normalized_uwi]
                matched = True
            
            # Try variations if not matched
            if not matched:
                # Try removing first digit
                if len(uwi_str) > 1 and uwi_str[0].isdigit():
                    try_uwi = uwi_str[1:].lower()
                    if try_uwi in pce_uwi_dict:
                        well_name = pce_uwi_dict[try_uwi]
                        matched = True
            
            if matched:
                # Initialize well data if not exists
                if well_name not in matched_wells:
                    # Get ALL PCE_CDA data for this well (default to 0 if not found)
                    cda_data = cda_lookup.get(well_name, {
                        'prodview_wh_gas': 0, 
                        'prodview_wh_cond': 0,
                        'gathered_gas': 0, 
                        'gathered_cond': 0
                    })
                    
                    matched_wells[well_name] = {
                        'well_name': well_name,
                        'valnav_data': None,
                        'accumap_data': None,
                        'prodview_wh_gas': cda_data['prodview_wh_gas'],
                        'prodview_wh_cond': cda_data['prodview_wh_cond'],
                        'gathered_gas': cda_data['gathered_gas'],
                        'gathered_cond': cda_data['gathered_cond']
                    }
                
                # Add ValNav data if available
                if uwi_str in valnav_data:
                    matched_wells[well_name]['valnav_data'] = valnav_data[uwi_str]
                
                # Add Public Data Accumap data if available
                if uwi_str in accumap_data:
                    matched_wells[well_name]['accumap_data'] = accumap_data[uwi_str]
            else:
                # Track which source this UWI came from
                in_valnav = uwi_str in valnav_data
                in_accumap = uwi_str in accumap_data
                
                if in_valnav and in_accumap:
                    unmatched_both.append(uwi_str)
                elif in_valnav:
                    unmatched_valnav.append(uwi_str)
                elif in_accumap:
                    unmatched_accumap.append(uwi_str)
        
        print(f"   Successfully matched: {len(matched_wells)} wells")
        print(f"   Unmatched ValNav UWIs: {len(unmatched_valnav)}")
        print(f"   Unmatched Public Data Accumap UWIs: {len(unmatched_accumap)}")
        print(f"   Unmatched in both sources: {len(unmatched_both)}")
        
        # -----------------------------------------------------------------
        # 10.5 CHECK FOR MISSING WELLS (COMPARE WITH ALLOCATION_FACTORS MASTER LIST)
        # -----------------------------------------------------------------
        print("\n" + "="*80)
        print("CHECKING FOR MISSING WELLS")
        print("="*80)
        
        # Helper function to normalize well names for comparison
        def normalize_well_name(name):
            """Normalize well name for consistent comparison."""
            if not name or not isinstance(name, str):
                return name
            # Convert to uppercase and strip
            return name.upper().strip()
        
        # Get the master list of all wells that have ever appeared in Allocation_Factors
        cursor.execute("SELECT DISTINCT [Well Name] FROM Allocation_Factors WHERE [Well Name] IS NOT NULL")
        all_af_wells = cursor.fetchall()
        master_wells = set()
        master_wells_original = {}  # Store original for display
        for row in all_af_wells:
            if row[0] and row[0].strip():
                original = row[0].strip()
                normalized = normalize_well_name(original)
                master_wells.add(normalized)
                master_wells_original[normalized] = original
        
        print(f"   Total wells in Allocation_Factors master list: {len(master_wells)}")
        
        # Get the wells that were successfully matched and loaded this month (normalized)
        loaded_wells = set()
        loaded_wells_original = {}  # Store original for display
        for well_name in matched_wells.keys():
            if well_name:
                normalized = normalize_well_name(well_name)
                loaded_wells.add(normalized)
                loaded_wells_original[normalized] = well_name
        
        print(f"   Wells successfully matched from ValNav/Public Data Accumap: {len(loaded_wells)}")
        
        # Find missing wells (in master list but not loaded this month)
        missing_normalized = master_wells - loaded_wells
        missing_count = len(missing_normalized)
        
        # Convert back to original names for display
        missing_wells = []
        for norm in sorted(missing_normalized):
            # Use master list original if available, otherwise use normalized
            missing_wells.append(master_wells_original.get(norm, norm))
        
        if missing_count > 0:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            missing_report = f"missing_wells_{month_start.strftime('%Y%m')}_{timestamp}.txt"
            
            print(f"\n   ⚠️ WARNING: {missing_count} wells had no ValNav/Public Data Accumap data:")
            for i, well in enumerate(missing_wells[:10], 1):
                print(f"      - {well}")
            if missing_count > 10:
                print(f"      ... and {missing_count - 10} more")
            
            # -----------------------------------------------------------------
            # ADD MISSING WELLS TO THE LOAD WITH ZEROS
            # -----------------------------------------------------------------
            print(f"\n   Adding missing wells to Allocation_Factors with zeros...")
            
            wells_added = 0
            for well_name in missing_wells:
                # Check if this well has any PCE_CDA data (for informational purposes only)
                cursor.execute("""
                    SELECT COUNT(*) FROM PCE_CDA 
                    WHERE [Well Name] = ? 
                    AND ProdDate BETWEEN ? AND ?
                """, well_name, month_start_date, month_end_date)
                cda_count = cursor.fetchone()[0]
                
                # Add to matched_wells dictionary with all zeros
                matched_wells[well_name] = {
                    'well_name': well_name,
                    'valnav_data': None,
                    'accumap_data': None,
                    'prodview_wh_gas': 0.0,
                    'prodview_wh_cond': 0.0,
                    'gathered_gas': 0.0,
                    'gathered_cond': 0.0
                }
                
                wells_added += 1
            
            print(f"   ✅ Added {wells_added} wells to the load with zeros")
            
            # Update loaded_wells set for summary
            for well_name in missing_wells:
                loaded_wells.add(normalize_well_name(well_name))
            
            # Save the missing wells report
            with open(missing_report, 'w') as f:
                f.write("="*80 + "\n")
                f.write(f"MISSING WELLS REPORT - {month_start.strftime('%B %Y')}\n")
                f.write("="*80 + "\n\n")
                f.write(f"Report Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Month: {month_start.strftime('%B %Y')}\n\n")
                f.write(f"Total wells in Allocation_Factors master list: {len(master_wells)}\n")
                f.write(f"Wells loaded from ValNav/Public Data Accumap: {len(matched_wells) - wells_added}\n")
                f.write(f"Wells added with zeros: {wells_added}\n")
                f.write(f"Total wells to be processed: {len(matched_wells)}\n\n")
                f.write("Wells with no ValNav/Public Data Accumap data (added with zeros):\n")
                for well in missing_wells:
                    f.write(f"   - {well}\n")
                f.write(f"\nNote: These wells were added to Allocation_Factors with zeros\n")
                f.write(f"      and will use default allocation factors (1.0) for PCE_CDA updates.\n")
            
            print(f"\n   Detailed report saved to: {missing_report}")
            
        else:
            print(f"\n   ✅ All {len(master_wells)} wells from Allocation_Factors master list were successfully loaded!")
            wells_added = 0
        
        # Update loaded count for summary
        total_loaded_wells = len(matched_wells)
        
        # -----------------------------------------------------------------
        # 11. SAVE UNMATCHED UWIS REPORT
        # -----------------------------------------------------------------
        if unmatched_valnav or unmatched_accumap or unmatched_both:
            print("\nSaving unmatched UWIs report...")
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_filename = f"combined_unmatched_uwis_{month_start.strftime('%Y%m')}_{timestamp}.txt"
            
            with open(report_filename, 'w') as f:
                f.write("="*80 + "\n")
                f.write(f"COMBINED UNMATCHED UWIS REPORT - {month_start.strftime('%B %Y')}\n")
                f.write("="*80 + "\n\n")
                
                f.write(f"Report Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Month: {month_start.strftime('%B %Y')}\n")
                f.write(f"ValNav File: {os.path.basename(valnav_path)}\n")
                f.write(f"Public Data Accumap File: {os.path.basename(accumap_path)}\n\n")
                
                f.write(f"Total wells matched: {len(matched_wells)}\n")
                f.write(f"Total UWIs processed: {len(all_source_uwis)}\n")
                if len(all_source_uwis) > 0:
                    f.write(f"Match rate: {(len(matched_wells)/len(all_source_uwis)*100):.1f}%\n\n")
                else:
                    f.write(f"Match rate: 0%\n\n")
                
                if unmatched_valnav:
                    f.write("\n" + "-"*40 + "\n")
                    f.write(f"UNMATCHED VALNAV UWIs ({len(unmatched_valnav)})\n")
                    f.write("-"*40 + "\n")
                    for i, uwi in enumerate(sorted(unmatched_valnav), 1):
                        f.write(f"{i:4}. {uwi}\n")
                
                if unmatched_accumap:
                    f.write("\n" + "-"*40 + "\n")
                    f.write(f"UNMATCHED PUBLIC DATA ACCUMAP UWIs ({len(unmatched_accumap)})\n")
                    f.write("-"*40 + "\n")
                    for i, uwi in enumerate(sorted(unmatched_accumap), 1):
                        f.write(f"{i:4}. {uwi}\n")
                
                if unmatched_both:
                    f.write("\n" + "-"*40 + "\n")
                    f.write(f"UNMATCHED IN BOTH SOURCES ({len(unmatched_both)})\n")
                    f.write("-"*40 + "\n")
                    for i, uwi in enumerate(sorted(unmatched_both), 1):
                        f.write(f"{i:4}. {uwi}\n")
            
            print(f"   Report saved to: {report_filename}")
        
        # -----------------------------------------------------------------
        # 12. INSERT COMBINED DATA INTO DATABASE
        # -----------------------------------------------------------------
        print("\n" + "="*80)
        print("INSERTING COMBINED DATA")
        print("="*80)
        
        # Get metadata
        valnav_source = os.path.basename(valnav_path)
        accumap_source = os.path.basename(accumap_path)
        loaded_at = datetime.now()
        
        # Track statistics
        wells_inserted = 0
        wells_valnav_only = 0
        wells_accumap_only = 0
        wells_both = 0
        wells_with_cda = 0
        errors = 0
        
        # Process each matched well
        print(f"\nInserting data for {len(matched_wells)} wells...")
        
        for well_idx, (well_name, well_data) in enumerate(matched_wells.items(), 1):
            try:
                # Extract data from all sources
                valnav_data_for_well = well_data['valnav_data']
                accumap_data_for_well = well_data['accumap_data']
                
                # Get all PCE_CDA fields
                prodview_wh_gas = well_data['prodview_wh_gas']
                prodview_wh_cond = well_data['prodview_wh_cond']
                gathered_gas = well_data['gathered_gas']
                gathered_cond = well_data['gathered_cond']
                
                # Determine data sources
                has_valnav = valnav_data_for_well is not None
                has_accumap = accumap_data_for_well is not None
                has_cda = (prodview_wh_gas > 0 or prodview_wh_cond > 0 or 
                          gathered_gas > 0 or gathered_cond > 0)
                
                # Update counters
                if has_valnav and has_accumap:
                    wells_both += 1
                elif has_valnav:
                    wells_valnav_only += 1
                elif has_accumap:
                    wells_accumap_only += 1
                
                if has_cda:
                    wells_with_cda += 1
                
                # Prepare values from ValNav/Accumap
                s2_gas = valnav_data_for_well['S2_Gas'] if has_valnav else 0
                sales_cond = valnav_data_for_well['Sales_Cond'] if has_valnav else 0
                sales_gas = accumap_data_for_well['Sales_Gas'] if has_accumap else 0
                
                # -----------------------------------------------------------------
                # CALCULATE ALLOCATION FACTORS
                # -----------------------------------------------------------------
                # WH_to_S2_AllocFactor = S2_Gas / Prodview_WH_Gas (1 if denominator = 0)
                if prodview_wh_gas == 0:
                    wh_to_s2 = 1.0
                else:
                    wh_to_s2 = s2_gas / prodview_wh_gas
                
                # WH_to_Sales_AllocFactor = Sales_Gas / Prodview_WH_Gas (1 if denominator = 0)
                if prodview_wh_gas == 0:
                    wh_to_sales_gas = 1.0
                else:
                    wh_to_sales_gas = sales_gas / prodview_wh_gas
                
                # WH_to_Sales_Cond_AllocFactor = Sales_Cond / Prodview_WH_Cond (1 if denominator = 0)
                if prodview_wh_cond == 0:
                    wh_to_sales_cond = 1.0
                else:
                    wh_to_sales_cond = sales_cond / prodview_wh_cond
                
                # -----------------------------------------------------------------
                # CALCULATE GATHERED TO RATIOS (TEXT FIELDS)
                # -----------------------------------------------------------------
                # Gathered_to_S2_Gas = S2_Gas / Gathered_Gas_Production (1 if denominator = 0)
                if gathered_gas == 0:
                    gathered_to_s2_str = "1"
                else:
                    gathered_to_s2 = s2_gas / gathered_gas
                    gathered_to_s2_str = str(gathered_to_s2)
                
                # Gathered_to_Sales = Sales_Gas / Gathered_Gas_Production (1 if denominator = 0)
                if gathered_gas == 0:
                    gathered_to_sales_str = "1"
                else:
                    gathered_to_sales = sales_gas / gathered_gas
                    gathered_to_sales_str = str(gathered_to_sales)
                
                # Gathered_to_Sales_Condensate = Sales_Condensate / Gathered_Condensate_Production (1 if denominator = 0)
                if gathered_cond == 0:
                    gathered_to_sales_cond_str = "1"
                else:
                    gathered_to_sales_cond = sales_cond / gathered_cond
                    gathered_to_sales_cond_str = str(gathered_to_sales_cond)
                
                # Combine source info
                combined_source = f"ValNav: {valnav_source}, Public Data Accumap: {accumap_source}"
                
                # Insert into database with ALL fields including the new text fields
                cursor.execute("""
                    INSERT INTO Allocation_Factors (
                        MonthStartDate, [Well Name], 
                        Prodview_WH_Gas, Prodview_WH_Cond,
                        S2_Gas, Sales_Condensate, Sales_Gas,
                        Gathered_Gas_Production, Gathered_Condensate_Production,
                        WH_to_S2_AllocFactor, WH_to_Sales_AllocFactor, WH_to_Sales_Cond_AllocFactor,
                        Gathered_to_S2_Gas, Gathered_to_Sales, Gathered_to_Sales_Condensate,
                        SourceFile, LoadedAt
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, month_start, well_name, 
                   prodview_wh_gas, prodview_wh_cond,
                   s2_gas, sales_cond, sales_gas, 
                   gathered_gas, gathered_cond,
                   wh_to_s2, wh_to_sales_gas, wh_to_sales_cond,
                   gathered_to_s2_str, gathered_to_sales_str, gathered_to_sales_cond_str,
                   combined_source, loaded_at)
                
                wells_inserted += 1
                
                # Show progress every 50 wells
                if wells_inserted % 50 == 0:
                    print(f"   Inserted {wells_inserted} wells...")
                
            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"   Error inserting well '{well_name}': {str(e)[:100]}")
        
        # Commit all changes
        conn.commit()
        
        # -----------------------------------------------------------------
        # 13. UPDATE PCE_CDA WITH ALL CALCULATED FIELDS
        # -----------------------------------------------------------------
        print("\n" + "="*80)
        print("UPDATING PCE_CDA WITH ALL CALCULATED FIELDS")
        print("="*80)

        print(f"   Updating daily PCE_CDA records for {month_start.strftime('%B %Y')}...")

        # Get all allocation factors for this month (all three factors plus sales gas)
        cursor.execute("""
            SELECT [Well Name], 
                   WH_to_S2_AllocFactor,
                   WH_to_Sales_AllocFactor,
                   WH_to_Sales_Cond_AllocFactor,
                   Sales_Gas
            FROM Allocation_Factors 
            WHERE MonthStartDate = ?
        """, month_start)

        allocation_rows = cursor.fetchall()
        print(f"   Found {len(allocation_rows)} wells with allocation factors for this month")

        # Calculate days in month for spreading monthly volume
        days_in_month = (month_end_date - month_start_date).days + 1

        # Update PCE_CDA for each well
        cda_updated = 0
        cda_errors = 0

        for well_name, wh_to_s2, wh_to_sales, wh_to_sales_cond, sales_gas in allocation_rows:
            try:
                # Convert to float with defaults
                wh_to_s2_val = float(wh_to_s2) if wh_to_s2 is not None else 1.0
                wh_to_sales_val = float(wh_to_sales) if wh_to_sales is not None else 1.0
                wh_to_sales_cond_val = float(wh_to_sales_cond) if wh_to_sales_cond is not None else 1.0
                monthly_sales_gas_val = float(sales_gas) if sales_gas is not None else 0
                
                # -----------------------------------------------------------------
                # Update 1: Gas - S2 Production
                # Uses WH gas if available, otherwise gathered gas
                # -----------------------------------------------------------------
                cursor.execute("""
                    UPDATE PCE_CDA 
                    SET [Gas - S2 Production] = 
                        ? * IIF([GasWH_Production] > 0, [GasWH_Production], [Gathered_Gas_Production])
                    WHERE [Well Name] = ? 
                    AND ProdDate BETWEEN ? AND ?
                """, wh_to_s2_val, well_name, month_start_date, month_end_date)
                
                # -----------------------------------------------------------------
                # Update 2: Gas - Sales Production (with monthly sales check)
                # -----------------------------------------------------------------
                if monthly_sales_gas_val > 0:
                    cursor.execute("""
                        UPDATE PCE_CDA 
                        SET [Gas - Sales Production] = 
                            ? * IIF([GasWH_Production] > 0, [GasWH_Production], [Gathered_Gas_Production])
                        WHERE [Well Name] = ? 
                        AND ProdDate BETWEEN ? AND ?
                    """, wh_to_sales_val, well_name, month_start_date, month_end_date)
                else:
                    daily_sales_gas = monthly_sales_gas_val / days_in_month
                    cursor.execute("""
                        UPDATE PCE_CDA 
                        SET [Gas - Sales Production] = ?
                        WHERE [Well Name] = ? 
                        AND ProdDate BETWEEN ? AND ?
                    """, daily_sales_gas, well_name, month_start_date, month_end_date)
                
                # -----------------------------------------------------------------
                # Update 3: Condensate - Sales Production
                # Uses WH condensate if available, otherwise gathered condensate
                # -----------------------------------------------------------------
                cursor.execute("""
                    UPDATE PCE_CDA 
                    SET [Condensate - Sales Production] = 
                        ? * IIF([Condensate_WH_Production] > 0, [Condensate_WH_Production], [Gathered_Condensate_Production])
                    WHERE [Well Name] = ? 
                    AND ProdDate BETWEEN ? AND ?
                """, wh_to_sales_cond_val, well_name, month_start_date, month_end_date)
                
                # -----------------------------------------------------------------
                # Update 4: Sales CGR Ratio (calculated from the other fields)
                # -----------------------------------------------------------------
                cursor.execute("""
                    UPDATE PCE_CDA 
                    SET [Sales CGR Ratio] = 
                        IIF([Gas - Sales Production] > 0, 
                            [Condensate - Sales Production] / [Gas - Sales Production], 
                            0)
                    WHERE [Well Name] = ? 
                    AND ProdDate BETWEEN ? AND ?
                """, well_name, month_start_date, month_end_date)
                
                rows_affected = cursor.rowcount
                cda_updated += rows_affected
                
            except Exception as e:
                cda_errors += 1
                if cda_errors <= 5:
                    print(f"   Error updating PCE_CDA for well '{well_name}': {str(e)[:100]}")

        # Commit all CDA updates
        conn.commit()
        print(f"   Updated {cda_updated} daily records in PCE_CDA")
        if cda_errors > 0:
            print(f"   Errors during PCE_CDA update: {cda_errors}")

        # Close connection
        conn.close()
        
        # -----------------------------------------------------------------
        # 14. SUMMARY
        # -----------------------------------------------------------------
        total_time = time.time() - total_start
        
        print("\n" + "="*80)
        print("COMBINED LOAD SUMMARY")
        print("="*80)
        
        print(f"\nMONTH PROCESSED:")
        print(f"   {month_start.strftime('%B %Y')}")
        
        print(f"\nSOURCE FILES:")
        print(f"   ValNav: {valnav_source}")
        print(f"   Public Data Accumap: {accumap_source}")
        
        print(f"\nDATA STATISTICS:")
        print(f"   Total ValNav records: {len(valnav_data)}")
        print(f"   Total Public Data Accumap records: {len(accumap_data)}")
        print(f"   Wells with CDA data: {wells_with_cda}")
        print(f"   Unique UWIs processed: {len(all_source_uwis)}")
        print(f"   Wells successfully matched: {len(matched_wells)}")
        if len(all_source_uwis) > 0:
            print(f"   Match rate: {(len(matched_wells)/len(all_source_uwis)*100):.1f}%")
        else:
            print(f"   Match rate: 0%")
        
        print(f"\nDATA DISTRIBUTION:")
        print(f"   Wells with both ValNav and Public Data Accumap data: {wells_both}")
        print(f"   Wells with ValNav data only: {wells_valnav_only}")
        print(f"   Wells with Public Data Accumap data only: {wells_accumap_only}")
        print(f"   Wells with CDA data (any field): {wells_with_cda}")
        
        print(f"\nDATABASE OPERATIONS:")
        print(f"   Previous records deleted: {existing_count}")
        print(f"   New records inserted: {wells_inserted}")
        print(f"   Processing errors: {errors}")
        print(f"   PCE_CDA records updated: {cda_updated}")
        print(f"   PCE_CDA update errors: {cda_errors}")
        
        print(f"\nWELL COUNT VERIFICATION:")
        print(f"   Total wells in Allocation_Factors master list: {len(master_wells)}")
        print(f"   Wells loaded from ValNav/Public Data Accumap: {total_loaded_wells - wells_added}")
        if wells_added > 0:
            print(f"   Wells added with zeros (no source data): {wells_added}")
        print(f"   Total wells processed: {total_loaded_wells}")
        
        if missing_count > 0:
            print(f"\n   ⚠️ The following wells were added with zeros (no source data):")
            for i, well in enumerate(missing_wells[:10], 1):
                print(f"      {i}. {well}")
            if missing_count > 10:
                print(f"      ... and {missing_count - 10} more")
            print(f"\n   See '{missing_report}' for complete list")
        
        print(f"\nPCE_CDA FIELDS UPDATED:")
        print(f"   - Gas - S2 Production: WH_to_S2 × (WH_Gas or Gathered_Gas)")
        print(f"   - Gas - Sales Production: IF monthly_sales>0 THEN WH_to_Sales × (WH_Gas or Gathered_Gas) ELSE monthly_sales/days")
        print(f"   - Condensate - Sales Production: WH_to_Sales_Cond × (WH_Cond or Gathered_Cond)")
        print(f"   - Sales CGR Ratio: Condensate_Sales / Gas_Sales (0 if gas_sales=0)")
        
        print(f"\nUNMATCHED UWIS:")
        print(f"   ValNav only: {len(unmatched_valnav)}")
        print(f"   Public Data Accumap only: {len(unmatched_accumap)}")
        print(f"   Both sources: {len(unmatched_both)}")
        
        print(f"\nPERFORMANCE:")
        print(f"   Total processing time: {total_time:.1f} seconds")
        print(f"   Destination: {SQL_SERVER}.{SQL_DATABASE}")
        
        if unmatched_valnav or unmatched_accumap:
            print(f"\nNOTE: Some UWIs could not be matched to PCE_WM table.")
            print(f"      Check '{report_filename}' for details.")
        
        print("\n" + "="*80)
        print("COMBINED LOAD COMPLETE!")
        print("="*80)
        
    except Exception as e:
        print(f"\nERROR: {e}")
        print("Full traceback:")
        traceback.print_exc()
        return False
    
    return True

def main_menu():
    """Main menu for selecting which loader to use."""
    
    while True:
        print("\n" + "="*80)
        print("MONTHLY DATA LOADER SYSTEM - SQL SERVER")
        print("="*80)
        print("\nSelect an option:")
        print("  1. Combined Monthly Loader (ValNav + Public Data Accumap + SQL Server)")
        print("  2. Exit")
        
        choice = input("\nEnter your choice (1-2): ").strip()
        
        if choice == '1':
            combined_monthly_loader()
        elif choice == '2':
            print("\nExiting...")
            break
        else:
            print("\nInvalid choice. Please enter 1 or 2.")
        
        input("\nPress Enter to continue...")

if __name__ == "__main__":
    main_menu()
    print("\nPress Enter to exit...")
    input()