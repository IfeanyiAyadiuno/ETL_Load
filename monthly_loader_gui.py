# monthly_loader_gui.py
import pandas as pd
import pyodbc
import time
from datetime import datetime, timedelta
import os
import re
import traceback
from db_connection import get_sql_conn

def run_monthly_loader(month_str, valnav_path, accumap_path, progress_callback=None, log_callback=None):
    """
    Run the monthly loader with GUI integration
    
    Args:
        month_str: Month in format "MMM YYYY" (e.g., "Dec 2025")
        valnav_path: Path to ValNav Excel file
        accumap_path: Path to Accumap Excel file
        progress_callback: Function to call with progress percentage (0-100)
        log_callback: Function to call with log messages
    
    Returns:
        dict: Summary statistics and warning messages
    """
    
    def log(message):
        """Send log message to GUI if callback exists"""
        if log_callback:
            log_callback(message)
        else:
            print(message)
    
    def progress(value):
        """Send progress to GUI if callback exists"""
        if progress_callback:
            progress_callback(value)
    
    log("\n" + "="*80)
    log("COMBINED MONTHLY LOADER - GUI VERSION")
    log("="*80)
    
    total_start = time.time()
    
    # Parse month
    try:
        month_date = datetime.strptime(month_str, "%b %Y")
        month_start = month_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        log(f"Month selected: {month_start.strftime('%B %Y')}")
    except:
        error_msg = f"ERROR: Invalid month format: {month_str}"
        log(error_msg)
        return {"error": error_msg}
    
    # Validate files exist
    if not os.path.exists(valnav_path):
        error_msg = f"ERROR: ValNav file not found: {valnav_path}"
        log(error_msg)
        return {"error": error_msg}
    
    if not os.path.exists(accumap_path):
        error_msg = f"ERROR: Accumap file not found: {accumap_path}"
        log(error_msg)
        return {"error": error_msg}
    
    # Initialize variables
    valnav_data = {}
    accumap_data = {}
    valnav_uwis = set()
    accumap_uwis = set()
    existing_count = 0
    report_filename = None
    missing_report = None
    
    try:
        # -----------------------------------------------------------------
        # READ VALNAV DATA
        # -----------------------------------------------------------------
        log("\n" + "="*80)
        log("PROCESSING VALNAV DATA")
        log("="*80)
        progress(10)
        
        # Read ValNav Excel file
        xl_file = pd.ExcelFile(valnav_path)
        sheet_names = xl_file.sheet_names
        
        # Try to find sheet with month name
        target_valnav_sheet = None
        month_search_full = month_start.strftime("%B %Y").lower()
        month_search_abbr = month_start.strftime("%b %Y").lower()

        log(f"   Looking for sheets containing: '{month_search_full}' or '{month_search_abbr}'")

        for sheet in sheet_names:
            sheet_lower = sheet.lower()
            if month_search_full in sheet_lower or month_search_abbr in sheet_lower:
                target_valnav_sheet = sheet
                log(f"   Found matching sheet: '{sheet}'")
                break

        if target_valnav_sheet is None:
            log(f"\nWARNING: Could not find sheet for {month_start.strftime('%B %Y')} in ValNav file.")
            target_valnav_sheet = sheet_names[0]
            log(f"   Using first sheet: '{target_valnav_sheet}'")
        
        log(f"   Reading ValNav sheet: '{target_valnav_sheet}'")
        
        # Read ValNav data
        df_valnav = pd.read_excel(valnav_path, sheet_name=target_valnav_sheet)
        log(f"   Successfully read {len(df_valnav)} rows from ValNav")
        
        # Clean ValNav UWI values
        df_valnav['UWI_clean_valnav'] = df_valnav['McDaniel database'].astype(str).str.strip()
        
        # Prepare ValNav data dictionary
        valnav_data = {}
        valnav_uwis = set()
        
        for idx, row in df_valnav.iterrows():
            uwi = row['UWI_clean_valnav']
            if pd.isna(uwi):
                continue
            
            uwi_str = str(uwi).strip()
            valnav_uwis.add(uwi_str)
            
            gas_volume = row['Gas Actual Volume'] if pd.notna(row['Gas Actual Volume']) else 0
            cond_volume = row['Allocation Disp Condensate Volume (m³)'] if pd.notna(row['Allocation Disp Condensate Volume (m³)']) else 0
            
            valnav_data[uwi_str] = {
                'S2_Gas': float(gas_volume),
                'Sales_Cond': float(cond_volume)
            }
        
        log(f"   Processed {len(valnav_data)} ValNav records with data")
        progress(20)
        
        # -----------------------------------------------------------------
        # READ ACCUMAP DATA
        # -----------------------------------------------------------------
        log("\n" + "="*80)
        log("PROCESSING PUBLIC DATA ACCUMAP DATA")
        log("="*80)
        
        # Read Accumap Excel file
        accumap_xl = pd.ExcelFile(accumap_path)
        accumap_sheets = accumap_xl.sheet_names
        
        # Try to find the correct sheet
        target_accumap_sheet = 'Sales Gas - to PRW'
        if target_accumap_sheet not in accumap_sheets:
            log(f"   WARNING: '{target_accumap_sheet}' sheet not found.")
            target_accumap_sheet = accumap_sheets[0]
            log(f"   Using first sheet: '{target_accumap_sheet}'")
        
        log(f"   Reading sheet: '{target_accumap_sheet}'")
        
        # Read Accumap data
        df_accumap = pd.read_excel(accumap_path, sheet_name=target_accumap_sheet)
        log(f"   Successfully read {len(df_accumap)} rows from Public Data Accumap")
        
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
        
        log(f"   Found {len(df_accumap_filtered)} Accumap records for {month_start.strftime('%B %Y')}")
        
        # Prepare data dictionary
        accumap_data = {}
        accumap_uwis = set()
        
        for idx, row in df_accumap_filtered.iterrows():
            uwi = row['UWI_clean_accumap']
            if pd.isna(uwi):
                continue
            
            uwi_str = str(uwi).strip()
            accumap_uwis.add(uwi_str)
            
            sales_gas = row['PRD Monthly Mktbl GAS e3m3'] if pd.notna(row['PRD Monthly Mktbl GAS e3m3']) else 0
            
            accumap_data[uwi_str] = {
                'Sales_Gas': float(sales_gas)
            }
        
        log(f"   Processed {len(accumap_data)} Accumap records with data")
        progress(30)
        
        # -----------------------------------------------------------------
        # CONNECT TO SQL SERVER
        # -----------------------------------------------------------------
        log("\n" + "="*80)
        log("SQL SERVER OPERATIONS")
        log("="*80)
        
        log("\nConnecting to SQL Server...")
        conn = get_sql_conn()
        cursor = conn.cursor()
        log("   Database connected successfully.")
        progress(35)
        
        # -----------------------------------------------------------------
        # DELETE EXISTING DATA FOR THE MONTH
        # -----------------------------------------------------------------
        log("\nClearing existing data for the month...")
        
        cursor.execute("""
            SELECT COUNT(*) FROM Allocation_Factors 
            WHERE MonthStartDate = ?
        """, month_start)
        
        existing_count = cursor.fetchone()[0]
        
        if existing_count > 0:
            cursor.execute("""
                DELETE FROM Allocation_Factors 
                WHERE MonthStartDate = ?
            """, month_start)
            conn.commit()
            log(f"   Deleted {existing_count} existing records for {month_start.strftime('%B %Y')}")
        else:
            log(f"   No existing records found for {month_start.strftime('%B %Y')}")
        
        progress(40)
        
        # -----------------------------------------------------------------
        # FETCH WELL MAPPINGS FROM PCE_WM
        # -----------------------------------------------------------------
        log("\nFetching well mappings from PCE_WM...")
        
        cursor.execute("SELECT [Value Navigator UWI], [Well Name] FROM PCE_WM WHERE [Value Navigator UWI] IS NOT NULL")
        all_pce_uwis = cursor.fetchall()
        
        # Create lookup dictionaries
        pce_uwi_dict = {}
        pce_original_to_wellname = {}
        
        for pce_uwi, well_name in all_pce_uwis:
            if pce_uwi:
                pce_uwi_str = str(pce_uwi).strip()
                pce_original_to_wellname[pce_uwi_str] = well_name
                
                variations = [pce_uwi_str.lower()]
                
                if len(pce_uwi_str) > 1 and pce_uwi_str[0].isdigit():
                    variations.append(pce_uwi_str[1:].lower())
                
                if '/' in pce_uwi_str:
                    parts = pce_uwi_str.split('/')
                    if len(parts) > 0:
                        last_part = parts[-1]
                        if last_part.isdigit():
                            clean_last = str(int(last_part))
                            new_uwi = '/'.join(parts[:-1] + [clean_last])
                            variations.append(new_uwi.lower())
                        
                        if last_part.isdigit() and len(last_part) == 1:
                            padded_last = last_part.zfill(2)
                            new_uwi = '/'.join(parts[:-1] + [padded_last])
                            variations.append(new_uwi.lower())
                
                for variation in variations:
                    pce_uwi_dict[variation] = well_name
        
        log(f"   Loaded {len(pce_original_to_wellname)} UWIs from PCE_WM table")
        progress(45)
        
        # -----------------------------------------------------------------
        # LOAD PCE_CDA DATA FOR THE MONTH
        # -----------------------------------------------------------------
        log("\n" + "="*80)
        log("LOADING PCE_CDA DATA FOR MONTH")
        log("="*80)
        
        # Calculate month end date
        if month_start.month == 12:
            month_end = datetime(month_start.year + 1, 1, 1) - timedelta(days=1)
        else:
            month_end = datetime(month_start.year, month_start.month + 1, 1) - timedelta(days=1)
        
        month_start_date = month_start.date()
        month_end_date = month_end.date()
        
        log(f"   Aggregating PCE_CDA data for {month_start.strftime('%B %Y')}...")
        
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
        
        cda_lookup = {}
        for well_name, gas_wh, cond_wh, gathered_gas, gathered_cond in cda_results:
            if well_name:
                cda_lookup[well_name] = {
                    'prodview_wh_gas': float(gas_wh) if gas_wh is not None else 0,
                    'prodview_wh_cond': float(cond_wh) if cond_wh is not None else 0,
                    'gathered_gas': float(gathered_gas) if gathered_gas is not None else 0,
                    'gathered_cond': float(gathered_cond) if gathered_cond is not None else 0
                }
        
        log(f"   Found CDA data for {len(cda_lookup)} wells")
        progress(50)
        
        # -----------------------------------------------------------------
        # MATCH UWIS AND PREPARE COMBINED DATA
        # -----------------------------------------------------------------
        log("\n" + "="*80)
        log("MATCHING UWIS AND PREPARING COMBINED DATA")
        log("="*80)
        
        all_source_uwis = valnav_uwis.union(accumap_uwis)
        log(f"   Total unique UWIs from both sources: {len(all_source_uwis)}")
        
        matched_wells = {}
        unmatched_valnav = []
        unmatched_accumap = []
        unmatched_both = []
        
        def normalize_uwi_for_matching(uwi_str):
            normalized = uwi_str.lower()
            if normalized.endswith('/02'):
                normalized = normalized[:-3] + '/2'
            return normalized
        
        for uwi in all_source_uwis:
            uwi_str = str(uwi)
            matched = False
            
            normalized_uwi = normalize_uwi_for_matching(uwi_str)
            
            if normalized_uwi in pce_uwi_dict:
                well_name = pce_uwi_dict[normalized_uwi]
                matched = True
            
            if not matched:
                if len(uwi_str) > 1 and uwi_str[0].isdigit():
                    try_uwi = uwi_str[1:].lower()
                    if try_uwi in pce_uwi_dict:
                        well_name = pce_uwi_dict[try_uwi]
                        matched = True
            
            if matched:
                if well_name not in matched_wells:
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
                
                if uwi_str in valnav_data:
                    matched_wells[well_name]['valnav_data'] = valnav_data[uwi_str]
                
                if uwi_str in accumap_data:
                    matched_wells[well_name]['accumap_data'] = accumap_data[uwi_str]
            else:
                in_valnav = uwi_str in valnav_data
                in_accumap = uwi_str in accumap_data
                
                if in_valnav and in_accumap:
                    unmatched_both.append(uwi_str)
                elif in_valnav:
                    unmatched_valnav.append(uwi_str)
                elif in_accumap:
                    unmatched_accumap.append(uwi_str)
        
        log(f"   Successfully matched: {len(matched_wells)} wells")
        log(f"   Unmatched ValNav UWIs: {len(unmatched_valnav)}")
        log(f"   Unmatched Accumap UWIs: {len(unmatched_accumap)}")
        log(f"   Unmatched in both sources: {len(unmatched_both)}")
        progress(60)
        
        # -----------------------------------------------------------------
        # CHECK FOR MISSING WELLS
        # -----------------------------------------------------------------
        log("\n" + "="*80)
        log("CHECKING FOR MISSING WELLS")
        log("="*80)
        
        def normalize_well_name(name):
            if not name or not isinstance(name, str):
                return name
            return name.upper().strip()
        
        cursor.execute("SELECT DISTINCT [Well Name] FROM Allocation_Factors WHERE [Well Name] IS NOT NULL")
        all_af_wells = cursor.fetchall()
        master_wells = set()
        master_wells_original = {}
        
        for row in all_af_wells:
            if row[0] and row[0].strip():
                original = row[0].strip()
                normalized = normalize_well_name(original)
                master_wells.add(normalized)
                master_wells_original[normalized] = original
        
        log(f"   Total wells in Allocation_Factors master list: {len(master_wells)}")
        
        loaded_wells = set()
        loaded_wells_original = {}
        for well_name in matched_wells.keys():
            if well_name:
                normalized = normalize_well_name(well_name)
                loaded_wells.add(normalized)
                loaded_wells_original[normalized] = well_name
        
        log(f"   Wells successfully matched from ValNav/Accumap: {len(loaded_wells)}")
        
        missing_normalized = master_wells - loaded_wells
        missing_count = len(missing_normalized)
        
        missing_wells = []
        warning_messages = []
        
        for norm in sorted(missing_normalized):
            missing_wells.append(master_wells_original.get(norm, norm))
        
        if missing_count > 0:
            log(f"\n   ⚠️ WARNING: {missing_count} wells had no ValNav/Accumap data:")
            warning_messages.append(f"{missing_count} wells had no source data")
            for i, well in enumerate(missing_wells[:10], 1):
                log(f"      - {well}")
            if missing_count > 10:
                log(f"      ... and {missing_count - 10} more")
            
            log(f"\n   Adding missing wells to Allocation_Factors with zeros...")
            
            wells_added = 0
            for well_name in missing_wells:
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
            
            log(f"   ✅ Added {wells_added} wells to the load with zeros")
        else:
            log(f"\n   ✅ All {len(master_wells)} wells from master list were successfully loaded!")
            wells_added = 0
        
        total_loaded_wells = len(matched_wells)
        progress(70)
        
        # -----------------------------------------------------------------
        # INSERT COMBINED DATA
        # -----------------------------------------------------------------
        log("\n" + "="*80)
        log("INSERTING COMBINED DATA")
        log("="*80)
        
        valnav_source = os.path.basename(valnav_path)
        accumap_source = os.path.basename(accumap_path)
        loaded_at = datetime.now()
        
        wells_inserted = 0
        wells_valnav_only = 0
        wells_accumap_only = 0
        wells_both = 0
        wells_with_cda = 0
        errors = 0
        
        log(f"\nInserting data for {len(matched_wells)} wells...")
        
        for well_idx, (well_name, well_data) in enumerate(matched_wells.items(), 1):
            try:
                valnav_data_for_well = well_data['valnav_data']
                accumap_data_for_well = well_data['accumap_data']
                
                prodview_wh_gas = well_data['prodview_wh_gas']
                prodview_wh_cond = well_data['prodview_wh_cond']
                gathered_gas = well_data['gathered_gas']
                gathered_cond = well_data['gathered_cond']
                
                has_valnav = valnav_data_for_well is not None
                has_accumap = accumap_data_for_well is not None
                has_cda = (prodview_wh_gas > 0 or prodview_wh_cond > 0 or 
                          gathered_gas > 0 or gathered_cond > 0)
                
                if has_valnav and has_accumap:
                    wells_both += 1
                elif has_valnav:
                    wells_valnav_only += 1
                elif has_accumap:
                    wells_accumap_only += 1
                
                if has_cda:
                    wells_with_cda += 1
                
                s2_gas = valnav_data_for_well['S2_Gas'] if has_valnav else 0
                sales_cond = valnav_data_for_well['Sales_Cond'] if has_valnav else 0
                sales_gas = accumap_data_for_well['Sales_Gas'] if has_accumap else 0
                
                # Calculate allocation factors
                if prodview_wh_gas == 0:
                    wh_to_s2 = 1.0
                else:
                    wh_to_s2 = s2_gas / prodview_wh_gas
                
                if prodview_wh_gas == 0:
                    wh_to_sales_gas = 1.0
                else:
                    wh_to_sales_gas = sales_gas / prodview_wh_gas
                
                if prodview_wh_cond == 0:
                    wh_to_sales_cond = 1.0
                else:
                    wh_to_sales_cond = sales_cond / prodview_wh_cond
                
                # Calculate gathered to ratios
                if gathered_gas == 0:
                    gathered_to_s2_str = "1"
                else:
                    gathered_to_s2 = s2_gas / gathered_gas
                    gathered_to_s2_str = str(gathered_to_s2)
                
                if gathered_gas == 0:
                    gathered_to_sales_str = "1"
                else:
                    gathered_to_sales = sales_gas / gathered_gas
                    gathered_to_sales_str = str(gathered_to_sales)
                
                if gathered_cond == 0:
                    gathered_to_sales_cond_str = "1"
                else:
                    gathered_to_sales_cond = sales_cond / gathered_cond
                    gathered_to_sales_cond_str = str(gathered_to_sales_cond)
                
                combined_source = f"ValNav: {valnav_source}, Accumap: {accumap_source}"
                
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
                
                if wells_inserted % 50 == 0:
                    log(f"   Inserted {wells_inserted} wells...")
                    progress(70 + (wells_inserted / len(matched_wells) * 20))
                
            except Exception as e:
                errors += 1
                if errors <= 5:
                    log(f"   Error inserting well '{well_name}': {str(e)[:100]}")
        
        conn.commit()
        progress(90)
        
        # -----------------------------------------------------------------
# REMOVE THIS ENTIRE SECTION - It belongs in Sales Ratios Update
# -----------------------------------------------------------------
# log("\n" + "="*80)
# log("UPDATING PCE_CDA WITH CALCULATED FIELDS")
# log("="*80)
# 
# log(f"   Updating daily PCE_CDA records for {month_start.strftime('%B %Y')}...")
# 
# cursor.execute("""
#     SELECT [Well Name], 
#            WH_to_S2_AllocFactor,
#            WH_to_Sales_AllocFactor,
#            WH_to_Sales_Cond_AllocFactor,
#            Sales_Gas
#     FROM Allocation_Factors 
#     WHERE MonthStartDate = ?
# """, month_start)
# 
# allocation_rows = cursor.fetchall()
# log(f"   Found {len(allocation_rows)} wells with allocation factors")
# 
# days_in_month = (month_end_date - month_start_date).days + 1
# cda_updated = 0
# cda_errors = 0
# 
# for well_name, wh_to_s2, wh_to_sales, wh_to_sales_cond, sales_gas in allocation_rows:
#     try:
#         wh_to_s2_val = float(wh_to_s2) if wh_to_s2 is not None else 1.0
#         wh_to_sales_val = float(wh_to_sales) if wh_to_sales is not None else 1.0
#         wh_to_sales_cond_val = float(wh_to_sales_cond) if wh_to_sales_cond is not None else 1.0
#         monthly_sales_gas_val = float(sales_gas) if sales_gas is not None else 0
#         
#         # Update Gas - S2 Production
#         cursor.execute("""
#             UPDATE PCE_CDA 
#             SET [Gas - S2 Production] = ? * [GasWH_Production]
#             WHERE [Well Name] = ? 
#             AND ProdDate BETWEEN ? AND ?
#         """, wh_to_s2_val, well_name, month_start_date, month_end_date)
#         
#         # Update Gas - Sales Production
#         if monthly_sales_gas_val > 0:
#             cursor.execute("""
#                 UPDATE PCE_CDA 
#                 SET [Gas - Sales Production] = ? * [GasWH_Production]
#                 WHERE [Well Name] = ? 
#                 AND ProdDate BETWEEN ? AND ?
#             """, wh_to_sales_val, well_name, month_start_date, month_end_date)
#         else:
#             daily_sales_gas = monthly_sales_gas_val / days_in_month
#             cursor.execute("""
#                 UPDATE PCE_CDA 
#                 SET [Gas - Sales Production] = ?
#                 WHERE [Well Name] = ? 
#                 AND ProdDate BETWEEN ? AND ?
#             """, daily_sales_gas, well_name, month_start_date, month_end_date)
#         
#         # Update Condensate - Sales Production
#         cursor.execute("""
#             UPDATE PCE_CDA 
#             SET [Condensate - Sales Production] = ? * [Condensate_WH_Production]
#             WHERE [Well Name] = ? 
#             AND ProdDate BETWEEN ? AND ?
#         """, wh_to_sales_cond_val, well_name, month_start_date, month_end_date)
#         
#         # Update Sales CGR Ratio
#         cursor.execute("""
#             UPDATE PCE_CDA 
#             SET [Sales CGR Ratio] = 
#                 IIF([Gas - Sales Production] > 0, 
#                     [Condensate - Sales Production] / [Gas - Sales Production], 
#                     0)
#             WHERE [Well Name] = ? 
#             AND ProdDate BETWEEN ? AND ?
#         """, well_name, month_start_date, month_end_date)
#         
#         cda_updated += 1
#         
#     except Exception as e:
#         cda_errors += 1
# 
# conn.commit()
# log(f"   Updated {cda_updated} wells in PCE_CDA")
# if cda_errors > 0:
#     log(f"   Errors during PCE_CDA update: {cda_errors}")

        conn.close()
        progress(100)
        
        # -----------------------------------------------------------------
        # RETURN SUMMARY
        # -----------------------------------------------------------------
        total_time = time.time() - total_start
        
        summary = {
            'valnav_records': len(valnav_data),
            'accumap_records': len(accumap_data),
            'matched_wells': len(matched_wells) - wells_added,
            'wells_added': wells_added,
            'total_wells': len(matched_wells),
            'duration': total_time,
            'warnings': ', '.join(warning_messages) if warning_messages else None
        }
        
        log("\n" + "="*80)
        log("LOAD COMPLETE!")
        log("="*80)
        
        return summary
        
    except Exception as e:
        error_msg = f"ERROR: {str(e)}"
        log(error_msg)
        log(traceback.format_exc())
        return {"error": error_msg}