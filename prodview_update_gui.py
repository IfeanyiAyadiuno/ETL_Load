import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from db_connection import get_sql_conn
from snowflake_connector import SnowflakeConnector

def run_prodview_update(start_month, end_month, progress_callback=None, log_callback=None):
    """
    Update production data from Snowflake for a range of months
    
    Args:
        start_month: Start month in format "MMM YYYY" (e.g., "Jan 2024")
        end_month: End month in format "MMM YYYY" (e.g., "Feb 2026")
        progress_callback: Function to call with progress percentage (0-100)
        log_callback: Function to call with log messages
    
    Returns:
        dict: Summary statistics
    """
    
    def log(message):
        if log_callback:
            log_callback(message)
        else:
            print(message)
    
    def progress(value):
        if progress_callback:
            progress_callback(value)
    
    log("\n" + "="*80)
    log("PRODVIEW/SNOWFLAKE DAILY PRODUCTION RETRIEVE")
    log("="*80)
    log(f"Range: {start_month} to {end_month}")
    
    total_start = time.time()
    
    try:
        # Parse months
        start_date = datetime.strptime(start_month, "%b %Y")
        end_date = datetime.strptime(end_month, "%b %Y")
        
        # Ensure start is before end
        if start_date > end_date:
            error_msg = "Start month must be before end month"
            log(f"ERROR: {error_msg}")
            return {"error": error_msg}
        
        # Connect to SQL Server
        log("\nConnecting to SQL Server...")
        conn = get_sql_conn()
        cursor = conn.cursor()
        log("✅ Database connected")
        
        # Get well mapping from PCE_WM
        log("\nFetching well mapping from PCE_WM...")
        cursor.execute("""
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
        """)
        
        mapping_rows = cursor.fetchall()
        mapping = []
        for row in mapping_rows:
            mapping.append({
                'gas_idrec': row[0],
                'pressures_idrec': row[1],
                'well_name': row[2],
                'formation': row[3],
                'layer': row[4],
                'fault_block': row[5],
                'pad_name': row[6],
                'lateral_length': row[7],
                'orient': row[8]
            })
        log(f"   Loaded {len(mapping)} wells")
        
        # Generate list of months to process
        current = start_date
        months_to_process = []
        while current <= end_date:
            months_to_process.append(current)
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)
        
        total_months = len(months_to_process)
        log(f"Found {total_months} months to process")
        
        if total_months == 0:
            return {
                'months_processed': 0,
                'wells_updated': 0,
                'cda_records': 0,
                'production_records': 0,
                'duration': 0
            }
        
        # Initialize counters
        months_processed = 0
        total_cda_records = 0
        total_production_records = 0
        
        # Process each month
        for month_idx, month_date in enumerate(months_to_process):
            month_start = month_date.replace(day=1)
            month_name = month_start.strftime('%B %Y')
            
            # Calculate month end
            if month_start.month == 12:
                month_end = datetime(month_start.year + 1, 1, 1) - timedelta(days=1)
            else:
                month_end = datetime(month_start.year, month_start.month + 1, 1) - timedelta(days=1)
            
            month_start_date = month_start.date()
            month_end_date = month_end.date()
            days_in_month = (month_end_date - month_start_date).days + 1
            
            log(f"\n{'='*60}")
            log(f"Processing {month_name}...")
            log(f"Range: {month_start_date} to {month_end_date}")
            log(f"Month {month_idx + 1} of {total_months}")
            
            # -----------------------------------------------------------------
            # STEP 1: Pull data from Snowflake
            # -----------------------------------------------------------------
            log("  Pulling data from Snowflake...")
            
            sf = SnowflakeConnector()
            
            # Pull ECF data
            ecf_query = f"""
            SELECT
                IDRECPARENT AS GasIDREC,
                CAST (DTTM AS DATE) AS ProdDate,
                EFFLUENTFACTOR AS ECF_Ratio
            FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitMeterOrificeEcf
            WHERE DTTM >= '{month_start_date}'
              AND DTTM <= '{month_end_date}'
            """
            ecf_df = sf.query(ecf_query)
            
            # Pull GasWH data
            gaswh_query = f"""
            SELECT
                IDRECPARENT AS GasIDREC,
                CAST(DTTM AS DATE) AS ProdDate,
                VOLENTERGAS AS GasWH_Production,
                DURONOR AS OnProdHours
            FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitMeterOrificeEntry
            WHERE DTTM >= '{month_start_date}'
              AND DTTM <= '{month_end_date}'
            """
            gaswh_df = sf.query(gaswh_query)
            
            # Pull CGR data
            cgr_query = f"""
            SELECT
                IDRECCOMP AS PressuresIDREC,
                CAST(DTTM AS DATE) AS ProdDate,
                CASE
                    WHEN RATEGAS IS NULL OR RATEGAS = 0 THEN NULL
                    ELSE (RATEHCLIQ / RATEGAS)
                END AS CGR_Ratio
            FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitCompGathMonthDayCalc
            WHERE DTTM >= '{month_start_date}'
              AND DTTM <= '{month_end_date}'
            """
            cgr_df = sf.query(cgr_query)
            
            # Pull WGR data
            wgr_query = f"""
            SELECT
                IDRECPARENT AS PressuresIDREC,
                CAST(DTTM AS DATE) AS ProdDate,
                WGR AS WGR_Ratio
            FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitCompRatios
            WHERE DTTM >= '{month_start_date}'
              AND DTTM <= '{month_end_date}'
            """
            wgr_df = sf.query(wgr_query)
            
            # Pull Pressures data
            pressures_query = f"""
            SELECT
                IDRECPARENT AS PressuresIDREC,
                CAST(DTTM AS DATE) AS ProdDate,
                PRESTUB AS TubingPressure,
                PRESCAS AS CasingPressure,
                SZCHOKE AS ChokeSize
            FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitCompParam
            WHERE DTTM >= '{month_start_date}'
              AND DTTM <= '{month_end_date}'
            """
            pressures_df = sf.query(pressures_query)
            
            # Pull Allocations data
            alloc_query = f"""
            SELECT
                IDRECCOMP AS PressuresIDREC,
                CAST(DTTM AS DATE) AS ProdDate,
                VOLPRODGATHGAS AS Gathered_Gas_Production,
                VOLPRODGATHHCLIQ AS Gathered_Condensate_Production,
                VOLNEWPRODALLOCNGL AS NGL_Production
            FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvunitallocmonthday
            WHERE DTTM >= '{month_start_date}'
              AND DTTM <= '{month_end_date}'
            """
            alloc_df = sf.query(alloc_query)
            
            # Pull Allocated Water data
            water_query = f"""
            SELECT
                IDRECCOMP AS PressuresIDREC,
                CAST(DTTM AS DATE) AS ProdDate,
                VOLWATER AS AllocatedWater_Rate
            FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvunitcompgathmonthdaycalc
            WHERE DTTM >= '{month_start_date}'
              AND DTTM <= '{month_end_date}'
            """
            water_df = sf.query(water_query)
            
            sf.close()
            
            log(f"    ECF: {len(ecf_df)} rows")
            log(f"    GasWH: {len(gaswh_df)} rows")
            log(f"    CGR: {len(cgr_df)} rows")
            log(f"    WGR: {len(wgr_df)} rows")
            log(f"    Pressures: {len(pressures_df)} rows")
            log(f"    Allocations: {len(alloc_df)} rows")
            log(f"    Water: {len(water_df)} rows")
            
            # -----------------------------------------------------------------
            # STEP 2: Delete existing data for this month
            # -----------------------------------------------------------------
            log("  Clearing existing data for month...")
            
            # Delete from PCE_CDA
            cursor.execute("""
                DELETE FROM PCE_CDA 
                WHERE ProdDate BETWEEN ? AND ?
            """, month_start_date, month_end_date)
            deleted_cda = cursor.rowcount
            log(f"    Deleted {deleted_cda} records from PCE_CDA")
            
            # Delete from PCE_Production
            cursor.execute("""
                DELETE FROM PCE_Production 
                WHERE [Date] BETWEEN ? AND ?
            """, month_start_date, month_end_date)
            deleted_prod = cursor.rowcount
            log(f"    Deleted {deleted_prod} records from PCE_Production")
            
            conn.commit()
            
            # -----------------------------------------------------------------
            # STEP 3: Build spine and insert into PCE_CDA
            # -----------------------------------------------------------------
            log("  Building daily data spine...")

            # Create date spine for each well
            all_rows = []
            date_range = pd.date_range(start=month_start_date, end=month_end_date, freq='D').date

            for well in mapping:
                for date in date_range:
                    all_rows.append({
                        'GasIDREC': well['gas_idrec'],
                        'PressuresIDREC': well['pressures_idrec'],
                        'Well Name': well['well_name'],
                        'ProdDate': date,
                        'Formation Producer': well['formation'],
                        'Layer Producer': well['layer'],
                        'Fault Block': well['fault_block'],
                        'Pad Name': well['pad_name'],
                        'Lateral Length': well['lateral_length'],
                        'Orient': well['orient']
                    })

            spine_df = pd.DataFrame(all_rows)
            log(f"    Created spine with {len(spine_df)} rows")

            # -----------------------------------------------------------------
            # STEP 4: Process and merge each data source
            # -----------------------------------------------------------------
            log("  Processing and merging data sources...")

            # Start with spine
            result_df = spine_df.copy()

            # Helper function to clean and prepare dataframes
            def prepare_df(df, id_col, date_col, value_cols):
                if df.empty:
                    return pd.DataFrame()
                
                # Handle Snowflake column naming (they come back as uppercase)
                df_clean = df.copy()
                column_map = {col.upper(): col for col in df_clean.columns}
                
                # Map to standard names
                result = pd.DataFrame()
                result['GasIDREC'] = df_clean[column_map.get(id_col.upper(), id_col)].astype(str).str.strip()
                result['ProdDate'] = pd.to_datetime(df_clean[column_map.get(date_col.upper(), date_col)]).dt.date
                
                for val_col in value_cols:
                    source_col = column_map.get(val_col.upper(), val_col)
                    if source_col in df_clean.columns:
                        result[val_col] = pd.to_numeric(df_clean[source_col], errors='coerce')
                    else:
                        result[val_col] = None
                
                # Remove duplicates (keep last)
                result = result.sort_values(['GasIDREC', 'ProdDate'])
                result = result.groupby(['GasIDREC', 'ProdDate'], as_index=False).last()
                
                return result

            # Process ECF
            if not ecf_df.empty:
                log("    Processing ECF data...")
                ecf_processed = prepare_df(ecf_df, 'GASIDREC', 'PRODDATE', ['ECF_Ratio'])
                if not ecf_processed.empty:
                    result_df = result_df.merge(ecf_processed, on=['GasIDREC', 'ProdDate'], how='left')
                else:
                    result_df['ECF_Ratio'] = None
            else:
                result_df['ECF_Ratio'] = None

            # Process GasWH
            if not gaswh_df.empty:
                log("    Processing GasWH data...")
                gaswh_processed = prepare_df(gaswh_df, 'GASIDREC', 'PRODDATE', ['GasWH_Production', 'OnProdHours'])
                if not gaswh_processed.empty:
                    result_df = result_df.merge(gaswh_processed, on=['GasIDREC', 'ProdDate'], how='left')
                else:
                    result_df['GasWH_Production'] = None
                    result_df['OnProdHours'] = None
            else:
                result_df['GasWH_Production'] = None
                result_df['OnProdHours'] = None

            # Process CGR (uses PressuresIDREC)
            if not cgr_df.empty:
                log("    Processing CGR data...")
                cgr_processed = prepare_df(cgr_df, 'PRESSURESIDREC', 'PRODDATE', ['CGR_Ratio'])
                if not cgr_processed.empty:
                    cgr_processed = cgr_processed.rename(columns={'GasIDREC': 'PressuresIDREC'})
                    result_df = result_df.merge(cgr_processed, left_on=['PressuresIDREC', 'ProdDate'], 
                                                right_on=['PressuresIDREC', 'ProdDate'], how='left')
                else:
                    result_df['CGR_Ratio'] = None
            else:
                result_df['CGR_Ratio'] = None

            # Process WGR (uses PressuresIDREC)
            if not wgr_df.empty:
                log("    Processing WGR data...")
                wgr_processed = prepare_df(wgr_df, 'PRESSURESIDREC', 'PRODDATE', ['WGR_Ratio'])
                if not wgr_processed.empty:
                    wgr_processed = wgr_processed.rename(columns={'GasIDREC': 'PressuresIDREC'})
                    result_df = result_df.merge(wgr_processed, left_on=['PressuresIDREC', 'ProdDate'], 
                                                right_on=['PressuresIDREC', 'ProdDate'], how='left')
                else:
                    result_df['WGR_Ratio'] = None
            else:
                result_df['WGR_Ratio'] = None

            # Process Pressures (uses PressuresIDREC)
            if not pressures_df.empty:
                log("    Processing Pressures data...")
                pressures_processed = prepare_df(pressures_df, 'PRESSURESIDREC', 'PRODDATE', 
                                                ['TubingPressure', 'CasingPressure', 'ChokeSize'])
                if not pressures_processed.empty:
                    pressures_processed = pressures_processed.rename(columns={'GasIDREC': 'PressuresIDREC'})
                    result_df = result_df.merge(pressures_processed, left_on=['PressuresIDREC', 'ProdDate'], 
                                                right_on=['PressuresIDREC', 'ProdDate'], how='left')
                else:
                    result_df['TubingPressure'] = None
                    result_df['CasingPressure'] = None
                    result_df['ChokeSize'] = None
            else:
                result_df['TubingPressure'] = None
                result_df['CasingPressure'] = None
                result_df['ChokeSize'] = None

            # Process Allocations (uses PressuresIDREC)
            if not alloc_df.empty:
                log("    Processing Allocations data...")
                alloc_processed = prepare_df(alloc_df, 'PRESSURESIDREC', 'PRODDATE', 
                                            ['Gathered_Gas_Production', 'Gathered_Condensate_Production', 'NGL_Production'])
                if not alloc_processed.empty:
                    alloc_processed = alloc_processed.rename(columns={'GasIDREC': 'PressuresIDREC'})
                    result_df = result_df.merge(alloc_processed, left_on=['PressuresIDREC', 'ProdDate'], 
                                                right_on=['PressuresIDREC', 'ProdDate'], how='left')
                else:
                    result_df['Gathered_Gas_Production'] = None
                    result_df['Gathered_Condensate_Production'] = None
                    result_df['NGL_Production'] = None
            else:
                result_df['Gathered_Gas_Production'] = None
                result_df['Gathered_Condensate_Production'] = None
                result_df['NGL_Production'] = None

            # Process Allocated Water (uses PressuresIDREC)
            if not water_df.empty:
                log("    Processing Allocated Water data...")
                water_processed = prepare_df(water_df, 'PRESSURESIDREC', 'PRODDATE', ['AllocatedWater_Rate'])
                if not water_processed.empty:
                    water_processed = water_processed.rename(columns={'GasIDREC': 'PressuresIDREC'})
                    result_df = result_df.merge(water_processed, left_on=['PressuresIDREC', 'ProdDate'], 
                                                right_on=['PressuresIDREC', 'ProdDate'], how='left')
                else:
                    result_df['AllocatedWater_Rate'] = None
            else:
                result_df['AllocatedWater_Rate'] = None

            # Calculate Condensate_WH_Production
            result_df['Condensate_WH_Production'] = result_df['GasWH_Production'] * result_df['CGR_Ratio']

            # Check if we have any data
            has_data = False
            for col in ['GasWH_Production', 'ECF_Ratio', 'CGR_Ratio', 'Gathered_Gas_Production']:
                if col in result_df.columns and result_df[col].notna().any():
                    has_data = True
                    break

            if not has_data:
                log("    ⚠️ WARNING: No data found for this month!")
            else:
                log(f"    Merged dataframe has {len(result_df)} rows")
                log(f"    Sample - GasWH_Production non-null: {result_df['GasWH_Production'].notna().sum()} rows")
            
            # -----------------------------------------------------------------
            # STEP 5: Insert into PCE_CDA
            # -----------------------------------------------------------------
            log("  Inserting into PCE_CDA...")

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

            rows_inserted = 0
            batch_size = 1000
            rows_batch = []

            for _, row in result_df.iterrows():
                rows_batch.append((
                    row.get('GasIDREC'),
                    row.get('PressuresIDREC'),
                    row.get('Well Name'),
                    row.get('ProdDate'),
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
                    row.get('Formation Producer'),
                    row.get('Layer Producer'),
                    row.get('Fault Block'),
                    row.get('Pad Name'),
                    None if pd.isna(row.get('Lateral Length')) else float(row.get('Lateral Length')),
                    row.get('Orient')
                ))
                
                if len(rows_batch) >= batch_size:
                    cursor.executemany(insert_sql, rows_batch)
                    rows_inserted += len(rows_batch)
                    rows_batch = []
                    log(f"    Inserted batch of {batch_size} rows...")

            # Insert remaining rows
            if rows_batch:
                cursor.executemany(insert_sql, rows_batch)
                rows_inserted += len(rows_batch)

            conn.commit()
            total_cda_records += rows_inserted
            log(f"  ✅ Inserted {rows_inserted} records into PCE_CDA")

            # -----------------------------------------------------------------
            # STEP 6: Update PCE_Production
            # -----------------------------------------------------------------
            log("  Updating PCE_Production...")

            # Insert with temporary sequence values
            cursor.execute("""
                INSERT INTO PCE_Production (
                    [Date], [Well Name],
                    [Days Seq], [Day Seq UPRT],
                    [Gas WH Production (10³m³)], [Condensate WH (m³/d)],
                    [Gas S2 Production (10³m³)], [Gas Sales Production (10³m³)],
                    [Condensate Sales (m³/d)], [Gathered Gas (e³m³/d)],
                    [Gathered Condensate (m³/d)], [Sales CGR (m³/e³m³)],
                    [CGR (m³/e³m³)], [WGR (m³/e³m³)], [ECF],
                    [Hours On], [Tubing Pressure (kPa)], [Casing Pressure (kPa)],
                    [Choke Size], 
                    [Alloc. Water Rate (m³)], [NGL (m³)],
                    [Formation Producer], [Layer Producer], [Fault Block],
                    [Pad Name], [Lateral Length], [Orientation]
                )
                SELECT 
                    c.ProdDate,
                    c.[Well Name],
                    0, 0,  -- Temporary sequence values
                    c.GasWH_Production,
                    c.Condensate_WH_Production,
                    c.[Gas - S2 Production],
                    c.[Gas - Sales Production],
                    c.[Condensate - Sales Production],
                    c.Gathered_Gas_Production,
                    c.Gathered_Condensate_Production,
                    c.[Sales CGR Ratio],
                    c.CGR_Ratio,
                    c.WGR_Ratio,
                    c.ECF_Ratio,
                    c.OnProdHours,
                    c.TubingPressure,
                    c.CasingPressure,
                    c.ChokeSize,
                    c.AllocatedWater_Rate,
                    c.NGL_Production,
                    c.[Formation Producer],
                    c.[Layer Producer],
                    c.[Fault Block],
                    c.[Pad Name],
                    c.[Lateral Length],
                    c.Orient
                FROM PCE_CDA c
                WHERE c.ProdDate BETWEEN ? AND ?
            """, month_start_date, month_end_date)

            prod_inserted = cursor.rowcount
            conn.commit()
            total_production_records += prod_inserted
            log(f"  ✅ Inserted {prod_inserted} records into PCE_Production")
            
            # Update overall progress
            progress_percent = int((month_idx + 1) / total_months * 100)
            progress(progress_percent)
        
        # -----------------------------------------------------------------
        # STEP 6: Recalculate sequences and cumulatives for affected wells
        # -----------------------------------------------------------------
        log("\n" + "="*60)
        log("Recalculating sequences and cumulatives...")
        
        # Get unique wells in the updated range
        cursor.execute("""
            SELECT DISTINCT [Well Name]
            FROM PCE_CDA
            WHERE ProdDate BETWEEN ? AND ?
        """, start_date.date(), end_date.date())
        
        affected_wells = [row[0] for row in cursor.fetchall()]
        log(f"Found {len(affected_wells)} wells to recalculate")
        
        for well_idx, well_name in enumerate(affected_wells):
            if well_idx % 10 == 0:
                log(f"  Processing well {well_idx + 1}/{len(affected_wells)}...")
            
            # Get all dates for this well in order
            cursor.execute("""
                SELECT ProdDate, GasWH_Production
                FROM PCE_CDA
                WHERE [Well Name] = ?
                ORDER BY ProdDate
            """, well_name)
            
            well_data = cursor.fetchall()
            
            # Calculate Days Seq (simple counter)
            days_seq = list(range(1, len(well_data) + 1))
            
            # Calculate Day Seq UPRT (repeats when production < 1)
            day_seq_uprt = []
            counter = 1
            i = 0
            gas_wh_values = [row[1] or 0 for row in well_data]
            
            while i < len(gas_wh_values):
                day_seq_uprt.append(counter)
                
                if gas_wh_values[i] >= 1:
                    counter += 1
                    i += 1
                else:
                    j = i + 1
                    while j < len(gas_wh_values) and gas_wh_values[j] < 1:
                        day_seq_uprt.append(counter)
                        j += 1
                    i = j
                    counter += 1
            
            # Update PCE_Production with sequence numbers
            for idx, (date, _) in enumerate(well_data):
                cursor.execute("""
                    UPDATE PCE_Production
                    SET [Days Seq] = ?,
                        [Day Seq UPRT] = ?
                    WHERE [Well Name] = ? AND [Date] = ?
                """, days_seq[idx], day_seq_uprt[idx], well_name, date)
            
            conn.commit()
        
        log("✅ Sequence recalculation complete")
        
        conn.close()
        
        total_time = time.time() - total_start
        
        summary = {
            'months_processed': months_processed,
            'wells_updated': len(affected_wells),
            'cda_records': total_cda_records,
            'production_records': total_production_records,
            'duration': total_time
        }
        
        log("\n" + "="*80)
        log("UPDATE COMPLETE!")
        log("="*80)
        log(f"Months processed: {months_processed}")
        log(f"Wells updated: {len(affected_wells)}")
        log(f"PCE_CDA records: {total_cda_records:,}")
        log(f"PCE_Production records: {total_production_records:,}")
        log(f"Total time: {total_time:.1f} seconds")
        
        return summary
        
    except Exception as e:
        error_msg = f"ERROR: {str(e)}"
        log(error_msg)
        import traceback
        log(traceback.format_exc())
        return {"error": error_msg}