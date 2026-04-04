import pandas as pd
import os
import numpy as np
from datetime import datetime

import log_format as lf
from db_connection import get_sql_conn

def safe_float(value):
    """Safely convert any value to float or return None - handles N/A"""
    if value is None or pd.isna(value):
        return None
    try:
        if isinstance(value, str):
            value = value.strip().replace(',', '')
            # Handle common text values that aren't numbers
            if value.lower() in ['', 'nan', 'null', 'none', '-', 'n/a', 'na']:
                return None
        result = float(value)
        return None if np.isinf(result) or np.isnan(result) else round(result, 4)
    except (ValueError, TypeError):
        return None

def get_float_value(val):
    """Safely convert to float or None, handling numpy NaN"""
    if val is None:
        return None
    if isinstance(val, float) and np.isnan(val):
        return None
    return float(val)

def get_string_value(val):
    """Safely convert to string or None, handling NaN and N/A"""
    if val is None or pd.isna(val):
        return None
    if isinstance(val, float) and np.isnan(val):
        return None
    s = str(val).strip()
    if s.lower() in ['', 'nan', 'null', 'none', '-', 'n/a', 'na']:
        return None
    return s

def import_typecurves(excel_path, log_callback=None, progress_callback=None):
    """Import type curves from Excel file"""
    def log(msg):
        if log_callback:
            log_callback(msg)
        else:
            print(msg)
    
    def progress(value):
        if progress_callback:
            progress_callback(value)
    
    conn = None
    try:
        log(lf.step("Reading Excel file"))
        progress(5)
        df_raw = pd.read_excel(excel_path, header=None)
        df_data = df_raw.iloc[1:].copy().reset_index(drop=True)
        
        data = {
            'Well Name': df_data[3].astype(str).str.strip(),
            'Gas_mcf': df_data[6],
            'Gas_mmcf': df_data[7],
            'Cond_bbl': df_data[14],
            'Cond_mbbl': df_data[15],
            'Formation': df_data[25] if len(df_data.columns) > 25 else None,
            'Layer': df_data[26] if len(df_data.columns) > 26 else None,
            'Fault': df_data[27] if len(df_data.columns) > 27 else None,
            'Pad': df_data[28] if len(df_data.columns) > 28 else None,
            'Remarks': df_data[29] if len(df_data.columns) > 29 else None,
            'Lateral_raw': df_data[30] if len(df_data.columns) > 30 else None,
            'Reserves_raw': df_data[31] if len(df_data.columns) > 31 else None,
        }
        
        df = pd.DataFrame(data)
        df = df[df['Well Name'].notna() & (df['Well Name'] != '') & (df['Well Name'] != 'nan')]
        df = df[~df['Well Name'].str.contains('nan', case=False, na=False)]
        
        if len(df) == 0:
            log(lf.warn("No valid rows found"))
            return False
        
        for col in ['Gas_mcf', 'Gas_mmcf', 'Cond_bbl', 'Cond_mbbl', 'Lateral_raw', 'Reserves_raw']:
            df[col] = df[col].apply(safe_float)
        
        for col in ['Formation', 'Layer', 'Fault', 'Pad', 'Remarks']:
            df[col] = df[col].apply(lambda x: get_string_value(x))
        
        current_date = datetime.now().date()
        
        df['Gas_WH_Prod'] = np.where(df['Gas_mcf'].notna(), (df['Gas_mcf'] / 35.473).round(4), None)
        df['Gas_WH_Cum'] = np.where(df['Gas_mmcf'].notna(), (df['Gas_mmcf'] / 35.473).round(4), None)
        df['Gas_S2_Prod'] = np.where(df['Gas_mcf'].notna(), (df['Gas_mcf'] / 35.493998762).round(4), None)
        df['Gas_S2_Cum'] = np.where(df['Gas_mmcf'].notna(), ((df['Gas_mmcf'] * 1000) / 35.493998762).round(4), None)
        df['Cond_WH'] = np.where(df['Cond_bbl'].notna(), (df['Cond_bbl'] / 6.28981077).round(4), None)
        df['Cond_WH_Cum'] = np.where(df['Cond_mbbl'].notna(), ((df['Cond_mbbl'] * 1000) / 6.28981077).round(4), None)
        df['Cond_Sales'] = np.where(df['Cond_bbl'].notna(), (df['Cond_bbl'] / 6.293).round(4), None)
        df['Cond_Sales_Cum'] = np.where(df['Cond_mbbl'].notna(), ((df['Cond_mbbl'] * 1000) / 6.293).round(4), None)
        df['Gathered_Gas'] = df['Gas_WH_Prod']
        df['Gas_Gathered_Cum'] = df['Gas_WH_Cum']
        df['Gathered_Cond'] = df['Cond_WH']
        df['Cond_Gathered_Cum'] = df['Cond_WH_Cum']
        df['On_Prod_Year'] = np.where(df['Reserves_raw'].notna(), df['Reserves_raw'].astype(int), None)
        
        log(lf.detail(f"Processed {lf.num(len(df))} rows"))
        progress(30)
        log(lf.step("Connecting to database"))
        conn = get_sql_conn()
        conn.autocommit = False
        cursor = conn.cursor()
        cursor.fast_executemany = True
        progress(40)
        
        log(lf.step("Deleting existing type curve data"))
        cursor.execute("DELETE FROM dbo.PCE_Production WHERE [Well Name] LIKE 'YE2%'")
        log(lf.detail(f"Deleted {lf.num(cursor.rowcount)} rows"))
        progress(50)
        
        rows = []
        build_skip = 0
        for idx, row in df.iterrows():
            try:
                rows.append((
                    str(row['Well Name']), current_date, 0, 0,
                    get_float_value(row['Gas_WH_Prod']), get_float_value(row['Gas_WH_Cum']),
                    get_float_value(row['Gas_S2_Prod']), get_float_value(row['Gas_S2_Cum']),
                    get_float_value(row['Cond_WH']), get_float_value(row['Cond_WH_Cum']),
                    get_float_value(row['Cond_Sales']), get_float_value(row['Cond_Sales_Cum']),
                    get_float_value(row['Gathered_Gas']), get_float_value(row['Gas_Gathered_Cum']),
                    get_float_value(row['Gathered_Cond']), get_float_value(row['Cond_Gathered_Cum']),
                    None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                    row['Formation'], row['Layer'], row['Fault'], row['Pad'],
                    get_float_value(row['Lateral_raw']), None, row['On_Prod_Year'], row['Remarks']
                ))
            except Exception:
                build_skip += 1

        if build_skip:
            log(lf.warn(f"Skipped {lf.num(build_skip)} rows with errors while building insert data"))

        if len(rows) == 0:
            log(lf.warn("No rows to insert"))
            conn.commit()
            return False

        progress(60)
        insert_sql = """
        INSERT INTO dbo.PCE_Production (
            [Well Name], [Date], [Days Seq], [Day Seq UPRT],
            [Gas WH Production (10³m³)], [Gas WH Cumulative Production (10³m³)],
            [Gas S2 Production (10³m³)], [Gas S2 Cumulative Production (10³m³)],
            [Condensate WH (m³/d)], [Condensate WH Cumulative Production (m³)],
            [Condensate Sales (m³/d)], [Condensate Sales Cumulative Production (m³)],
            [Gathered Gas (e³m³/d)], [Gas Gathered Cumulative (e³m³)],
            [Gathered Condensate (m³/d)], [Condensate Gathered Cumulative (m³)],
            [Sales CGR (m³/e³m³)], [CGR (m³/e³m³)], [WGR (m³/e³m³)], [ECF],
            [Hours On], [Tubing Pressure (kPa)], [Casing Pressure (kPa)], [Choke Size],
            [Alloc. Water Rate (m³)], [NGL (m³)],
            [Gas WH Avg (10³m³)], [Gas S2 Avg (10³m³)],
            [Gas Gathered Avg (e³m³/d)], [Condensate Gathered Avg (m³/d)],
            [Formation Producer], [Layer Producer], [Fault Block],
            [Pad Name], [Lateral Length], [Orientation],
            [On Production Year], [Remarks]
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        batch_size = 250
        total_inserted = 0

        try:
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                cursor.executemany(insert_sql, batch)
                total_inserted += len(batch)
                log(lf.detail(f"Inserted {lf.num(total_inserted)}/{lf.num(len(rows))} rows..."))
                progress(60 + int((i + len(batch)) / len(rows) * 35))
            conn.commit()
        except Exception:
            conn.rollback()
            log(lf.warn("Batch failed, trying row-by-row"))
            total_inserted = 0
            cursor.execute("DELETE FROM dbo.PCE_Production WHERE [Well Name] LIKE 'YE2%'")
            row_skip = 0
            for row in rows:
                try:
                    cursor.execute(insert_sql, row)
                    total_inserted += 1
                except Exception:
                    row_skip += 1
            if row_skip:
                log(lf.warn(f"Skipped {lf.num(row_skip)} rows with errors"))
            conn.commit()
            progress(60 + int((total_inserted / len(rows)) * 35))

        progress(100)
        log(lf.success(f"Import complete: {lf.num(total_inserted)}/{lf.num(len(rows))} rows"))
        return True
        
    except Exception as e:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        log(lf.error(str(e)))
        import traceback
        log(traceback.format_exc())
        return False
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
