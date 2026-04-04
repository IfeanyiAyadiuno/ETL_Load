import pandas as pd
from datetime import datetime
import os
import sys
import traceback
import re

import log_format as lf
from db_connection import get_sql_conn

def clean_well_name(name):
    """Clean well name by removing extra spaces and normalizing"""
    if pd.isna(name) or not isinstance(name, str):
        return name
    
    # Remove leading/trailing spaces
    cleaned = name.strip()
    
    # Replace multiple spaces with single space
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    # Remove spaces around dashes (optional - sometimes helps)
    cleaned = re.sub(r'\s*-\s*', '-', cleaned)
    
    return cleaned

def import_surveys(excel_path, import_mode="append", progress_callback=None, log_callback=None):
    """
    Import survey data from Excel to SQL Server
    
    Args:
        excel_path: Path to Excel file
        import_mode: 'append', 'overwrite', or 'merge'
        progress_callback: Function for progress updates (0-100)
        log_callback: Function for log messages
    
    Returns:
        dict: Import statistics
    """
    
    def log(message):
        if log_callback:
            log_callback(message)
        else:
            print(message)
    
    def progress(value):
        if progress_callback:
            progress_callback(value)
    
    conn = None
    try:
        log(
            lf.header(
                "SURVEY DATA IMPORT",
                File=os.path.basename(excel_path),
                Mode=import_mode,
            )
        )
        # -----------------------------------------------------------------
        # STEP 1: Read Excel file
        # -----------------------------------------------------------------
        log(lf.step("Reading Excel file"))
        df = pd.read_excel(excel_path)
        log(lf.detail(f"Read {lf.num(len(df))} rows from Excel"))
        progress(10)
        
        # -----------------------------------------------------------------
        # STEP 2: Rename columns to match database
        # -----------------------------------------------------------------
        log(lf.step("Mapping columns"))
        
        column_mapping = {
            'Well name':              'Well Name',
            'Well Unique Identifier': 'UWI',
            'Subsea Elevation':       'Subsea Elevation',
            'Inclination':            'Inclination',
            'Azimuth Angle':          'Azimuth Angle',
            'Measured Depth':         'Measured Depth',
            'True Vertical Depth':    'True Vertical Depth',
            'Offset in EW':           'Offset in EW',
            'Offset in NS':           'Offset in NS',
            'East':                   'East',
            'North':                  'North',
            'PAD':                    'PAD',
        }
        
        df.rename(columns=column_mapping, inplace=True)
        log(lf.detail(f"Mapped {lf.num(len(column_mapping))} columns"))
        progress(20)
        
        # -----------------------------------------------------------------
        # STEP 3: Clean the data
        # -----------------------------------------------------------------
        log(lf.step("Cleaning data"))
        
        # Clean Well Name
        df['Well Name Cleaned'] = df['Well Name'].apply(clean_well_name)
        log(lf.detail("Cleaned Well Name column"))
        
        # Show sample of cleaned names
        sample_df = df[['Well Name', 'Well Name Cleaned']].head(3)
        for _, row in sample_df.iterrows():
            log(lf.item(f"'{row['Well Name']}' → '{row['Well Name Cleaned']}'"))
        
        progress(30)
        
        # -----------------------------------------------------------------
        # STEP 4: Validate required columns
        # -----------------------------------------------------------------
        log(lf.step("Validating data"))
        required_cols = [
            'Well Name', 'UWI', 'Subsea Elevation', 
            'Inclination', 'Azimuth Angle', 'Measured Depth', 
            'True Vertical Depth', 'Offset in EW', 'Offset in NS', 
            'East', 'North', 'PAD'
        ]
        
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            error_msg = f"Missing required columns: {missing_cols}"
            log(lf.error(error_msg))
            return {"error": error_msg}
        
        log(lf.detail("All required columns present"))
        
        # Check for nulls in required columns
        null_counts = df[required_cols].isnull().sum()
        if null_counts.sum() > 0:
            log(lf.warn("Null values found in required columns:"))
            for col in required_cols:
                if null_counts[col] > 0:
                    log(lf.item(f"{col}: {lf.num(int(null_counts[col]))} nulls"))
        
        progress(40)
        
        # -----------------------------------------------------------------
        # STEP 5: Match wells to database using cleaned names
        # -----------------------------------------------------------------
        log(lf.step("Matching wells to database"))
        
        conn = get_sql_conn()
        
        # Get all valid base composite names from PCE_WM (also clean them)
        valid_wells_df = pd.read_sql("""
            SELECT DISTINCT [Base Composite Name] 
            FROM PCE_WM 
            WHERE [Base Composite Name] IS NOT NULL
              AND ([Exception] IS NULL OR [Exception] = '' OR [Exception] = 'N')
        """, conn)
        
        # Clean the database names too
        valid_wells_df['Cleaned Name'] = valid_wells_df['Base Composite Name'].apply(clean_well_name)
        valid_wells = set(valid_wells_df['Cleaned Name'].tolist())
        
        log(lf.detail(f"Found {lf.num(len(valid_wells))} valid wells in database"))
        
        # Show sample of database names
        db_samples = list(valid_wells)[:3]
        log(lf.detail(f"Sample DB names: {db_samples}"))
        
        # Check which wells match using cleaned names
        df['Well Found'] = df['Well Name Cleaned'].isin(valid_wells)
        matched_df = df[df['Well Found']].copy()
        unmatched_df = df[~df['Well Found']].copy()
        
        log(lf.success(f"{lf.num(len(matched_df))} rows matched to database wells"))
        log(lf.warn(f"{lf.num(len(unmatched_df))} rows did not match"))
        
        if not unmatched_df.empty:
            # Show sample of unmatched wells
            log(lf.detail("Sample unmatched wells (first 10):"))
            for name in unmatched_df['Well Name Cleaned'].dropna().unique()[:10]:
                log(lf.item(f"'{name}'"))
            
            # Save unmatched wells to file for review
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            unmatched_file = f"unmatched_survey_wells_{timestamp}.csv"
            unmatched_df[['Well Name', 'Well Name Cleaned', 'UWI']].drop_duplicates().to_csv(unmatched_file, index=False)
            log(lf.detail(f"Unmatched wells saved to: {unmatched_file}"))
        
        progress(50)
        
        if matched_df.empty:
            log(lf.error("No matching wells to import"))
            return {
                'total_rows': len(df),
                'matched': 0,
                'unmatched': len(unmatched_df),
                'inserted': 0,
                'duplicates': 0,
                'errors': 0
            }
        
        # -----------------------------------------------------------------
        # STEP 6: Handle duplicates based on import mode
        # -----------------------------------------------------------------
        log(lf.step(f"Processing with mode: {import_mode}"))
        
        cursor = conn.cursor()
        
        if import_mode == "overwrite" or import_mode == "rewrite":
            uwis = matched_df['UWI'].unique().tolist()
            total_deleted = 0
            # Batched DELETE with IN clause instead of per-UWI round-trips
            batch_size = 500
            for i in range(0, len(uwis), batch_size):
                batch = uwis[i:i + batch_size]
                placeholders = ','.join(['?'] * len(batch))
                cursor.execute(f"DELETE FROM PCE_Surveys WHERE UWI IN ({placeholders})", batch)
                total_deleted += cursor.rowcount
            conn.commit()
            log(lf.detail(f"Deleted {lf.num(total_deleted)} existing records for {lf.num(len(uwis))} wells"))
        
        elif import_mode == "append":
            # Check for existing records and filter them out
            log(lf.detail("Checking for existing records..."))
            
            # Get unique UWIs from the data we're importing
            uwis_to_check = matched_df['UWI'].dropna().unique().tolist()
            
            # Initialize skipped_count for summary
            skipped_count = 0
            original_matched_count = len(matched_df)
            
            if uwis_to_check:
                # Query existing records for these UWIs
                # Create a set of tuples (UWI, Station Number) to identify existing records
                # Assuming UWI + Station Number is the unique key combination
                placeholders = ','.join(['?' for _ in uwis_to_check])
                existing_query = f"""
                    SELECT [UWI], [Measured Depth]
                    FROM PCE_Surveys
                    WHERE [UWI] IN ({placeholders})
                """
                cursor.execute(existing_query, uwis_to_check)
                existing_records = cursor.fetchall()
                
                existing_df = pd.DataFrame(existing_records, columns=['_ex_uwi', '_ex_depth'])
                existing_df['_ex_uwi'] = existing_df['_ex_uwi'].astype(str).str.strip()
                existing_df['_ex_depth'] = pd.to_numeric(existing_df['_ex_depth'], errors='coerce')

                log(lf.detail(f"Found {lf.num(len(existing_df))} existing records in database"))

                # Vectorized anti-join instead of per-row apply()
                matched_df['_uwi_key'] = matched_df['UWI'].astype(str).str.strip()
                matched_df['_depth_key'] = pd.to_numeric(matched_df['Measured Depth'], errors='coerce')

                before_count = len(matched_df)
                merged = matched_df.merge(
                    existing_df,
                    left_on=['_uwi_key', '_depth_key'],
                    right_on=['_ex_uwi', '_ex_depth'],
                    how='left',
                    indicator=True,
                )
                matched_df = merged[merged['_merge'] == 'left_only'].drop(
                    columns=['_ex_uwi', '_ex_depth', '_merge', '_uwi_key', '_depth_key'],
                ).copy()
                after_count = len(matched_df)
                skipped_count = before_count - after_count
                
                log(lf.detail(f"Filtered out {lf.num(skipped_count)} existing records"))
                log(lf.detail(f"{lf.num(after_count)} new records to insert"))
                
                if matched_df.empty:
                    log(lf.detail("No new records to insert. All records already exist in database."))
                    return {
                        'total_rows': len(df),
                        'matched': original_matched_count,
                        'unmatched': len(unmatched_df),
                        'inserted': 0,
                        'duplicates': skipped_count,
                        'errors': 0
                    }
        
        progress(60)
        
        # -----------------------------------------------------------------
        # STEP 7: Insert data
        # -----------------------------------------------------------------
        log(lf.step("Inserting data into database"))
        error_count = 0
        
        # Prepare insert SQL
        insert_sql = """
        INSERT INTO PCE_Surveys (
            [UWI], [Well Name],
            [Subsea Elevation],
            [Inclination], [Azimuth Angle],
            [Measured Depth], [True Vertical Depth],
            [Offset in EW], [Offset in NS],
            [East], [North],
            [PAD], [SourceFile]
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Vectorized tuple construction
        insert_cols = [
            'UWI', 'Well Name Cleaned', 'Subsea Elevation',
            'Inclination', 'Azimuth Angle', 'Measured Depth',
            'True Vertical Depth', 'Offset in EW', 'Offset in NS',
            'East', 'North', 'PAD',
        ]
        sub = matched_df[insert_cols].astype(object)
        sub[sub.isna()] = None
        source_file = os.path.basename(excel_path)
        rows_to_insert = [
            tuple(row) + (source_file,)
            for row in sub.itertuples(index=False, name=None)
        ]

        cursor.fast_executemany = True
        batch_size = 5000
        total_inserted = 0
        duplicate_skipped = 0
        total_rows = len(rows_to_insert)

        for i in range(0, total_rows, batch_size):
            batch = rows_to_insert[i:i + batch_size]
            try:
                cursor.executemany(insert_sql, batch)
                total_inserted += len(batch)
            except Exception:
                for row in batch:
                    try:
                        cursor.execute(insert_sql, row)
                        total_inserted += 1
                    except Exception as e:
                        if "Violation of UNIQUE KEY" in str(e):
                            duplicate_skipped += 1
                        else:
                            error_count += 1
                            log(lf.error(f"{str(e)[:100]}"))
            conn.commit()

            if (i + len(batch)) % 5000 == 0 or (i + len(batch)) >= total_rows:
                pct = int((i + len(batch)) / total_rows * 100) if total_rows else 0
                progress(60 + int(pct * 0.4))
                log(lf.detail(f"Progress: {lf.num(min(i + len(batch), total_rows))}/{lf.num(total_rows)} rows"))
        
        progress(100)
        
        # -----------------------------------------------------------------
        # STEP 8: Return summary
        # -----------------------------------------------------------------
        # For append mode, use skipped_count if it was calculated, otherwise use duplicate_skipped
        if import_mode == "append" and 'skipped_count' in locals():
            duplicates_final = skipped_count
            # Original matched count includes both new and skipped
            matched_final = original_matched_count
        else:
            duplicates_final = duplicate_skipped
            matched_final = len(matched_df) + duplicate_skipped if duplicate_skipped > 0 else len(matched_df)
        
        summary = {
            'total_rows': len(df),
            'matched': matched_final,
            'unmatched': len(unmatched_df),
            'inserted': total_inserted,
            'duplicates': duplicates_final,
            'errors': error_count,
        }
        
        log(
            lf.summary(
                "IMPORT COMPLETE",
                {
                    "Total rows in file": summary["total_rows"],
                    "Rows matched to wells": summary["matched"],
                    "Rows unmatched": summary["unmatched"],
                    "Rows inserted": summary["inserted"],
                    "Duplicates skipped": summary["duplicates"],
                    "Errors": summary["errors"],
                },
            )
        )
        
        return summary
        
    except Exception as e:
        log(lf.error(str(e)))
        traceback.print_exc()
        return {"error": str(e)}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

def main():
    """Command-line entry point"""
    if len(sys.argv) < 2:
        print("Usage: python survey_import.py <excel_file_path> [mode]")
        print("Modes: append (default), overwrite, merge")
        return
    
    excel_path = sys.argv[1]
    mode = sys.argv[2] if len(sys.argv) > 2 else "append"
    
    import_surveys(excel_path, mode)

if __name__ == "__main__":
    main()