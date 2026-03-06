import pandas as pd
import pyodbc
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
import os
import sys
import traceback
import re

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
    
    log("="*60)
    log("STARTING SURVEY DATA IMPORT")
    log(f"File: {excel_path}")
    log(f"Mode: {import_mode}")
    log("="*60)
    
    try:
        # -----------------------------------------------------------------
        # STEP 1: Read Excel file
        # -----------------------------------------------------------------
        log("\n📂 Reading Excel file...")
        df = pd.read_excel(excel_path)
        log(f"   Read {len(df)} rows from Excel")
        progress(10)
        
        # -----------------------------------------------------------------
        # STEP 2: Rename columns to match database
        # -----------------------------------------------------------------
        log("\n🔄 Mapping columns to database schema...")
        
        column_mapping = {
            'Well name': 'Well Name',
            'Well Unique Identifier': 'UWI',
            'Subsea Elevation': 'Subsea Elevation',
            'Surface Location Latitude (NAD83)': 'Surface Location Latitude (NAD83)',
            'Surface Location Longitude (NAD83)': 'Surface Location Longitude (NAD83)',
            'Surface Location Zone (NAD83)': 'Surface Location Zone (NAD83)',
            'Surface Location Easting (NAD83)': 'Surface Location Easting (NAD83)',
            'Surface Location Northing (NAD83)': 'Surface Location Northing (NAD83)',
            'Bottom Location Latitude (NAD83)': 'Bottom Location Latitude (NAD83)',
            'Bottom Location Longitude (NAD83)': 'Bottom Location Longitude (NAD83)',
            'Bottom Location Zone (NAD83)': 'Bottom Location Zone (NAD83)',
            'Bottom Location Easting (NAD83)': 'Bottom Location Easting (NAD83)',
            'Bottom Location Northing (NAD83)': 'Bottom Location Northing (NAD83)',
            'Total Station Number': 'Total Station Number',
            'Station Number': 'Station Number',
            'Inclination': 'Inclination',
            'Azimuth Angle': 'Azimuth Angle',
            'Measured Depth': 'Measured Depth',
            'True Vertical Depth': 'True Vertical Depth',
            'Offset in EW': 'Offset in EW',
            'Offset in NS': 'Offset in NS',
            'East': 'East',
            'North': 'North',
            'PAD': 'PAD'
        }
        
        df.rename(columns=column_mapping, inplace=True)
        log(f"   Mapped {len(column_mapping)} columns")
        progress(20)
        
        # -----------------------------------------------------------------
        # STEP 3: Clean the data
        # -----------------------------------------------------------------
        log("\n🧹 Cleaning data...")
        
        # Clean Well Name
        df['Well Name Cleaned'] = df['Well Name'].apply(clean_well_name)
        log(f"   Cleaned Well Name column")
        
        # Show sample of cleaned names
        sample_df = df[['Well Name', 'Well Name Cleaned']].head(3)
        for _, row in sample_df.iterrows():
            log(f"      '{row['Well Name']}' → '{row['Well Name Cleaned']}'")
        
        progress(30)
        
        # -----------------------------------------------------------------
        # STEP 4: Validate required columns
        # -----------------------------------------------------------------
        log("\n✅ Validating data...")
        required_cols = [
            'Well Name', 'UWI', 'Subsea Elevation', 
            'Inclination', 'Azimuth Angle', 'Measured Depth', 
            'True Vertical Depth', 'Offset in EW', 'Offset in NS', 
            'East', 'North', 'PAD'
        ]
        
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            error_msg = f"Missing required columns: {missing_cols}"
            log(f"❌ {error_msg}")
            return {"error": error_msg}
        
        log(f"   All required columns present")
        
        # Check for nulls in required columns
        null_counts = df[required_cols].isnull().sum()
        if null_counts.sum() > 0:
            log(f"⚠️  Warning: Null values found in required columns:")
            for col in required_cols:
                if null_counts[col] > 0:
                    log(f"      {col}: {null_counts[col]} nulls")
        
        progress(40)
        
        # -----------------------------------------------------------------
        # STEP 5: Match wells to database using cleaned names
        # -----------------------------------------------------------------
        log("\n🔗 Matching wells to database...")
        
        conn = get_sql_conn()
        
        # Get all valid base composite names from PCE_WM (also clean them)
        valid_wells_df = pd.read_sql("""
            SELECT DISTINCT [Base Composite Name] 
            FROM PCE_WM 
            WHERE [Base Composite Name] IS NOT NULL
        """, conn)
        
        # Clean the database names too
        valid_wells_df['Cleaned Name'] = valid_wells_df['Base Composite Name'].apply(clean_well_name)
        valid_wells = set(valid_wells_df['Cleaned Name'].tolist())
        
        log(f"   Found {len(valid_wells)} valid wells in database")
        
        # Show sample of database names
        db_samples = list(valid_wells)[:3]
        log(f"   Sample DB names: {db_samples}")
        
        # Check which wells match using cleaned names
        df['Well Found'] = df['Well Name Cleaned'].isin(valid_wells)
        matched_df = df[df['Well Found']].copy()
        unmatched_df = df[~df['Well Found']].copy()
        
        log(f"   ✓ {len(matched_df)} rows matched to database wells")
        log(f"   ⚠️ {len(unmatched_df)} rows did not match")
        
        if not unmatched_df.empty:
            # Show sample of unmatched wells
            log(f"\n   Sample unmatched wells (first 10):")
            for name in unmatched_df['Well Name Cleaned'].dropna().unique()[:10]:
                log(f"      - '{name}'")
            
            # Save unmatched wells to file for review
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            unmatched_file = f"unmatched_survey_wells_{timestamp}.csv"
            unmatched_df[['Well Name', 'Well Name Cleaned', 'UWI']].drop_duplicates().to_csv(unmatched_file, index=False)
            log(f"\n   Unmatched wells saved to: {unmatched_file}")
        
        progress(50)
        
        if matched_df.empty:
            log("\n❌ No matching wells to import")
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
        log(f"\n🔄 Processing with mode: {import_mode}")
        
        if import_mode == "overwrite":
            # Delete existing records for these UWI values
            uwis = matched_df['UWI'].unique().tolist()
            
            cursor = conn.cursor()
            for uwi in uwis:
                cursor.execute("DELETE FROM Surveys WHERE UWI = ?", uwi)
            deleted = cursor.rowcount
            conn.commit()
            log(f"   Deleted {deleted} existing records for {len(uwis)} wells")
        
        progress(60)
        
        # -----------------------------------------------------------------
        # STEP 7: Insert data
        # -----------------------------------------------------------------
        log("\n💾 Inserting data into database...")
        
        # Prepare insert SQL
        insert_sql = """
        INSERT INTO Surveys (
            [UWI], [Well Name],
            [Subsea Elevation],
            [Surface Location Latitude (NAD83)], [Surface Location Longitude (NAD83)],
            [Surface Location Zone (NAD83)], [Surface Location Easting (NAD83)],
            [Surface Location Northing (NAD83)],
            [Bottom Location Latitude (NAD83)], [Bottom Location Longitude (NAD83)],
            [Bottom Location Zone (NAD83)], [Bottom Location Easting (NAD83)],
            [Bottom Location Northing (NAD83)],
            [Total Station Number], [Station Number],
            [Inclination], [Azimuth Angle],
            [Measured Depth], [True Vertical Depth],
            [Offset in EW], [Offset in NS],
            [East], [North],
            [PAD], [SourceFile]
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Prepare rows for insertion
        rows_to_insert = []
        for _, row in matched_df.iterrows():
            rows_to_insert.append((
                row.get('UWI'),
                row.get('Well Name Cleaned'),  # Use the cleaned name
                row.get('Subsea Elevation'),
                row.get('Surface Location Latitude (NAD83)'),
                row.get('Surface Location Longitude (NAD83)'),
                row.get('Surface Location Zone (NAD83)'),
                row.get('Surface Location Easting (NAD83)'),
                row.get('Surface Location Northing (NAD83)'),
                row.get('Bottom Location Latitude (NAD83)'),
                row.get('Bottom Location Longitude (NAD83)'),
                row.get('Bottom Location Zone (NAD83)'),
                row.get('Bottom Location Easting (NAD83)'),
                row.get('Bottom Location Northing (NAD83)'),
                row.get('Total Station Number'),
                row.get('Station Number'),
                row.get('Inclination'),
                row.get('Azimuth Angle'),
                row.get('Measured Depth'),
                row.get('True Vertical Depth'),
                row.get('Offset in EW'),
                row.get('Offset in NS'),
                row.get('East'),
                row.get('North'),
                row.get('PAD'),
                os.path.basename(excel_path)  # SourceFile
            ))
        
        # Insert in batches
        batch_size = 1000
        total_inserted = 0
        duplicate_skipped = 0
        
        cursor = conn.cursor()
        
        for i in range(0, len(rows_to_insert), batch_size):
            batch = rows_to_insert[i:i + batch_size]
            
            for row in batch:
                try:
                    cursor.execute(insert_sql, row)
                    total_inserted += 1
                except Exception as e:
                    if "Violation of UNIQUE KEY" in str(e):
                        duplicate_skipped += 1
                    else:
                        log(f"      ❌ Error: {str(e)[:100]}")
            
            conn.commit()
            
            if (i + batch_size) % 5000 == 0 or (i + batch_size) >= len(rows_to_insert):
                pct = int((i + len(batch)) / len(rows_to_insert) * 100)
                progress(60 + int(pct * 0.4))
                log(f"      Progress: {min(i + batch_size, len(rows_to_insert))}/{len(rows_to_insert)} rows")
        
        conn.close()
        
        progress(100)
        
        # -----------------------------------------------------------------
        # STEP 8: Return summary
        # -----------------------------------------------------------------
        summary = {
            'total_rows': len(df),
            'matched': len(matched_df),
            'unmatched': len(unmatched_df),
            'inserted': total_inserted,
            'duplicates': duplicate_skipped,
            'errors': 0
        }
        
        log("\n" + "="*60)
        log("IMPORT COMPLETE!")
        log("="*60)
        log(f"Total rows in file: {summary['total_rows']}")
        log(f"Rows matched to wells: {summary['matched']}")
        log(f"Rows unmatched: {summary['unmatched']}")
        log(f"Rows inserted: {summary['inserted']}")
        log(f"Duplicates skipped: {summary['duplicates']}")
        
        return summary
        
    except Exception as e:
        log(f"\n❌ ERROR: {str(e)}")
        traceback.print_exc()
        return {"error": str(e)}

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