import pandas as pd
import pyodbc
import os
from dotenv import load_dotenv

load_dotenv()

# SQL Server connection settings
SQL_SERVER = os.getenv("SQL_SERVER", "CALVMSQL02")
SQL_DATABASE = os.getenv("SQL_DATABASE", "Re_Main_Production")
SQL_DRIVER = os.getenv("SQL_DRIVER", "{ODBC Driver 17 for SQL Server}")

def get_sql_conn():
    conn_str = (
        f'DRIVER={SQL_DRIVER};'
        f'SERVER={SQL_SERVER};'
        f'DATABASE={SQL_DATABASE};'
        f'Trusted_Connection=yes;'
    )
    return pyodbc.connect(conn_str)

def find_missing_wells(survey_excel_path):
    """
    Find which wells in PCE_WM are NOT in the survey Excel file
    Outputs a simple text file with the results
    """
    
    print("=" * 60)
    print("FINDING WELLS NOT IN DIRECTIONAL SURVEY")
    print("=" * 60)
    
    try:
        # -----------------------------------------------------------------
        # STEP 1: Read survey Excel file
        # -----------------------------------------------------------------
        print(f"\n📂 Reading survey file...")
        survey_df = pd.read_excel(survey_excel_path)
        
        # Find the well name column
        possible_columns = ['Well name', 'Well Name', 'WELL NAME', 'Well', 'WELL']
        survey_well_col = None
        for col in possible_columns:
            if col in survey_df.columns:
                survey_well_col = col
                break
        
        if survey_well_col is None:
            print("❌ Could not find well name column in survey file")
            return
        
        # Get unique well names from survey
        survey_wells = set(survey_df[survey_well_col].dropna().astype(str).str.strip())
        print(f"   Found {len(survey_wells)} unique wells in survey")
        
        # -----------------------------------------------------------------
        # STEP 2: Get all wells from PCE_WM
        # -----------------------------------------------------------------
        print(f"\n🏭 Fetching wells from database...")
        conn = get_sql_conn()
        
        query = """
        SELECT 
            [Well Name],
            [Composite Name],
            [Base Composite Name]
        FROM PCE_WM
        ORDER BY [Well Name]
        """
        
        pce_df = pd.read_sql(query, conn)
        conn.close()
        print(f"   Found {len(pce_df)} wells in PCE_WM")
        
        # -----------------------------------------------------------------
        # STEP 3: Find wells NOT in survey
        # -----------------------------------------------------------------
        print(f"\n🔍 Comparing...")
        
        missing_wells = []
        matched_wells = []
        
        for _, row in pce_df.iterrows():
            well_name = str(row['Well Name']).strip() if pd.notna(row['Well Name']) else ""
            composite = str(row['Composite Name']).strip() if pd.notna(row['Composite Name']) else ""
            base = str(row['Base Composite Name']).strip() if pd.notna(row['Base Composite Name']) else ""
            
            # Check if any version of this well name appears in survey
            in_survey = False
            matched_on = ""
            
            if well_name and well_name in survey_wells:
                in_survey = True
                matched_on = f"Well Name: {well_name}"
            elif composite and composite in survey_wells:
                in_survey = True
                matched_on = f"Composite Name: {composite}"
            elif base and base in survey_wells:
                in_survey = True
                matched_on = f"Base Composite: {base}"
            
            if in_survey:
                matched_wells.append({
                    'Well Name': well_name,
                    'Matched On': matched_on
                })
            else:
                missing_wells.append({
                    'Well Name': well_name,
                    'Composite Name': composite,
                    'Base Composite': base
                })
        
        # -----------------------------------------------------------------
        # STEP 4: Save results to text file
        # -----------------------------------------------------------------
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"wells_not_in_survey_{timestamp}.txt"
        
        with open(output_file, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("WELLS NOT IN DIRECTIONAL SURVEY\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n\n")
            
            f.write(f"SUMMARY:\n")
            f.write(f"   Total wells in PCE_WM: {len(pce_df)}\n")
            f.write(f"   Wells found in survey: {len(matched_wells)}\n")
            f.write(f"   Wells NOT in survey: {len(missing_wells)}\n")
            f.write(f"   Coverage: {len(matched_wells)/len(pce_df)*100:.1f}%\n\n")
            
            f.write("=" * 80 + "\n")
            f.write("LIST OF WELLS NOT IN SURVEY\n")
            f.write("=" * 80 + "\n\n")
            
            if missing_wells:
                for i, well in enumerate(missing_wells, 1):
                    f.write(f"{i:3}. {well['Well Name']}\n")
                    if well['Composite Name']:
                        f.write(f"      Composite: {well['Composite Name']}\n")
                    if well['Base Composite']:
                        f.write(f"      Base: {well['Base Composite']}\n")
                    f.write("\n")
            else:
                f.write("   ✅ All wells are in the survey!\n")
            
            f.write("\n" + "=" * 80 + "\n")
            f.write("END OF REPORT\n")
            f.write("=" * 80 + "\n")
        
        # -----------------------------------------------------------------
        # STEP 5: Print summary to console
        # -----------------------------------------------------------------
        print("\n" + "=" * 60)
        print("RESULTS")
        print("=" * 60)
        print(f"   Total wells in PCE_WM: {len(pce_df)}")
        print(f"   Wells in survey: {len(matched_wells)}")
        print(f"   Wells NOT in survey: {len(missing_wells)}")
        print(f"   Coverage: {len(matched_wells)/len(pce_df)*100:.1f}%")
        print(f"\n📄 Results saved to: {output_file}")
        
        if missing_wells:
            print(f"\n⚠️ First 10 missing wells:")
            for i, well in enumerate(missing_wells[:10], 1):
                print(f"   {i}. {well['Well Name']}")
        
        print("=" * 60)
        
        return output_file
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        return None

def main():
    survey_path = input("Enter path to survey Excel file: ").strip()
    
    if not os.path.exists(survey_path):
        print(f"File not found: {survey_path}")
        return
    
    find_missing_wells(survey_path)
    
    input("\nPress Enter to exit...")

if __name__ == "__main__":
    from datetime import datetime
    main()
   
    "I:\ResEng\Tools\Programmers Paradise\mvp_cda_load\Survey.xlsx"