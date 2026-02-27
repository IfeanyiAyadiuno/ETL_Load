import pyodbc
import time
from datetime import datetime
import traceback

def reset_cda_calculated_fields():
    """
    Reset the four calculated fields in CDA_Table to NULL/0 in batches:
    - Gas - S2 Production
    - Gas - Sales Production
    - Condensate - Sales Production
    - Sales CGR Ratio
    """
    
    print("\n" + "="*80)
    print("CDA_TABLE RESET - CLEAR CALCULATED FIELDS (BATCHED)")
    print("="*80)
    
    # Database path
    db_path = r"I:\ResEng\Tools\Programmers Paradise\GUI_WM\PCE_WM1.accdb"
    
    print(f"\nDatabase: {db_path}")
    print("\n⚠️  WARNING: This will clear ALL values in the following columns:")
    print("   - Gas - S2 Production")
    print("   - Gas - Sales Production")
    print("   - Condensate - Sales Production")
    print("   - Sales CGR Ratio")
    print("\n   This action CANNOT be undone!")
    print("\nIMPORTANT: Close Microsoft Access before continuing!")
    
    confirm = input("\nType 'RESET' to confirm: ")
    if confirm != 'RESET':
        print("Reset cancelled.")
        return
    
    total_start = time.time()
    
    try:
        # Connect to database
        print("\nConnecting to database...")
        conn_str = (
            r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
            r'DBQ=' + db_path + ';'
        )
        conn = pyodbc.connect(conn_str, autocommit=False)
        cursor = conn.cursor()
        print("   Database connected successfully.")
        
        # -----------------------------------------------------------------
        # STEP 1: Check if columns exist
        # -----------------------------------------------------------------
        print("\n" + "-"*60)
        print("STEP 1: Verifying columns exist")
        print("-"*60)
        
        # Get column names from CDA_Table
        cursor.execute("SELECT TOP 1 * FROM CDA_Table")
        columns = [column[0] for column in cursor.description]
        
        target_cols = [
            'Gas - S2 Production',
            'Gas - Sales Production', 
            'Condensate - Sales Production',
            'Sales CGR Ratio'
        ]
        
        missing_cols = []
        for col in target_cols:
            if col in columns:
                print(f"   ✅ '{col}' - FOUND")
            else:
                print(f"   ❌ '{col}' - MISSING")
                missing_cols.append(col)
        
        if missing_cols:
            print(f"\n   ❌ ERROR: Missing columns: {missing_cols}")
            print("   Cannot proceed without these columns.")
            return
        
        # -----------------------------------------------------------------
        # STEP 2: Get record count and set up batching
        # -----------------------------------------------------------------
        print("\n" + "-"*60)
        print("STEP 2: Setting up batch processing")
        print("-"*60)
        
        cursor.execute("SELECT COUNT(*) FROM CDA_Table")
        total_records = cursor.fetchone()[0]
        print(f"   Total records in CDA_Table: {total_records:,}")
        
        # Set batch size to stay under lock limit
        batch_size = 5000  # Well under the 9500 default limit
        
        # Calculate number of batches
        num_batches = (total_records + batch_size - 1) // batch_size
        print(f"   Batch size: {batch_size:,} records")
        print(f"   Number of batches: {num_batches:,}")
        
        # -----------------------------------------------------------------
        # STEP 3: Reset columns in batches
        # -----------------------------------------------------------------
        print("\n" + "-"*60)
        print("STEP 3: Resetting columns to NULL in batches")
        print("-"*60)
        
        total_updated = 0
        
        for batch_num in range(num_batches):
            offset = batch_num * batch_size
            print(f"\n   Processing batch {batch_num + 1}/{num_batches} (records {offset + 1:,} to {min(offset + batch_size, total_records):,})...")
            
            # For each column, update this batch
            for col in target_cols:
                try:
                    cursor.execute(f"""
                        UPDATE CDA_Table 
                        SET [{col}] = NULL
                        WHERE [{col}] IS NOT NULL 
                        AND [ProdDate] IN (
                            SELECT TOP {batch_size} [ProdDate] 
                            FROM CDA_Table 
                            WHERE [{col}] IS NOT NULL
                            ORDER BY [ProdDate]
                        )
                    """)
                    rows_affected = cursor.rowcount
                    
                except Exception as e:
                    # If the above fails, try a simpler approach using a temp table
                    print(f"      Batch update failed, trying alternative method...")
                    conn.rollback()
                    
                    # Alternative: Get primary keys for this batch
                    cursor.execute(f"""
                        SELECT TOP {batch_size} [ProdDate], [Well Name]
                        FROM CDA_Table 
                        WHERE [{col}] IS NOT NULL
                        ORDER BY [ProdDate]
                    """)
                    
                    rows_to_update = cursor.fetchall()
                    
                    for row in rows_to_update:
                        prod_date, well_name = row
                        cursor.execute(f"""
                            UPDATE CDA_Table 
                            SET [{col}] = NULL
                            WHERE [ProdDate] = ? AND [Well Name] = ?
                        """, prod_date, well_name)
                    
                    rows_affected = len(rows_to_update)
                
                total_updated += rows_affected
            
            # Commit after each batch
            conn.commit()
            print(f"      Batch {batch_num + 1} complete. Total records processed so far: {min((batch_num + 1) * batch_size, total_records):,}")
        
        # -----------------------------------------------------------------
        # STEP 4: Verify reset
        # -----------------------------------------------------------------
        print("\n" + "-"*60)
        print("STEP 4: Verifying reset")
        print("-"*60)
        
        for col in target_cols:
            cursor.execute(f"SELECT COUNT(*) FROM CDA_Table WHERE [{col}] IS NOT NULL")
            remaining = cursor.fetchone()[0]
            if remaining == 0:
                print(f"   ✅ '{col}' - successfully cleared")
            else:
                print(f"   ⚠️ '{col}' - {remaining:,} records still have values")
        
        # -----------------------------------------------------------------
        # SUMMARY
        # -----------------------------------------------------------------
        total_time = time.time() - total_start
        
        print("\n" + "="*80)
        print("RESET SUMMARY")
        print("="*80)
        print(f"   Total records in CDA_Table: {total_records:,}")
        print(f"   Batch size used: {batch_size:,}")
        print(f"   Number of batches: {num_batches:,}")
        print(f"   Total time: {total_time:.1f} seconds")
        print("\n" + "="*80)
        print("RESET COMPLETE!")
        print("="*80)
        
        conn.close()
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        traceback.print_exc()
        try:
            conn.rollback()
        except:
            pass
        return False
    
    return True

if __name__ == "__main__":
    reset_cda_calculated_fields()
    print("\nPress Enter to exit...")
    input()