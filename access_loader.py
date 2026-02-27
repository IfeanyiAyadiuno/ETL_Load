
# access_loader.py
import os
import math
from datetime import datetime, time
import pandas as pd
import pyodbc
from dotenv import load_dotenv

load_dotenv()

ACCESS_PATH = os.getenv("ACCESS_PATH")
ACCESS_DRIVER = os.getenv("ACCESS_DRIVER", "{Microsoft Access Driver (*.mdb, *.accdb)}")


def get_access_conn():
    if not ACCESS_PATH:
        raise RuntimeError("ACCESS_PATH missing in .env")
    conn_str = f"DRIVER={ACCESS_DRIVER};DBQ={ACCESS_PATH};"
    return pyodbc.connect(conn_str)


def ensure_cda_table():
    """
    Create CDA_Table if it doesn't exist.
    """
    ddl = """
    CREATE TABLE CDA_Table (
        ProdDate DATETIME,
        GasIDREC TEXT(64),
        PressuresIDREC TEXT(64),
        [Well Name] TEXT(255),

        ECF_Ratio DOUBLE,
        GasWH_Production DOUBLE,
        OnProdHours DOUBLE,

        CGR_Ratio DOUBLE,
        WGR_Ratio DOUBLE,

        TubingPressure DOUBLE,
        CasingPressure DOUBLE,
        ChokeSize DOUBLE,

        Gathered_Gas_Production DOUBLE,
        Gathered_Condensate_Production DOUBLE,
        NGL_Production DOUBLE,

        AllocatedWater_Rate DOUBLE,
        Condensate_WH_Production DOUBLE
    )
    """

    with get_access_conn() as cn:
        cur = cn.cursor()
        try:
            cur.execute("SELECT TOP 1 * FROM CDA_Table")
        except Exception:
            cur.execute(ddl)
            # optional index (helps delete/query)
            try:
                cur.execute("CREATE INDEX idx_cda_key ON CDA_Table (ProdDate, GasIDREC)")
            except Exception:
                pass
        cn.commit()


def clear_entire_cda_table():
    """
    Fastest way to clear the entire CDA_Table.
    Uses TRUNCATE-like approach (drop and recreate).
    """
    print("  Using fast clear method (drop and recreate)...")
    
    with get_access_conn() as cn:
        cur = cn.cursor()
        
        # Count rows first
        try:
            cur.execute("SELECT COUNT(*) FROM CDA_Table")
            total_rows = cur.fetchone()[0]
            print(f"  Found {total_rows:,} rows to delete")
        except:
            total_rows = 0
        
        if total_rows == 0:
            print("  Table already empty")
            return
        
        try:
            # Drop the table
            cur.execute("DROP TABLE CDA_Table")
            cn.commit()
            print(f"  ✓ Dropped table with {total_rows:,} rows")
        except Exception as e:
            print(f"  Warning: Could not drop table: {e}")
            # Fall back to DELETE *
            cur.execute("DELETE * FROM CDA_Table")
            cn.commit()
            print(f"  ✓ Deleted all {total_rows:,} rows using DELETE *")
    
    # Recreate the table structure
    ensure_cda_table()
    print("  ✓ Table recreated and ready for new data")


def delete_cda_range(start: str, end: str, fast_mode=True):
    """
    Delete rows in [start, end] inclusive.
    
    Parameters:
    - start: Start date string
    - end: End date string  
    - fast_mode: If True and deleting all data, use drop/recreate (MUCH faster)
    """
    start_dt = datetime.combine(pd.to_datetime(start).date(), time.min)
    end_dt_exclusive = datetime.combine(pd.to_datetime(end).date(), time.min) + pd.Timedelta(days=1)

    # Open connection to check what needs to be deleted
    cn = get_access_conn()
    cur = cn.cursor()
    
    # First, count total rows in table and rows to delete
    cur.execute("SELECT COUNT(*) FROM CDA_Table")
    total_in_table = cur.fetchone()[0]
    
    cur.execute(
        "SELECT COUNT(*) FROM CDA_Table WHERE ProdDate >= ? AND ProdDate < ?",
        start_dt, end_dt_exclusive
    )
    rows_to_delete = cur.fetchone()[0]
    
    print(f"  Table has {total_in_table:,} total rows")
    print(f"  Deleting {rows_to_delete:,} rows in date range")
    
    if rows_to_delete == 0:
        print("  No rows to delete")
        cn.close()
        return
    
    # FAST MODE: If deleting everything (or almost everything), drop and recreate
    if fast_mode and rows_to_delete >= total_in_table * 0.95:
        print("  Detected: Deleting 95%+ of table - using fast clear method")
        cn.close()  # Close this connection before drop/recreate
        clear_entire_cda_table()
        return  # Exit here - table is already cleared
    
    # Otherwise, use batched deletion
    print("  Using batched deletion...")
    batch_size = 50000  # Increased from 10,000 - adjust if you still get errors
    deleted_total = 0
    
    try:
        while True:
            # Delete a batch
            cur.execute(f"""
                DELETE FROM CDA_Table 
                WHERE ProdDate >= ? AND ProdDate < ?
                AND ProdDate IN (
                    SELECT TOP {batch_size} ProdDate 
                    FROM CDA_Table 
                    WHERE ProdDate >= ? AND ProdDate < ?
                )
            """, start_dt, end_dt_exclusive, start_dt, end_dt_exclusive)
            
            rows_deleted = cur.rowcount
            cn.commit()
            
            if rows_deleted == 0:
                break
            
            deleted_total += rows_deleted
            pct = (deleted_total / rows_to_delete * 100) if rows_to_delete > 0 else 0
            print(f"  Deleted {deleted_total:,} / {rows_to_delete:,} rows ({pct:.1f}%)...")
        
        print(f"  ✓ Successfully deleted {deleted_total:,} rows")
    finally:
        cn.close()


def _to_py(v):
    """
    Convert pandas NaN to None for Access inserts.
    """
    if v is None:
        return None
    try:
        if isinstance(v, float) and math.isnan(v):
            return None
    except Exception:
        pass
    return v


def insert_cda_rows(df: pd.DataFrame):
    """
    Insert dataframe into CDA_Table.
    - Inserts only columns that exist in df
    - Converts ProdDate to Python datetime
    - Uses safe executemany (no fast_executemany for Access) + chunking
    - Skips empty dataframes
    """
    if df.empty:
        print("  No rows to insert - dataframe is empty")
        return

    out = df.copy()

    # Convert to Python datetime (NOT pandas Timestamp)
    out["ProdDate"] = pd.to_datetime(out["ProdDate"]).dt.floor("D")
    out["ProdDate"] = out["ProdDate"].apply(lambda x: x.to_pydatetime() if pd.notna(x) else None)

    desired_cols = [
        "ProdDate", "GasIDREC", "PressuresIDREC", "Well Name",
        "ECF_Ratio", "GasWH_Production", "OnProdHours",
        "CGR_Ratio", "WGR_Ratio",
        "TubingPressure", "CasingPressure", "ChokeSize",
        "Gathered_Gas_Production", "Gathered_Condensate_Production", "NGL_Production",
        "AllocatedWater_Rate", "Condensate_WH_Production",
    ]

    cols = [c for c in desired_cols if c in out.columns]
    missing = [c for c in desired_cols if c not in out.columns]

    print("\nInsert columns:", cols)
    if missing:
        print("Skipping missing columns (not in df yet):", missing)

    # Build INSERT statement dynamically
    col_sql = []
    for c in cols:
        if c == "Well Name":
            col_sql.append("[Well Name]")
        else:
            col_sql.append(c)

    insert_sql = f"""
    INSERT INTO CDA_Table ({", ".join(col_sql)})
    VALUES ({",".join(["?"] * len(cols))})
    """

    # Build rows + ensure Access-friendly types
    def normalize_value(v):
        v = _to_py(v)  # NaN->None
        # pandas Timestamp -> python datetime
        if isinstance(v, pd.Timestamp):
            return v.to_pydatetime()
        # Handle numpy datetime64
        if hasattr(v, 'dtype') and 'datetime64' in str(v.dtype):
            return pd.Timestamp(v).to_pydatetime()
        return v

    rows = [tuple(normalize_value(v) for v in r)
            for r in out[cols].itertuples(index=False, name=None)]

    print(f"Rows to insert: {len(rows):,}")

    if len(rows) == 0:
        print("  No rows to insert - skipping")
        return

    # Chunked insert (safe)
    chunk_size = 400  # small, safe for Access
    with get_access_conn() as cn:
        cur = cn.cursor()
        # IMPORTANT: Access ODBC can hang with fast_executemany
        cur.fast_executemany = False

        try:
            total_inserted = 0
            for i in range(0, len(rows), chunk_size):
                batch = rows[i:i + chunk_size]
                cur.executemany(insert_sql, batch)
                cn.commit()
                total_inserted += len(batch)
                print(f"  Inserted {total_inserted:,} / {len(rows):,} rows")
            print(f"✅ Access insert complete: {total_inserted:,} rows inserted")
        except Exception as e:
            print("❌ Access insert failed:", type(e).__name__, e)
            raise