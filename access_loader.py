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

        GatheredGas_Production DOUBLE,
        NewProdCondSales_Production DOUBLE,
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


def delete_cda_range(start: str, end: str):
    """
    Delete rows in [start, end] inclusive.
    """
    start_dt = datetime.combine(pd.to_datetime(start).date(), time.min)
    end_dt_exclusive = datetime.combine(pd.to_datetime(end).date(), time.min) + pd.Timedelta(days=1)

    with get_access_conn() as cn:
        cur = cn.cursor()
        cur.execute(
            "DELETE FROM CDA_Table WHERE ProdDate >= ? AND ProdDate < ?",
            start_dt, end_dt_exclusive
        )
        cn.commit()


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
    """
    out = df.copy()

    # Convert to Python datetime (NOT pandas Timestamp)
    out["ProdDate"] = pd.to_datetime(out["ProdDate"]).dt.floor("D")
    out["ProdDate"] = out["ProdDate"].apply(lambda x: x.to_pydatetime() if pd.notna(x) else None)

    desired_cols = [
        "ProdDate", "GasIDREC", "PressuresIDREC", "Well Name",
        "ECF_Ratio", "GasWH_Production", "OnProdHours",
        "CGR_Ratio", "WGR_Ratio",
        "TubingPressure", "CasingPressure", "ChokeSize",
        "GatheredGas_Production", "NewProdCondSales_Production", "NGL_Production",
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
        col_sql.append("[Well Name]" if c == "Well Name" else c)

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
        return v

    rows = [tuple(normalize_value(v) for v in r)
            for r in out[cols].itertuples(index=False, name=None)]

    print("Rows to insert:", len(rows))

    # Chunked insert (safe)
    chunk_size = 200  # small, safe for Access
    with get_access_conn() as cn:
        cur = cn.cursor()
        # IMPORTANT: Access ODBC can hang with fast_executemany
        cur.fast_executemany = False

        try:
            for i in range(0, len(rows), chunk_size):
                batch = rows[i:i + chunk_size]
                cur.executemany(insert_sql, batch)
                cn.commit()
                print(f"Inserted {i + len(batch)} / {len(rows)}")
            print("✅ Access insert commit complete")
        except Exception as e:
            print("❌ Access insert failed:", type(e).__name__, e)
            raise

