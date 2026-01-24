
# access_io.py
from pathlib import Path
import pyodbc
import pandas as pd

DB_PATH = Path(r"I:\ResEng\Tools\Programmers Paradise\GUI_WM\PCE_WM1.accdb")

WM_TABLE = "PCE_WM"
CDA_TABLE = "CDA_Table"


def connect_access():
    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        rf"DBQ={DB_PATH};"
        r"UID=Admin;PWD=;"
    )
    return pyodbc.connect(conn_str)


def get_pressures_id_for_gas(cur, gas_id: str) -> str | None:
    sql = f"""
        SELECT TOP 1 [PressuresIDREC]
        FROM [{WM_TABLE}]
        WHERE [GasIDREC] = ?
    """
    cur.execute(sql, (gas_id,))
    row = cur.fetchone()
    return str(row[0]).strip() if row and row[0] is not None else None


def get_well_name_for_gas(cur, gas_id: str) -> str | None:
    sql = """
        SELECT TOP 1 [Well Name]
        FROM [PCE_WM]
        WHERE [GasIDREC] = ?
    """
    cur.execute(sql, (gas_id,))
    row = cur.fetchone()
    return row[0] if row and row[0] is not None else None



def delete_cda_window(cur, gas_id: str, start_date: str, end_date_excl: str) -> int:
    """
    Deletes CDA rows for a GasIDREC in window: [start_date, end_date_excl)
    Uses ProdDateTime field.
    """
    sql = f"""
        DELETE FROM [{CDA_TABLE}]
        WHERE [GasIDREC] = ?
          AND [ProdDateTime] >= ?
          AND [ProdDateTime] <  ?;
    """
    cur.execute(sql, (gas_id, start_date, end_date_excl))
    return max(cur.rowcount, 0)


def insert_cda_rows(cur, df: pd.DataFrame) -> int:
    """
    Inserts rows into CDA_Table. Expects df to already have columns that exist in Access.
    """
    if df is None or df.empty:
        return 0

    # drop autonumber if present
    df = df.drop(columns=["ID"], errors="ignore")

    # --- NEW: normalize/clean column names ---
    df = df.copy()
    df.columns = [str(c).strip().replace("\ufeff", "").replace("\n", " ").replace("\r", " ") for c in df.columns]

    # --- NEW: only insert columns that actually exist in Access table ---
    cur.execute(f"SELECT TOP 1 * FROM [{CDA_TABLE}]")
    access_cols = [d[0] for d in cur.description]  # field names in CDA_Table

    keep_cols = [c for c in df.columns if c in access_cols]
    drop_cols = [c for c in df.columns if c not in access_cols]

    if not keep_cols:
        raise RuntimeError(f"No matching columns to insert. DF cols={list(df.columns)} Access cols={access_cols}")

    if drop_cols:
        print("[insert_cda_rows] Dropping non-Access columns:", drop_cols)

    df = df[keep_cols]

    # build insert SQL
    col_sql = ", ".join(f"[{c}]" for c in keep_cols)
    placeholders = ", ".join("?" for _ in keep_cols)
    sql = f"INSERT INTO [{CDA_TABLE}] ({col_sql}) VALUES ({placeholders})"

    # convert NaN -> None for ODBC
    data = df.where(pd.notna(df), None).values.tolist()

    try:
        cur.executemany(sql, data)
    except Exception as e:
        print("\n[insert_cda_rows] FAILED SQL:\n", sql)
        print("[insert_cda_rows] Columns:", keep_cols)
        raise

    return len(data)




def update_cda_pressure_params(cur, df_params: pd.DataFrame) -> int:
    """
    Updates CDA_Table TubingPressure/CasingPressure/ChokeSize.
    Matches on:
      PressuresIDREC + DateValue(ProdDateTime)
    """
    if df_params is None or df_params.empty:
        return 0

    needed = {"PressuresIDREC", "ProdDateTime", "TubingPressure", "CasingPressure", "ChokeSize"}
    missing = needed - set(df_params.columns)
    if missing:
        raise KeyError(f"df_params missing columns: {missing}")

    df = df_params.copy()
    df["PressuresIDREC"] = df["PressuresIDREC"].astype(str).str.strip()
    df["ProdDateTime"] = pd.to_datetime(df["ProdDateTime"], errors="coerce")
    df = df.dropna(subset=["ProdDateTime"])
    # normalize to midnight for Access date param consistency
    df["ProdDateMidnight"] = df["ProdDateTime"].dt.normalize()

    sql = f"""
        UPDATE [{CDA_TABLE}]
        SET
            [TubingPressure] = ?,
            [CasingPressure] = ?,
            [ChokeSize] = ?
        WHERE
            [PressuresIDREC] = ?
            AND DateValue([ProdDateTime]) = DateValue(?);
    """

    updated = 0
    for _, r in df.iterrows():
        tub = None if pd.isna(r["TubingPressure"]) else float(r["TubingPressure"])
        cas = None if pd.isna(r["CasingPressure"]) else float(r["CasingPressure"])
        chk = None if pd.isna(r["ChokeSize"]) else float(r["ChokeSize"])
        pid = r["PressuresIDREC"]
        dtm = r["ProdDateMidnight"].to_pydatetime()

        cur.execute(sql, (tub, cas, chk, pid, dtm))
        updated += max(cur.rowcount, 0)

    return updated


def debug_cda_window(cur, gas_id: str, start_date: str, end_date_excl: str):
    sql = f"""
        SELECT [GasIDREC], [PressuresIDREC], [ProdDateTime],
               [GasWH_Production], [OnProdHours],
               [TubingPressure], [CasingPressure], [ChokeSize]
        FROM [{CDA_TABLE}]
        WHERE [GasIDREC] = ?
          AND [ProdDateTime] >= ?
          AND [ProdDateTime] <  ?
        ORDER BY [ProdDateTime];
    """
    cur.execute(sql, (gas_id, start_date, end_date_excl))
    rows = cur.fetchall()
    print(f"\n[ACCESS DEBUG] CDA rows in window: {len(rows)}")
    for r in rows[:10]:
        print(r)

def update_cda_wgr(cur, df_wgr: pd.DataFrame) -> int:
    """
    Update CDA_Table.WGR_Ratio by matching:
      PressuresIDREC + DateValue(ProdDateTime)
    """
    if df_wgr is None or df_wgr.empty:
        return 0

    needed = {"PressuresIDREC", "ProdDateTime", "WGR_Ratio"}
    missing = needed - set(df_wgr.columns)
    if missing:
        raise KeyError(f"df_wgr missing columns: {missing}")

    df = df_wgr.copy()
    df["PressuresIDREC"] = df["PressuresIDREC"].astype(str).str.strip()
    df["ProdDateTime"] = pd.to_datetime(df["ProdDateTime"], errors="coerce")
    df = df.dropna(subset=["ProdDateTime"])
    df["ProdDateMidnight"] = df["ProdDateTime"].dt.normalize()

    sql = f"""
        UPDATE [{CDA_TABLE}]
        SET [WGR_Ratio] = ?
        WHERE [PressuresIDREC] = ?
          AND DateValue([ProdDateTime]) = DateValue(?);
    """

    updated = 0
    for _, r in df.iterrows():
        wgr = None if pd.isna(r["WGR_Ratio"]) else float(r["WGR_Ratio"])
        pid = r["PressuresIDREC"]
        dtm = r["ProdDateMidnight"].to_pydatetime()
        cur.execute(sql, (wgr, pid, dtm))
        updated += max(cur.rowcount, 0)

    return updated

def update_cda_ecf(cur, df_ecf: pd.DataFrame) -> int:
    """
    Update CDA_Table.ECF_Ratio by matching on:
      GasIDREC + DateValue(ProdDateTime)

    Handles Snowflake/pandas uppercase columns (GASIDREC/PRODDATETIME/ECF_RATIO).
    """
    if df_ecf is None or df_ecf.empty:
        return 0

    df = df_ecf.copy()

    # ✅ Normalize column casing (Snowflake often returns UPPERCASE)
    df.columns = [c.upper() for c in df.columns]

    needed = {"GASIDREC", "PRODDATETIME", "ECF_RATIO"}
    missing = needed - set(df.columns)
    if missing:
        raise KeyError(f"df_ecf missing columns: {missing}. Got: {list(df.columns)}")

    df["GASIDREC"] = df["GASIDREC"].astype(str)
    df["PRODDATETIME"] = pd.to_datetime(df["PRODDATETIME"], errors="coerce")
    df = df.dropna(subset=["PRODDATETIME", "ECF_RATIO"])

    # date-only key to match Access rows regardless of time
    df["PRODDATE"] = df["PRODDATETIME"].dt.date

    sql = f"""
        UPDATE [{CDA_TABLE}]
        SET [ECF_Ratio] = ?
        WHERE [GasIDREC] = ?
          AND DateValue([ProdDateTime]) = ?;
    """

    updated = 0
    for _, r in df.iterrows():
        cur.execute(sql, (float(r["ECF_RATIO"]), r["GASIDREC"], r["PRODDATE"]))
        updated += max(cur.rowcount, 0)

    return updated

def update_cda_cgr(cur, df_cgr: pd.DataFrame) -> int:
    """
    Update CDA_Table.CGR_Ratio by matching:
      PressuresIDREC AND DateValue(ProdDateTime)
    """
    if df_cgr is None or df_cgr.empty:
        return 0

    needed = {"PressuresIDREC", "ProdDateTime", "CGR_Ratio"}
    missing = needed - set(df_cgr.columns)
    if missing:
        raise KeyError(f"df_cgr missing columns: {missing}")

    df = df_cgr.copy()
    df["PressuresIDREC"] = df["PressuresIDREC"].astype(str)

    df["ProdDateTime"] = pd.to_datetime(df["ProdDateTime"], errors="coerce")
    df = df.dropna(subset=["ProdDateTime"])
    df["ProdDate"] = df["ProdDateTime"].dt.date

    df["CGR_Ratio"] = pd.to_numeric(df["CGR_Ratio"], errors="coerce")
    df = df.dropna(subset=["CGR_Ratio"])

    sql = f"""
        UPDATE [{CDA_TABLE}]
        SET CGR_Ratio = ?
        WHERE [PressuresIDREC] = ?
          AND DateValue([ProdDateTime]) = ?;
    """

    updated = 0
    for _, r in df.iterrows():
        cur.execute(sql, (float(r["CGR_Ratio"]), r["PressuresIDREC"], r["ProdDate"]))
        updated += max(cur.rowcount, 0)

    return updated

def update_cda_condensate_wh(cur) -> int:
    """
    Condensate_WH_Production = GasWH_Production * CGR_Ratio
    Only updates rows where both values exist.
    """
    sql = """
        UPDATE [CDA_Table]
        SET [Condensate_WH_Production] = 
            IIF(
                [GasWH_Production] IS NULL OR [CGR_Ratio] IS NULL,
                NULL,
                [GasWH_Production] * [CGR_Ratio]
            )
        WHERE
            [GasWH_Production] IS NOT NULL
            AND [CGR_Ratio] IS NOT NULL
    """
    cur.execute(sql)
    return cur.rowcount

def update_cda_alloc_monthday(cur, df_alloc) -> int:
    """
    Updates: Gath_Gas_Production, New_Prod_Cond_Sales_Production, NGL
    Match on PressuresIDREC + DateValue(ProdDateTime).
    """
    if df_alloc is None or df_alloc.empty:
        return 0

    df = df_alloc.copy()
    df["ProdDateTime"] = pd.to_datetime(df["ProdDateTime"], errors="coerce")
    df = df.dropna(subset=["ProdDateTime"])
    df["ProdDate"] = df["ProdDateTime"].dt.date

    sql = """
        UPDATE [CDA_Table]
        SET
            [Gath_Gas_Production] = ?,
            [New_Prod_Cond_Sales_Production] = ?,
            [NGL] = ?
        WHERE
            [PressuresIDREC] = ?
            AND DateValue([ProdDateTime]) = ?;
    """

    updated = 0
    for _, r in df.iterrows():
        gg  = None if pd.isna(r.get("Gath_Gas_Production")) else float(r["Gath_Gas_Production"])
        npc = None if pd.isna(r.get("New_Prod_Cond_Sales_Production")) else float(r["New_Prod_Cond_Sales_Production"])
        ngl = None if pd.isna(r.get("NGL")) else float(r["NGL"])
        pid = str(r["PressuresIDREC"])
        pdt = r["ProdDate"]

        cur.execute(sql, (gg, npc, ngl, pid, pdt))
        updated += max(cur.rowcount, 0)

    return updated


def update_cda_alloc_water(cur, df_water: pd.DataFrame) -> int:
    """
    Updates CDA_Table.Allocated_Water_Rate_Production using:
      PressuresIDREC + DateValue(ProdDateTime)
    """
    if df_water is None or df_water.empty:
        return 0

    needed = {"PressuresIDREC", "ProdDateTime", "Allocated_Water_Rate_Production"}
    missing = needed - set(df_water.columns)
    if missing:
        raise KeyError(f"df_water missing columns: {missing}")

    df = df_water.copy()
    df["PressuresIDREC"] = df["PressuresIDREC"].astype(str)
    df["ProdDateTime"] = pd.to_datetime(df["ProdDateTime"], errors="coerce")
    df = df.dropna(subset=["ProdDateTime"])
    df["ProdDate"] = df["ProdDateTime"].dt.date

    sql = f"""
        UPDATE [{CDA_TABLE}]
        SET [Allocated_Water_Rate_Production] = ?
        WHERE [PressuresIDREC] = ?
          AND DateValue([ProdDateTime]) = ?;
    """

    updated = 0
    for _, r in df.iterrows():
        val = None if pd.isna(r["Allocated_Water_Rate_Production"]) else float(r["Allocated_Water_Rate_Production"])
        cur.execute(sql, (val, r["PressuresIDREC"], r["ProdDate"]))
        updated += max(cur.rowcount, 0)

    return updated
