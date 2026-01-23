
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
    Inserts rows into CDA_Table.
    Only inserts columns that actually exist in Access table.
    """
    if df is None or df.empty:
        return 0

    # Drop Access autonumber if present
    df = df.drop(columns=["ID"], errors="ignore")

    # Read Access CDA columns so we don't insert unknown fields
    cur.execute(f"SELECT TOP 1 * FROM [{CDA_TABLE}];")
    access_cols = [d[0] for d in cur.description]  # column names in Access

    # Keep only columns that exist in Access
    keep_cols = [c for c in df.columns if c in access_cols]
    df2 = df[keep_cols].copy()

    if df2.empty:
        raise RuntimeError(
            "After filtering to Access columns, nothing left to insert. "
            f"DF columns were: {list(df.columns)} | Access columns are: {access_cols}"
        )

    col_sql = ", ".join(f"[{c}]" for c in df2.columns)
    placeholders = ", ".join("?" for _ in df2.columns)
    sql = f"INSERT INTO [{CDA_TABLE}] ({col_sql}) VALUES ({placeholders});"

    data = df2.where(pd.notna(df2), None).values.tolist()
    cur.executemany(sql, data)
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

