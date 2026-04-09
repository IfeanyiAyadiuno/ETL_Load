"""
Shared allocation SQL for Production Accounting (ValNav) vs Public Sales (Accumap + CGR).

PA writes ValNav-backed Allocation_Factors rows and applies S2 + condensate sales to
PCE_CDA / PCE_Production only. Public Sales merges Accumap sales gas into Allocation_Factors,
updates only gas sales and Sales CGR on PCE_CDA, then syncs all four sales columns on
PCE_Production from CDA.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from typing import Callable, Dict, List, Optional, Tuple, Union

import pandas as pd

import log_format as lf


MonthStart = Union[datetime, date]


def calendar_month_bounds(month_start: MonthStart) -> Tuple[date, date, int]:
    """First day, last day, and day count for the calendar month of month_start."""
    y, m = month_start.year, month_start.month
    if m == 12:
        last = date(y, 12, 31)
    else:
        last = date(y, m + 1, 1) - timedelta(days=1)
    first = date(y, m, 1)
    days = (last - first).days + 1
    return first, last, days


def fetch_pce_uwi_to_well_name(cursor) -> Dict[str, str]:
    """Lowercased UWI variants -> PCE_WM [Well Name] (same rules as monthly_loader_gui)."""
    cursor.execute(
        "SELECT [Value Navigator UWI], [Well Name] "
        "FROM PCE_WM "
        "WHERE [Value Navigator UWI] IS NOT NULL "
        "AND ([Exception] IS NULL OR [Exception] = '' OR [Exception] = 'N')"
    )
    pce_uwi_dict: Dict[str, str] = {}
    for pce_uwi, well_name in cursor.fetchall():
        if not pce_uwi or not well_name:
            continue
        pce_uwi_str = str(pce_uwi).strip()
        variations = [pce_uwi_str.lower()]
        if len(pce_uwi_str) > 1 and pce_uwi_str[0].isdigit():
            variations.append(pce_uwi_str[1:].lower())
        if "/" in pce_uwi_str:
            parts = pce_uwi_str.split("/")
            if parts:
                last_part = parts[-1]
                if last_part.isdigit():
                    clean_last = str(int(last_part))
                    new_uwi = "/".join(parts[:-1] + [clean_last])
                    variations.append(new_uwi.lower())
                if last_part.isdigit() and len(last_part) == 1:
                    padded_last = last_part.zfill(2)
                    new_uwi = "/".join(parts[:-1] + [padded_last])
                    variations.append(new_uwi.lower())
        for variation in variations:
            pce_uwi_dict[variation] = well_name
    return pce_uwi_dict


def normalize_uwi_for_matching(uwi_str: str) -> str:
    normalized = uwi_str.lower()
    if normalized.endswith("/02"):
        normalized = normalized[:-3] + "/2"
    return normalized


def read_accumap_sales_by_uwi(accumap_path: str, month_start: datetime) -> Dict[str, float]:
    """
    Read Accumap 'Sales Gas - to PRW' sheet; return UWI string (as in file) -> PRD Monthly Mktbl GAS e3m3.
    """
    accumap_xl = pd.ExcelFile(accumap_path)
    sheets = accumap_xl.sheet_names
    target_sheet = "Sales Gas - to PRW"
    if target_sheet not in sheets:
        target_sheet = sheets[0]
    df_accumap = pd.read_excel(accumap_path, sheet_name=target_sheet)
    df_accumap["UWI_clean_accumap"] = df_accumap["Unique Well ID"].astype(str).str.strip()
    df_accumap["UWI_clean_accumap"] = df_accumap["UWI_clean_accumap"].apply(
        lambda x: x[:-1] if isinstance(x, str) and x.endswith("0") and len(x) > 1 else x
    )
    df_accumap["Date_parsed"] = pd.to_datetime(df_accumap["Date"], errors="coerce")
    df_f = df_accumap[
        (df_accumap["Date_parsed"].dt.year == month_start.year)
        & (df_accumap["Date_parsed"].dt.month == month_start.month)
    ].copy()
    df_f = df_f.dropna(subset=["UWI_clean_accumap"]).copy()
    df_f["UWI_clean_accumap"] = df_f["UWI_clean_accumap"].astype(str).str.strip()
    df_f["PRD Monthly Mktbl GAS e3m3"] = pd.to_numeric(
        df_f["PRD Monthly Mktbl GAS e3m3"], errors="coerce"
    ).fillna(0)
    df_f = df_f.drop_duplicates(subset=["UWI_clean_accumap"], keep="last")
    return {
        str(uwi).strip(): float(gas)
        for uwi, gas in zip(df_f["UWI_clean_accumap"], df_f["PRD Monthly Mktbl GAS e3m3"])
    }


def map_accumap_uwi_to_well_sales(
    accumap_by_uwi: Dict[str, float], pce_uwi_dict: Dict[str, str]
) -> Tuple[Dict[str, float], List[str], List[Tuple[str, str, float]]]:
    """
    Map Accumap UWIs to PCE_WM well names. Last UWI mapping wins per well for ``well_sales``.
    Returns (well_name -> sales_gas, unmatched_uwis, matched_rows) where ``matched_rows`` is
    (accumap_uwi, pce_well_name, sales_gas) for every Accumap row that matched (including
    multiple UWIs that map to the same well).
    """
    well_sales: Dict[str, float] = {}
    unmatched_uwis: List[str] = []
    matched_rows: List[Tuple[str, str, float]] = []
    for uwi_str, sales_gas in accumap_by_uwi.items():
        uwi_str = str(uwi_str)
        matched = False
        well_name = None
        nu = normalize_uwi_for_matching(uwi_str)
        if nu in pce_uwi_dict:
            well_name = pce_uwi_dict[nu]
            matched = True
        if not matched and len(uwi_str) > 1 and uwi_str[0].isdigit():
            try_uwi = uwi_str[1:].lower()
            if try_uwi in pce_uwi_dict:
                well_name = pce_uwi_dict[try_uwi]
                matched = True
        if matched and well_name:
            sg = float(sales_gas)
            well_sales[well_name] = sg
            matched_rows.append((uwi_str, well_name, sg))
        else:
            unmatched_uwis.append(uwi_str)
    return well_sales, unmatched_uwis, matched_rows


def merge_accumap_into_allocation_factors(
    conn,
    month_start: MonthStart,
    accumap_path: str,
    log: Optional[Callable[[str], None]] = None,
) -> Dict[str, Union[int, str, None]]:
    """
    Update Sales_Gas, WH_to_Sales_AllocFactor, Gathered_to_Sales for existing rows
    for MonthStartDate. Wells without Accumap data get Sales_Gas = 0 and factors cleared.
    """
    def _log(msg: str) -> None:
        if log:
            log(msg)

    if not accumap_path or not os.path.isfile(accumap_path):
        return {"error": f"Accumap file not found: {accumap_path}"}

    ms_dt = (
        month_start
        if isinstance(month_start, datetime)
        else datetime(month_start.year, month_start.month, 1)
    )

    cursor = conn.cursor()
    pce_uwi_dict = fetch_pce_uwi_to_well_name(cursor)
    accumap_by_uwi = read_accumap_sales_by_uwi(accumap_path, ms_dt)
    well_sales, unmatched_uwi_list, _matched_detail = map_accumap_uwi_to_well_sales(
        accumap_by_uwi, pce_uwi_dict
    )
    n_matched_uwi = len(accumap_by_uwi) - len(unmatched_uwi_list)
    _log(
        lf.detail(
            f"Accumap: {lf.num(len(accumap_by_uwi))} UWIs read, "
            f"{lf.num(n_matched_uwi)} matched to {lf.num(len(well_sales))} PCE_WM well(s), "
            f"{lf.num(len(unmatched_uwi_list))} unmatched UWIs"
        )
    )

    cursor.execute(
        """
        SELECT [Well Name], Prodview_WH_Gas, Gathered_Gas_Production
        FROM Allocation_Factors
        WHERE MonthStartDate = ?
        """,
        month_start,
    )
    rows = cursor.fetchall()
    if not rows:
        return {"error": f"No Allocation_Factors rows for {ms_dt.strftime('%b %Y')} — run PA first."}

    update_sql = """
        UPDATE Allocation_Factors SET
            Sales_Gas = ?,
            WH_to_Sales_AllocFactor = ?,
            Gathered_to_Sales = ?
        WHERE MonthStartDate = ? AND [Well Name] = ?
    """
    updated = 0
    for well_name, prodview_wh_gas, gathered_gas in rows:
        if not well_name:
            continue
        sales_gas = float(well_sales.get(well_name, 0.0))
        pw = float(prodview_wh_gas) if prodview_wh_gas is not None else 0.0
        gg = float(gathered_gas) if gathered_gas is not None else 0.0
        wh_to_sales = 1.0 if pw == 0 else sales_gas / pw
        gathered_to_sales = "1" if gg == 0 else str(sales_gas / gg)
        cursor.execute(
            update_sql,
            (sales_gas, wh_to_sales, gathered_to_sales, month_start, well_name),
        )
        updated += 1

    conn.commit()
    _log(lf.detail(f"Updated Accumap fields on {lf.num(updated)} Allocation_Factors rows"))
    return {"rows_updated": updated, "unmatched_accumap_uwis": len(unmatched_uwi_list)}


def apply_valnav_allocation_to_cda_and_production(
    conn,
    month_start: MonthStart,
    log: Optional[Callable[[str], None]] = None,
) -> None:
    """PCE_CDA + PCE_Production: S2 gas and condensate sales only (ValNav path)."""
    def _log(msg: str) -> None:
        if log:
            log(msg)

    month_start_date, month_end_date, _days = calendar_month_bounds(month_start)
    cursor = conn.cursor()

    _log(lf.detail("Applying ValNav allocation to PCE_CDA (S2 gas, condensate sales)…"))
    cursor.execute(
        """
        UPDATE c SET
            c.[Gas - S2 Production] = ISNULL(a.WH_to_S2_AllocFactor, 1.0)
                                      * c.[GasWH_Production],
            c.[Condensate - Sales Production] = ISNULL(a.WH_to_Sales_Cond_AllocFactor, 1.0)
                                                * c.[Condensate_WH_Production]
        FROM PCE_CDA c
        INNER JOIN Allocation_Factors a
            ON c.[Well Name] = a.[Well Name]
        WHERE a.MonthStartDate = ?
          AND c.ProdDate BETWEEN ? AND ?
        """,
        month_start,
        month_start_date,
        month_end_date,
    )
    cda_n = cursor.rowcount
    conn.commit()
    _log(lf.detail(f"PCE_CDA rows touched (S2 + condensate sales): {lf.num(cda_n)}"))

    _log(lf.detail("Syncing PCE_Production (Gas S2, condensate sales only)…"))
    cursor.execute(
        """
        UPDATE p SET
            p.[Gas S2 Production (10³m³)] = c.[Gas - S2 Production],
            p.[Condensate Sales (m³/d)] = c.[Condensate - Sales Production]
        FROM PCE_Production p
        INNER JOIN PCE_WM w ON p.[Well Name] = w.[Composite Name]
        INNER JOIN PCE_CDA c ON w.[Well Name] = c.[Well Name] AND p.[Date] = c.ProdDate
        WHERE c.ProdDate BETWEEN ? AND ?
          AND (w.[Exception] IS NULL OR w.[Exception] = '' OR w.[Exception] = 'N')
        """,
        month_start_date,
        month_end_date,
    )
    prod_n = cursor.rowcount
    conn.commit()
    _log(lf.detail(f"PCE_Production rows touched: {lf.num(prod_n)}"))


def apply_full_sales_ratios_for_month(
    conn,
    month_start: MonthStart,
    log: Optional[Callable[[str], None]] = None,
) -> Tuple[int, int, int]:
    """
    Public Sales pass: on PCE_CDA, only [Gas - Sales Production] (Accumap) and [Sales CGR Ratio]
    (from current CDA gas sales and condensate sales, typically after PA). PCE_Production gets
    a full four-column sync from PCE_CDA (S2, gas sales, condensate sales, CGR).
    Returns (cda_rows, production_rows, wells_count); cda_rows is the rowcount of the gas-sales
    UPDATE only.
    """
    def _log(msg: str) -> None:
        if log:
            log(msg)

    month_start_date, month_end_date, days_in_month = calendar_month_bounds(month_start)
    cursor = conn.cursor()

    _log(lf.detail("Updating PCE_CDA ([Gas - Sales Production] from Accumap)…"))
    cursor.execute(
        """
        UPDATE c SET
            c.[Gas - Sales Production] = CASE
                WHEN ISNULL(a.Sales_Gas, 0) > 0
                THEN ISNULL(a.WH_to_Sales_AllocFactor, 1.0) * c.[GasWH_Production]
                ELSE ISNULL(a.Sales_Gas, 0) / ?
            END
        FROM PCE_CDA c
        INNER JOIN Allocation_Factors a
            ON c.[Well Name] = a.[Well Name]
        WHERE a.MonthStartDate = ?
          AND c.ProdDate BETWEEN ? AND ?
        """,
        days_in_month,
        month_start,
        month_start_date,
        month_end_date,
    )
    cda_rows = cursor.rowcount

    _log(lf.detail("Recalculating PCE_CDA [Sales CGR Ratio]…"))
    cursor.execute(
        """
        UPDATE c SET
            c.[Sales CGR Ratio] = IIF(c.[Gas - Sales Production] > 0,
                c.[Condensate - Sales Production] / c.[Gas - Sales Production],
                0)
        FROM PCE_CDA c
        INNER JOIN Allocation_Factors a
            ON c.[Well Name] = a.[Well Name]
        WHERE a.MonthStartDate = ?
          AND c.ProdDate BETWEEN ? AND ?
        """,
        month_start,
        month_start_date,
        month_end_date,
    )
    conn.commit()

    cursor.execute(
        """
        SELECT COUNT(DISTINCT [Well Name])
        FROM Allocation_Factors
        WHERE MonthStartDate = ?
        """,
        month_start,
    )
    wells_count = cursor.fetchone()[0]

    cursor.execute(
        """
        UPDATE p SET
            p.[Gas S2 Production (10³m³)] = c.[Gas - S2 Production],
            p.[Gas Sales Production (10³m³)] = c.[Gas - Sales Production],
            p.[Condensate Sales (m³/d)] = c.[Condensate - Sales Production],
            p.[Sales CGR (m³/e³m³)] = c.[Sales CGR Ratio]
        FROM PCE_Production p
        INNER JOIN PCE_WM w ON p.[Well Name] = w.[Composite Name]
        INNER JOIN PCE_CDA c ON w.[Well Name] = c.[Well Name] AND p.[Date] = c.ProdDate
        WHERE c.ProdDate BETWEEN ? AND ?
          AND (w.[Exception] IS NULL OR w.[Exception] = '' OR w.[Exception] = 'N')
        """,
        month_start_date,
        month_end_date,
    )
    production_rows = cursor.rowcount
    conn.commit()

    return cda_rows, production_rows, wells_count
