"""
Production Accounting (PA) monthly loader.

Reads the ValNav month-end Excel export, refreshes ``Allocation_Factors`` for
the selected month (ValNav S2/condensate/NGL volumes, preserving ``Sales_Gas``
from Public Sales), and applies allocation factors and daily NGL ratios to
``PCE_CDA`` and ``PCE_Production``. Driven by the PA dialog and runs on a
``QThread`` worker.
"""

import log_format as lf
import pandas as pd
import pyodbc
import time
from datetime import datetime, timedelta
import os
import traceback
from db_connection import get_sql_conn
from sales_allocation_updates import (
    fetch_pce_uwi_to_well_name,
    fetch_pce_wm_wells,
    resolve_valnav_uwi_to_well_name,
)
from valnav_columns import (
    resolve_valnav_column,
    resolve_valnav_cond_column,
    resolve_valnav_gas_column,
    resolve_valnav_ngl_columns,
    resolve_valnav_uwi_column,
    strip_valnav_column_names,
)

_AF_NGL_COLS = ("NGL_C2", "NGL_C3", "NGL_C4", "NGL_C5", "PA_NGLs")

# ValNav excel NGL key -> Allocation_Factors column
_VALNAV_NGL_TO_AF = (
    ("NGL-C2", "NGL_C2"),
    ("NGL-C3", "NGL_C3"),
    ("NGL-C4", "NGL_C4"),
    ("NGL-C5", "NGL_C5"),
    ("NGLs", "PA_NGLs"),
)


def _af_month_sql_date(month_start: datetime):
    """Calendar month key for Allocation_Factors (date, not datetime)."""
    return month_start.date()


def _float_or_none_valnav(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if pd.isna(value):
        return None
    return float(value)


def _af_ngl_for_well(valnav_data_for_well, ngl_preserved: dict) -> dict:
    """NGL volumes for AF insert: ValNav sheet wins when present for the well."""
    if valnav_data_for_well is None:
        return ngl_preserved
    out = dict(ngl_preserved)
    for vn_key, af_col in _VALNAV_NGL_TO_AF:
        if vn_key in valnav_data_for_well:
            out[af_col] = valnav_data_for_well[vn_key]
    return out


def resolve_valnav_sheet_name(sheet_names, month_start: datetime) -> str:
    """
    Find the ValNav worksheet for *month_start*.

    Sheet name must contain the 4-digit year and either abbreviated or full month
    (e.g. ``Apr 2026`` or ``April 2026``). Raises ValueError if no sheet matches.
    """
    month_abbr = month_start.strftime("%b %Y")
    month_full = month_start.strftime("%B %Y")
    year = str(month_start.year)
    abbr_key = month_abbr.lower()
    full_key = month_full.lower()

    for sheet in sheet_names:
        sheet_lower = sheet.lower()
        if year not in sheet_lower:
            continue
        if abbr_key in sheet_lower or full_key in sheet_lower:
            return sheet

    available = ", ".join(sheet_names) if sheet_names else "(none)"
    raise ValueError(
        f"{month_abbr} is not in the ValNav Excel file. "
        f"Add a worksheet named like '{month_abbr}' or '{month_full}'. "
        f"Available sheets: {available}."
    )


def compute_wh_to_s2_alloc_factor(s2_gas, monthly_gathered_gas):
    """
    ValNav monthly S2 / summed gathered gas for the month.

    Applied to daily ``Gathered_Gas_Production`` on PCE_CDA (not WH gas).
    Returns None when gathered gas total is zero (never default to 1.0 pass-through).
    """
    gathered = float(monthly_gathered_gas) if monthly_gathered_gas is not None else 0.0
    if gathered <= 0:
        return None
    s2 = float(s2_gas) if s2_gas is not None else 0.0
    return s2 / gathered


def compute_wh_to_sales_cond_alloc_factor(sales_cond, prodview_wh_cond):
    """Sales cond / summed WH cond for the month; None when WH cond total is zero."""
    pw = float(prodview_wh_cond) if prodview_wh_cond is not None else 0.0
    if pw <= 0:
        return None
    sc = float(sales_cond) if sales_cond is not None else 0.0
    return sc / pw


def run_monthly_loader(month_str, valnav_path, progress_callback=None, log_callback=None, accumap_path=None):
    """
    Run the PA monthly loader (ValNav only). Accumap / public sales gas is applied from
    Public Sales Data and Ratios. ``accumap_path`` is ignored if passed (backward compatible).
    When reloading a month that already has ``Allocation_Factors`` rows, existing
    ``Sales_Gas`` per well is preserved (``WH_to_Sales_AllocFactor`` and
    ``Gathered_to_Sales`` are recomputed from ValNav/CDA and that value).

    Args:
        month_str: Month in format "MMM YYYY" (e.g., "Dec 2025")
        valnav_path: Path to ValNav Excel file
        progress_callback: Function to call with progress percentage (0-100)
        log_callback: Function to call with log messages
        accumap_path: Deprecated; ignored.

    Returns:
        dict: Summary statistics and warning messages
    """
    
    def log(message):
        """Send log message to GUI if callback exists"""
        if log_callback:
            log_callback(message)
        else:
            print(message)
    
    def progress(value):
        """Send progress to GUI if callback exists"""
        if progress_callback:
            progress_callback(value)
    
    total_start = time.time()
    
    # Parse month
    try:
        month_date = datetime.strptime(month_str, "%b %Y")
        month_start = month_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        log(lf.detail(f"Month selected: {month_start.strftime('%B %Y')}"))
    except Exception:
        error_msg = f"Invalid month format: {month_str}"
        log(lf.error(error_msg))
        return {"error": error_msg}
    
    # Validate files exist
    if not os.path.exists(valnav_path):
        error_msg = f"ValNav file not found: {valnav_path}"
        log(lf.error(error_msg))
        return {"error": error_msg}

    if accumap_path:
        log(lf.detail("Note: Accumap is not used in PA; load public sales gas via Public Sales Data and Ratios."))
    
    # Initialize variables
    valnav_data = {}
    valnav_uwis = set()
    existing_count = 0
    report_filename = None
    missing_report = None
    
    try:
        # Read ValNav data
        progress(10)
        
        xl_file = pd.ExcelFile(valnav_path)
        sheet_names = xl_file.sheet_names

        try:
            target_valnav_sheet = resolve_valnav_sheet_name(sheet_names, month_start)
        except ValueError as exc:
            log(lf.error(str(exc)))
            return {"error": str(exc)}

        log(
            lf.detail(
                f"Using ValNav sheet {target_valnav_sheet!r} for {month_start.strftime('%b %Y')}"
            )
        )

        # Read ValNav data
        df_valnav = pd.read_excel(valnav_path, sheet_name=target_valnav_sheet)
        strip_valnav_column_names(df_valnav)

        col_uwi = resolve_valnav_uwi_column(df_valnav)
        col_gas = resolve_valnav_gas_column(df_valnav)
        col_cond = resolve_valnav_cond_column(df_valnav)
        col_ngl = resolve_valnav_ngl_columns(df_valnav)
        log(
            lf.detail(
                f"ValNav sheet {target_valnav_sheet!r}: UWI column {col_uwi!r}, "
                f"S2 gas {col_gas!r}, condensate {col_cond!r}"
            )
        )
        if col_ngl:
            log(
                lf.detail(
                    "ValNav NGL columns: "
                    + ", ".join(f"{k}={v!r}" for k, v in col_ngl.items())
                )
            )
        else:
            log(
                lf.warn(
                    "ValNav sheet missing NGL columns (NGL-C2…C5, NGLs); "
                    "Allocation_Factors NGL volumes will not be updated from ValNav."
                )
            )

        # Clean ValNav UWI values
        df_valnav["UWI_clean_valnav"] = df_valnav[col_uwi].astype(str).str.strip()

        # Prepare ValNav data dictionary
        valnav_data = {}
        valnav_uwis = set()

        df_vn = df_valnav.dropna(subset=["UWI_clean_valnav"]).copy()
        df_vn["UWI_clean_valnav"] = df_vn["UWI_clean_valnav"].astype(str).str.strip()
        df_vn["_S2_Gas"] = pd.to_numeric(df_vn[col_gas], errors="coerce").fillna(0)
        df_vn["_Sales_Cond"] = pd.to_numeric(df_vn[col_cond], errors="coerce").fillna(0)
        df_vn = df_vn.drop_duplicates(subset=["UWI_clean_valnav"], keep="last")
        valnav_uwis = set(df_vn["UWI_clean_valnav"])
        _vn_idx = df_vn.set_index("UWI_clean_valnav")
        valnav_data = {}
        for uwi, r in _vn_idx.iterrows():
            entry = {
                "S2_Gas": float(r["_S2_Gas"]),
                "Sales_Cond": float(r["_Sales_Cond"]),
            }
            if col_ngl:
                for excel_key, col_name in col_ngl.items():
                    entry[excel_key] = _float_or_none_valnav(r.get(col_name))
            valnav_data[uwi] = entry
        
        log(lf.detail(f"Loaded {lf.num(len(valnav_data))} ValNav records"))
        if not valnav_data:
            log(lf.error("ValNav sheet has no UWI rows; nothing was changed in the database."))
            return {"error": "ValNav sheet has no UWI rows for the selected month."}
        progress(20)
        
        # -----------------------------------------------------------------
        # CONNECT TO SQL SERVER
        # -----------------------------------------------------------------
        log(lf.step("SQL Server operations"))
        
        log(lf.step("Connecting to SQL Server"))
        conn = get_sql_conn()
        cursor = conn.cursor()
        log(lf.success("Database connected"))
        progress(35)

        # -----------------------------------------------------------------
        # FETCH WELL MAPPINGS FROM PCE_WM
        # -----------------------------------------------------------------
        log(lf.step("Fetching well mappings from PCE_WM"))
        
        pce_uwi_dict = fetch_pce_uwi_to_well_name(cursor)
        log(lf.detail(f"Loaded {lf.num(len(pce_uwi_dict))} UWI lookup key(s) from PCE_WM"))
        progress(45)
        
        # -----------------------------------------------------------------
        # LOAD PCE_CDA DATA FOR THE MONTH
        # -----------------------------------------------------------------
        log(lf.step("Loading PCE_CDA data for month"))
        
        # Calculate month end date
        if month_start.month == 12:
            month_end = datetime(month_start.year + 1, 1, 1) - timedelta(days=1)
        else:
            month_end = datetime(month_start.year, month_start.month + 1, 1) - timedelta(days=1)
        
        month_start_date = month_start.date()
        month_end_date = month_end.date()
        
        log(lf.detail(f"Aggregating PCE_CDA data for {month_start.strftime('%B %Y')}..."))
        
        cursor.execute("""
            SELECT [Well Name], 
                   SUM(GasWH_Production) as TotalGasWH,
                   SUM(Condensate_WH_Production) as TotalCondWH,
                   SUM(Gathered_Gas_Production) as TotalGatheredGas,
                   SUM(Gathered_Condensate_Production) as TotalGatheredCond
            FROM PCE_CDA 
            WHERE ProdDate BETWEEN ? AND ?
            GROUP BY [Well Name]
        """, month_start_date, month_end_date)
        
        cda_results = cursor.fetchall()
        
        cda_lookup = {}
        for well_name, gas_wh, cond_wh, gathered_gas, gathered_cond in cda_results:
            if well_name:
                cda_lookup[well_name] = {
                    'prodview_wh_gas': float(gas_wh) if gas_wh is not None else 0,
                    'prodview_wh_cond': float(cond_wh) if cond_wh is not None else 0,
                    'gathered_gas': float(gathered_gas) if gathered_gas is not None else 0,
                    'gathered_cond': float(gathered_cond) if gathered_cond is not None else 0
                }
        
        log(lf.detail(f"Found CDA data for {lf.num(len(cda_lookup))} wells"))

        days_in_month = (month_end_date - month_start_date).days + 1
        log(lf.step("Monthly → daily (ValNav, informational only)"))
        log(lf.detail(
            f"{month_start.strftime('%B %Y')}: {lf.num(days_in_month)} calendar days; "
            "S2/condensate factors use monthly ValNav volumes vs summed daily CDA (unchanged)."
        ))
        progress(50)
        
        # -----------------------------------------------------------------
        # MATCH UWIS (ValNav only; Accumap is applied in Public Sales)
        # -----------------------------------------------------------------
        log(lf.step("Matching ValNav UWIs to wells"))
        
        log(lf.detail(f"Total unique ValNav UWIs: {lf.num(len(valnav_uwis))}"))
        
        matched_wells = {}
        unmatched_valnav = []
        
        for uwi in valnav_uwis:
            uwi_str = str(uwi)
            well_name = resolve_valnav_uwi_to_well_name(uwi_str, pce_uwi_dict)
            if well_name:
                if well_name not in matched_wells:
                    cda_data = cda_lookup.get(well_name, {
                        'prodview_wh_gas': 0, 
                        'prodview_wh_cond': 0,
                        'gathered_gas': 0, 
                        'gathered_cond': 0
                    })
                    
                    matched_wells[well_name] = {
                        'well_name': well_name,
                        'valnav_data': None,
                        'accumap_data': None,
                        'uwi': None,
                        'prodview_wh_gas': cda_data['prodview_wh_gas'],
                        'prodview_wh_cond': cda_data['prodview_wh_cond'],
                        'gathered_gas': cda_data['gathered_gas'],
                        'gathered_cond': cda_data['gathered_cond']
                    }
                
                if uwi_str in valnav_data:
                    matched_wells[well_name]['valnav_data'] = valnav_data[uwi_str]
                    matched_wells[well_name]['uwi'] = uwi_str
            else:
                unmatched_valnav.append(uwi_str)
        
        log(lf.detail(f"Successfully matched: {lf.num(len(matched_wells))} wells"))
        log(lf.detail(f"Unmatched ValNav UWIs: {lf.num(len(unmatched_valnav))}"))
        progress(60)
        
        # -----------------------------------------------------------------
        # ENSURE ALL PCE_WM WELLS HAVE AF ROWS (ValNav + zero stubs)
        # -----------------------------------------------------------------
        log(lf.step("Ensuring all PCE_WM wells are included"))
        warning_messages = []

        wm_wells = fetch_pce_wm_wells(cursor)
        log(lf.detail(f"Total wells in PCE_WM: {lf.num(len(wm_wells))}"))

        valnav_matched_wells = {}
        for wn, data in matched_wells.items():
            if data.get("valnav_data") is None:
                continue
            key = str(wn).strip() if wn else ""
            if not key:
                continue
            valnav_matched_wells[key] = {**data, "well_name": key}
        log(lf.detail(
            f"Wells matched from ValNav: {lf.num(len(valnav_matched_wells))}"
        ))

        wells_added = 0
        for wm_well_name, wm_uwi in wm_wells:
            wm_key = str(wm_well_name).strip() if wm_well_name else ""
            if not wm_key:
                continue
            if wm_key in valnav_matched_wells:
                if not valnav_matched_wells[wm_key].get("uwi") and wm_uwi:
                    valnav_matched_wells[wm_key]["uwi"] = wm_uwi
                continue
            cda_data = cda_lookup.get(wm_key, cda_lookup.get(wm_well_name, {
                'prodview_wh_gas': 0,
                'prodview_wh_cond': 0,
                'gathered_gas': 0,
                'gathered_cond': 0,
            }))
            valnav_matched_wells[wm_key] = {
                'well_name': wm_key,
                'valnav_data': None,
                'accumap_data': None,
                'uwi': wm_uwi,
                'prodview_wh_gas': cda_data['prodview_wh_gas'],
                'prodview_wh_cond': cda_data['prodview_wh_cond'],
                'gathered_gas': cda_data['gathered_gas'],
                'gathered_cond': cda_data['gathered_cond'],
            }
            wells_added += 1

        if wells_added > 0:
            log(lf.detail(
                f"Added {lf.num(wells_added)} PCE_WM well(s) with zero ValNav volumes"
            ))
            warning_messages.append(
                f"{wells_added} wells had no ValNav data (zero stubs written)"
            )
        else:
            log(lf.success("All PCE_WM wells had ValNav data for this month"))
        valnav_with_data = sum(
            1 for d in valnav_matched_wells.values() if d.get("valnav_data") is not None
        )
        if valnav_with_data == 0:
            conn.close()
            msg = (
                f"No ValNav rows matched PCE_WM for {month_start.strftime('%b %Y')}. "
                "Nothing was changed in the database."
            )
            log(lf.error(msg))
            return {"error": msg}

        log(
            lf.detail(
                f"Wells for {month_start.strftime('%b %Y')}: "
                f"{lf.num(valnav_with_data)} with ValNav data, "
                f"{lf.num(len(valnav_matched_wells))} total (incl. PCE_WM zero stubs)"
            )
        )

        # -----------------------------------------------------------------
        # DELETE EXISTING ALLOCATION_FACTORS FOR THE SELECTED MONTH ONLY
        # -----------------------------------------------------------------
        log(lf.step(f"Clearing Allocation_Factors for {month_start.strftime('%B %Y')}"))
        af_month = _af_month_sql_date(month_start)

        preserved_sales_gas = {}
        preserved_ngl = {}
        ngl_select = ", ".join(f"[{c}]" for c in _AF_NGL_COLS)
        cursor.execute(
            f"""
            SELECT [Well Name], Sales_Gas, {ngl_select}
            FROM Allocation_Factors
            WHERE CAST(MonthStartDate AS DATE) = ?
            """,
            af_month,
        )
        for row in cursor.fetchall():
            wn = row[0]
            if not wn:
                continue
            key = str(wn).strip()
            sg = row[1]
            try:
                preserved_sales_gas[key] = float(sg) if sg is not None else 0.0
            except (TypeError, ValueError):
                preserved_sales_gas[key] = 0.0
            ngl_vals = {}
            for i, col in enumerate(_AF_NGL_COLS, start=2):
                val = row[i]
                if val is None:
                    ngl_vals[col] = None
                else:
                    try:
                        ngl_vals[col] = float(val)
                    except (TypeError, ValueError):
                        ngl_vals[col] = None
            preserved_ngl[key] = ngl_vals

        existing_count = len(preserved_sales_gas)
        if existing_count > 0:
            n_nonzero = sum(1 for v in preserved_sales_gas.values() if v != 0.0)
            log(
                lf.detail(
                    f"Preserving Sales_Gas for {lf.num(existing_count)} well(s) "
                    f"({lf.num(n_nonzero)} non-zero) before reload"
                )
            )
            n_ngl_pres = sum(
                1
                for vals in preserved_ngl.values()
                if any(v is not None for v in vals.values())
            )
            if n_ngl_pres:
                log(
                    lf.detail(
                        f"Preserving NGL volumes for {lf.num(n_ngl_pres)} well(s) before reload"
                    )
                )

        cursor.execute(
            """
            DELETE FROM Allocation_Factors
            WHERE CAST(MonthStartDate AS DATE) = ?
            """,
            af_month,
        )
        deleted_count = cursor.rowcount if cursor.rowcount and cursor.rowcount >= 0 else existing_count
        conn.commit()
        log(
            lf.detail(
                f"Deleted {lf.num(deleted_count)} existing record(s) for "
                f"{month_start.strftime('%B %Y')}"
            )
        )

        progress(70)
        
        # -----------------------------------------------------------------
        # INSERT COMBINED DATA
        # -----------------------------------------------------------------
        log(lf.step("Inserting combined data"))
        
        valnav_source = os.path.basename(valnav_path)
        loaded_at = datetime.now()
        
        wells_inserted = 0
        wells_valnav_only = 0
        wells_with_cda = 0
        errors = 0
        
        log(lf.detail(f"Inserting data for {lf.num(len(valnav_matched_wells))} wells"))
        
        combined_source = f"ValNav: {valnav_source} (public sales gas via Public Sales dialog)"
        rows_to_insert = []

        for well_name, well_data in valnav_matched_wells.items():
            try:
                valnav_data_for_well = well_data["valnav_data"]

                prodview_wh_gas = well_data["prodview_wh_gas"]
                prodview_wh_cond = well_data["prodview_wh_cond"]
                gathered_gas = well_data["gathered_gas"]
                gathered_cond = well_data["gathered_cond"]

                has_cda = (
                    prodview_wh_gas > 0
                    or prodview_wh_cond > 0
                    or gathered_gas > 0
                    or gathered_cond > 0
                )

                if valnav_data_for_well is not None:
                    wells_valnav_only += 1
                if has_cda:
                    wells_with_cda += 1

                if valnav_data_for_well is not None:
                    s2_gas = valnav_data_for_well["S2_Gas"]
                    sales_cond = valnav_data_for_well["Sales_Cond"]
                else:
                    s2_gas = 0.0
                    sales_cond = 0.0
                wkey = str(well_name).strip() if well_name else ""
                sales_gas = preserved_sales_gas.get(wkey, 0.0)
                ngl_preserved = preserved_ngl.get(wkey, {})
                ngl_for_af = _af_ngl_for_well(valnav_data_for_well, ngl_preserved)
                well_uwi = well_data.get("uwi")

                wh_to_s2 = compute_wh_to_s2_alloc_factor(s2_gas, gathered_gas)
                wh_to_sales_cond = compute_wh_to_sales_cond_alloc_factor(
                    sales_cond, prodview_wh_cond
                )
                wh_to_sales_gas = (
                    None
                    if prodview_wh_gas <= 0
                    else sales_gas / prodview_wh_gas
                )

                gathered_to_s2_str = (
                    None if gathered_gas <= 0 else str(s2_gas / gathered_gas)
                )
                gathered_to_sales_str = (
                    None if gathered_gas <= 0 else str(sales_gas / gathered_gas)
                )
                gathered_to_sales_cond_str = (
                    None if gathered_cond <= 0 else str(sales_cond / gathered_cond)
                )

                wkey_insert = str(well_name).strip() if well_name else ""
                rows_to_insert.append((
                    af_month, wkey_insert, well_uwi,
                    prodview_wh_gas, prodview_wh_cond,
                    s2_gas, sales_cond, sales_gas,
                    gathered_gas, gathered_cond,
                    wh_to_s2, wh_to_sales_gas, wh_to_sales_cond,
                    gathered_to_s2_str, gathered_to_sales_str, gathered_to_sales_cond_str,
                    ngl_for_af.get("NGL_C2"),
                    ngl_for_af.get("NGL_C3"),
                    ngl_for_af.get("NGL_C4"),
                    ngl_for_af.get("NGL_C5"),
                    ngl_for_af.get("PA_NGLs"),
                    combined_source, loaded_at,
                ))
            except Exception as e:
                errors += 1
                if errors <= 5:
                    log(lf.error(f"Preparing well '{well_name}': {str(e)[:100]}"))

        # One row per (month, well); guard against duplicate keys in the batch.
        deduped_rows = {}
        for row in rows_to_insert:
            deduped_rows[(row[0], row[1])] = row
        if len(deduped_rows) < len(rows_to_insert):
            log(
                lf.warn(
                    f"Dropped {lf.num(len(rows_to_insert) - len(deduped_rows))} "
                    "duplicate well row(s) before insert"
                )
            )
        rows_to_insert = list(deduped_rows.values())
        total_loaded_wells = len(valnav_matched_wells)

        insert_sql = """
            INSERT INTO Allocation_Factors (
                MonthStartDate, [Well Name], [UWI],
                Prodview_WH_Gas, Prodview_WH_Cond,
                S2_Gas, Sales_Condensate, Sales_Gas,
                Gathered_Gas_Production, Gathered_Condensate_Production,
                WH_to_S2_AllocFactor, WH_to_Sales_AllocFactor, WH_to_Sales_Cond_AllocFactor,
                Gathered_to_S2_Gas, Gathered_to_Sales, Gathered_to_Sales_Condensate,
                NGL_C2, NGL_C3, NGL_C4, NGL_C5, PA_NGLs,
                SourceFile, LoadedAt
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.fast_executemany = True
        batch_size = 5000
        for i in range(0, len(rows_to_insert), batch_size):
            batch = rows_to_insert[i:i + batch_size]
            cursor.executemany(insert_sql, batch)
            wells_inserted += len(batch)
            if wells_inserted % 50 == 0 or (i + len(batch)) >= len(rows_to_insert):
                log(lf.detail(f"Inserted {lf.num(wells_inserted)} wells"))
                progress(70 + (wells_inserted / max(len(valnav_matched_wells), 1) * 20))

        conn.commit()
        progress(85)

        from sales_allocation_updates import apply_valnav_allocation_to_cda_and_production

        log(lf.step("Applying ValNav allocation to PCE_CDA and PCE_Production"))
        apply_valnav_allocation_to_cda_and_production(conn, month_start, log=log)

        progress(88)
        from ngl_monthly_update import run_ngl_monthly_from_allocation_factors

        log(lf.step("Applying monthly NGL ratios from Allocation_Factors to PCE_Production"))
        ngl_summary = run_ngl_monthly_from_allocation_factors(
            conn, month_start, log=log
        )
        progress(98)

        conn.close()
        progress(100)
        
        # -----------------------------------------------------------------
        # RETURN SUMMARY
        # -----------------------------------------------------------------
        total_time = time.time() - total_start
        
        summary = {
            'valnav_records': len(valnav_data),
            'matched_wells': len(valnav_matched_wells) - wells_added,
            'wells_added': wells_added,
            'total_wells': len(valnav_matched_wells),
            'duration': total_time,
            'warnings': ', '.join(warning_messages) if warning_messages else None,
            'month': month_str,
            'ngl_skipped': ngl_summary.skipped,
            'ngl_skip_reason': ngl_summary.skip_reason,
            'ngl_wells': ngl_summary.wells_matched,
            'ngl_rows_updated': ngl_summary.rows_updated,
        }
        
        complete_metrics = {
            "Completed": lf.timestamp(),
            "Month": summary["month"],
            "ValNav records": summary["valnav_records"],
            "Wells matched": summary["matched_wells"],
            "Wells added (zeros)": summary["wells_added"],
            "Total wells": summary["total_wells"],
            "NGL wells": summary["ngl_wells"],
            "NGL prod rows updated": summary["ngl_rows_updated"],
            "Duration": lf.elapsed(total_time),
            "Note": "Run Public Sales Data and Ratios to load Accumap and refresh gas sales + CGR",
        }
        if ngl_summary.skipped:
            complete_metrics["NGL skipped"] = ngl_summary.skip_reason
        if summary["warnings"]:
            complete_metrics["Warnings"] = summary["warnings"]
        log(lf.summary("COMPLETE", complete_metrics))
        
        return summary
        
    except Exception as e:
        error_msg = str(e)
        log(lf.error(error_msg))
        log(traceback.format_exc())
        return {"error": error_msg}
    