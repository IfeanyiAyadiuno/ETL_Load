import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

import log_format as lf
from db_connection import get_sql_conn
from snowflake_connector import SnowflakeConnector


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_SF_QUERIES = {
    "ecf": (
        "GASIDREC",
        """
        SELECT
            IDRECPARENT AS GasIDREC,
            CAST(DTTM AS DATE) AS ProdDate,
            EFFLUENTFACTOR AS ECF_Ratio
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitMeterOrificeEcf
        WHERE DTTM >= %s AND DTTM <= %s
        """,
    ),
    "gaswh": (
        "GASIDREC",
        """
        SELECT
            IDRECPARENT AS GasIDREC,
            CAST(DTTM AS DATE) AS ProdDate,
            VOLENTERGAS AS GasWH_Production,
            DURONOR AS OnProdHours
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitMeterOrificeEntry
        WHERE DTTM >= %s AND DTTM <= %s
        """,
    ),
    "cgr": (
        "PRESSURESIDREC",
        """
        SELECT
            IDRECCOMP AS PressuresIDREC,
            CAST(DTTM AS DATE) AS ProdDate,
            CASE
                WHEN RATEGAS IS NULL OR RATEGAS = 0 THEN NULL
                ELSE (RATEHCLIQ / RATEGAS)
            END AS CGR_Ratio
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitCompGathMonthDayCalc
        WHERE DTTM >= %s AND DTTM <= %s
        """,
    ),
    "wgr": (
        "PRESSURESIDREC",
        """
        SELECT
            IDRECPARENT AS PressuresIDREC,
            CAST(DTTM AS DATE) AS ProdDate,
            WGR AS WGR_Ratio
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitCompRatios
        WHERE DTTM >= %s AND DTTM <= %s
        """,
    ),
    "pressures": (
        "PRESSURESIDREC",
        """
        SELECT
            IDRECPARENT AS PressuresIDREC,
            CAST(DTTM AS DATE) AS ProdDate,
            PRESTUB AS TubingPressure,
            PRESCAS AS CasingPressure,
            SZCHOKE AS ChokeSize
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitCompParam
        WHERE DTTM >= %s AND DTTM <= %s
        """,
    ),
    "alloc": (
        "PRESSURESIDREC",
        """
        SELECT
            IDRECCOMP AS PressuresIDREC,
            CAST(DTTM AS DATE) AS ProdDate,
            VOLPRODGATHGAS AS Gathered_Gas_Production,
            VOLPRODGATHHCLIQ AS Gathered_Condensate_Production,
            VOLNEWPRODALLOCNGL AS NGL_Production
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvunitallocmonthday
        WHERE DTTM >= %s AND DTTM <= %s
        """,
    ),
    "water": (
        "PRESSURESIDREC",
        """
        SELECT
            IDRECCOMP AS PressuresIDREC,
            CAST(DTTM AS DATE) AS ProdDate,
            VOLWATER AS AllocatedWater_Rate
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvunitcompgathmonthdaycalc
        WHERE DTTM >= %s AND DTTM <= %s
        """,
    ),
}


def _pull_all_snowflake_data(sf, start_date, end_date, log):
    """Pull all 7 Snowflake datasets in one shot for the full date range."""
    params = (str(start_date), str(end_date))
    results = {}
    for name, (_id_col, sql) in _SF_QUERIES.items():
        df = sf.query(sql, params)
        results[name] = df
        log(lf.detail(f"  {name}: {lf.num(len(df))} rows"))
    return results


def _fetch_well_mapping(cursor):
    """Fetch well mapping from PCE_WM (exclude exception wells)."""
    cursor.execute("""
        SELECT
            GasIDREC,
            PressuresIDREC,
            [Well Name],
            [Formation Producer],
            [Layer Producer],
            [Fault Block],
            [Pad Name],
            [Lateral Length],
            [Orient]
        FROM PCE_WM
        WHERE GasIDREC IS NOT NULL
          AND ([Exception] IS NULL OR [Exception] = '' OR [Exception] = 'N')
    """)
    cols = [
        'GasIDREC', 'PressuresIDREC', 'Well Name',
        'Formation Producer', 'Layer Producer', 'Fault Block',
        'Pad Name', 'Lateral Length', 'Orient',
    ]
    rows = cursor.fetchall()
    return pd.DataFrame.from_records(rows, columns=cols)


def _build_spine(mapping_df, date_range):
    """Build the wells x dates cross-join spine using vectorized operations."""
    dates_df = pd.DataFrame({'ProdDate': date_range})
    mapping_df = mapping_df.copy()
    mapping_df['_key'] = 1
    dates_df['_key'] = 1
    spine = mapping_df.merge(dates_df, on='_key').drop(columns='_key')
    return spine


def _prepare_sf_df(df, id_col, date_col, value_cols):
    """Clean and deduplicate a Snowflake result set."""
    if df.empty:
        return pd.DataFrame()

    column_map = {col.upper(): col for col in df.columns}

    result = pd.DataFrame()
    src_id = column_map.get(id_col.upper(), id_col)
    result['_join_key'] = df[src_id].astype(str).str.strip()

    src_date = column_map.get(date_col.upper(), date_col)
    result['ProdDate'] = pd.to_datetime(df[src_date]).dt.date

    for vc in value_cols:
        src = column_map.get(vc.upper(), vc)
        if src in df.columns:
            result[vc] = pd.to_numeric(df[src], errors='coerce')
        else:
            result[vc] = np.nan

    result = (
        result
        .sort_values(['_join_key', 'ProdDate'])
        .groupby(['_join_key', 'ProdDate'], as_index=False)
        .last()
    )
    return result


def _merge_sf_data(spine_df, sf_data):
    """Merge all 7 Snowflake datasets onto the spine."""
    result = spine_df.copy()

    merge_specs = [
        ('ecf',       'GasIDREC',       ['ECF_Ratio']),
        ('gaswh',     'GasIDREC',       ['GasWH_Production', 'OnProdHours']),
        ('cgr',       'PressuresIDREC', ['CGR_Ratio']),
        ('wgr',       'PressuresIDREC', ['WGR_Ratio']),
        ('pressures', 'PressuresIDREC', ['TubingPressure', 'CasingPressure', 'ChokeSize']),
        ('alloc',     'PressuresIDREC', ['Gathered_Gas_Production', 'Gathered_Condensate_Production', 'NGL_Production']),
        ('water',     'PressuresIDREC', ['AllocatedWater_Rate']),
    ]

    for name, join_col, value_cols in merge_specs:
        raw = sf_data.get(name, pd.DataFrame())
        id_key = _SF_QUERIES[name][0]
        processed = _prepare_sf_df(raw, id_key, 'PRODDATE', value_cols)

        if processed.empty:
            for vc in value_cols:
                result[vc] = np.nan
        else:
            processed = processed.rename(columns={'_join_key': join_col})
            result = result.merge(
                processed,
                on=[join_col, 'ProdDate'],
                how='left',
            )

    return result


def _apply_gaswh_replacement(result_df):
    """Vectorized GasWH replacement: use Gathered when GasWH is missing/tiny."""
    if 'GasWH_Production' not in result_df.columns or 'Gathered_Gas_Production' not in result_df.columns:
        return result_df, 0

    gas = result_df['GasWH_Production']
    gathered = result_df['Gathered_Gas_Production']
    has_gathered = gathered.notna()

    replace_mask = has_gathered & (
        gas.isna() | (gas == 0) | ((gas > 0) & (gas <= 2))
    )

    result_df.loc[replace_mask, 'GasWH_Production'] = gathered[replace_mask]
    result_df['Condensate_WH_Production'] = (
        result_df['GasWH_Production'] * result_df['CGR_Ratio']
    )
    return result_df, int(replace_mask.sum())


def _nan_to_none(val):
    """Convert NaN/NaT to None for DB insertion; keep everything else."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    return val


def _df_to_insert_rows(df, columns):
    """Convert DataFrame columns to list-of-tuples with NaN→None, vectorized."""
    sub = df[columns].copy()
    sub = sub.where(sub.notna(), None)
    return [tuple(_nan_to_none(v) for v in row) for row in sub.itertuples(index=False, name=None)]


_CDA_INSERT_SQL = """
INSERT INTO PCE_CDA (
    [GasIDREC], [PressuresIDREC], [Well Name], [ProdDate],
    [GasWH_Production], [Condensate_WH_Production],
    [WGR_Ratio], [CGR_Ratio], [ECF_Ratio],
    [OnProdHours], [TubingPressure], [CasingPressure], [ChokeSize],
    [Gathered_Gas_Production], [Gathered_Condensate_Production],
    [NGL_Production], [AllocatedWater_Rate],
    [Formation Producer], [Layer Producer], [Fault Block], [Pad Name],
    [Lateral Length], [Orient]
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_CDA_COLUMNS = [
    'GasIDREC', 'PressuresIDREC', 'Well Name', 'ProdDate',
    'GasWH_Production', 'Condensate_WH_Production',
    'WGR_Ratio', 'CGR_Ratio', 'ECF_Ratio',
    'OnProdHours', 'TubingPressure', 'CasingPressure', 'ChokeSize',
    'Gathered_Gas_Production', 'Gathered_Condensate_Production',
    'NGL_Production', 'AllocatedWater_Rate',
    'Formation Producer', 'Layer Producer', 'Fault Block', 'Pad Name',
    'Lateral Length', 'Orient',
]

_PROD_INSERT_SQL = """
INSERT INTO PCE_Production (
    [Date], [Days Seq], [Day Seq UPRT], [Well Name],
    [Gas WH Production (10³m³)], [Condensate WH (m³/d)],
    [Gas S2 Production (10³m³)], [Gas Sales Production (10³m³)],
    [Condensate Sales (m³/d)], [Gathered Gas (e³m³/d)],
    [Gathered Condensate (m³/d)], [Sales CGR (m³/e³m³)],
    [CGR (m³/e³m³)], [WGR (m³/e³m³)], [ECF],
    [Hours On], [Tubing Pressure (kPa)], [Casing Pressure (kPa)],
    [Choke Size], [Gas WH Cumulative Production (10³m³)],
    [Gas S2 Cumulative Production (10³m³)],
    [Gas Sales Cumulative Production (10³m³)],
    [Condensate Sales Cumulative Production (m³)],
    [Condensate WH Cumulative Production (m³)],
    [Gas Gathered Cumulative (e³m³)],
    [Condensate Gathered Cumulative (m³)],
    [Formation Producer], [Layer Producer], [Fault Block],
    [Pad Name], [Lateral Length], [Orientation],
    [On Production Year], [Alloc. Water Rate (m³)], [NGL (m³)],
    [Gas WH Avg (10³m³)], [Gas S2 Avg (10³m³)],
    [Gas Gathered Avg (e³m³/d)], [Condensate Gathered Avg (m³/d)]
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_PROD_COLUMNS = [
    'Date', 'Days Seq', 'Day Seq UPRT', 'Well Name',
    'Gas WH Production (10³m³)', 'Condensate WH (m³/d)',
    'Gas S2 Production (10³m³)', 'Gas Sales Production (10³m³)',
    'Condensate Sales (m³/d)', 'Gathered Gas (e³m³/d)',
    'Gathered Condensate (m³/d)', 'Sales CGR (m³/e³m³)',
    'CGR (m³/e³m³)', 'WGR (m³/e³m³)', 'ECF',
    'Hours On', 'Tubing Pressure (kPa)', 'Casing Pressure (kPa)',
    'Choke Size', 'Gas WH Cumulative Production (10³m³)',
    'Gas S2 Cumulative Production (10³m³)',
    'Gas Sales Cumulative Production (10³m³)',
    'Condensate Sales Cumulative Production (m³)',
    'Condensate WH Cumulative Production (m³)',
    'Gas Gathered Cumulative (e³m³)',
    'Condensate Gathered Cumulative (m³)',
    'Formation Producer', 'Layer Producer', 'Fault Block',
    'Pad Name', 'Lateral Length', 'Orientation',
    'On Production Year', 'Alloc. Water Rate (m³)', 'NGL (m³)',
    'Gas WH Avg (10³m³)', 'Gas S2 Avg (10³m³)',
    'Gas Gathered Avg (e³m³/d)', 'Condensate Gathered Avg (m³/d)',
]


def _month_boundaries(dt):
    """Return (first_day, last_day) as date objects for the month of *dt*."""
    first = dt.replace(day=1)
    if first.month == 12:
        last = datetime(first.year + 1, 1, 1) - timedelta(days=1)
    else:
        last = datetime(first.year, first.month + 1, 1) - timedelta(days=1)
    return first.date(), last.date()


def _generate_months(start_dt, end_dt):
    """Yield datetime objects for each month in the range."""
    current = start_dt
    while current <= end_dt:
        yield current
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)


# ---------------------------------------------------------------------------
# run_prodview_update  (Full Rebuild of CDA for selected range)
# ---------------------------------------------------------------------------

def run_prodview_update(start_month, end_month, progress_callback=None, log_callback=None):
    """
    Update production data from Snowflake for a range of months.

    Returns:
        dict: Summary statistics
    """

    def log(message):
        if log_callback:
            log_callback(message)
        else:
            print(message)

    def progress(value):
        if progress_callback:
            progress_callback(value)

    log(lf.header(
        "PRODVIEW/SNOWFLAKE DAILY PRODUCTION RETRIEVE",
        Range=f"{start_month} to {end_month}",
    ))

    total_start = time.time()

    try:
        start_date = datetime.strptime(start_month, "%b %Y")
        end_date = datetime.strptime(end_month, "%b %Y")

        if start_date > end_date:
            error_msg = "Start month must be before end month"
            log(lf.error(error_msg))
            return {"error": error_msg}

        # SQL Server connection
        log(lf.step("Connecting to SQL Server..."))
        conn = get_sql_conn()
        cursor = conn.cursor()
        cursor.fast_executemany = True
        log(lf.success("Database connected"))

        # Well mapping
        log(lf.step("Fetching well mapping from PCE_WM..."))
        mapping_df = _fetch_well_mapping(cursor)
        log(lf.detail(f"Loaded {lf.num(len(mapping_df))} wells"))

        months = list(_generate_months(start_date, end_date))
        total_months = len(months)
        log(lf.detail(f"Found {lf.num(total_months)} months to process"))

        if total_months == 0:
            return {'months_processed': 0, 'wells_updated': 0,
                    'cda_records': 0, 'production_records': 0, 'duration': 0}

        # ---- Single Snowflake pull for entire range ----
        overall_start = start_date.date()
        _, overall_end = _month_boundaries(end_date)

        log(lf.step(f"Pulling all Snowflake data ({overall_start} to {overall_end})..."))
        sf = SnowflakeConnector()
        try:
            sf_data = _pull_all_snowflake_data(sf, overall_start, overall_end, log)
        finally:
            sf.close()

        total_cda_records = 0
        total_production_records = 0

        for month_idx, month_dt in enumerate(months):
            month_start, month_end = _month_boundaries(month_dt)
            month_name = month_dt.strftime('%B %Y')
            date_range = pd.date_range(start=month_start, end=month_end, freq='D').date

            log(lf.step(f"Processing {month_name} ({month_idx + 1}/{total_months})"))

            # Slice Snowflake data for this month
            month_sf = {}
            for name, full_df in sf_data.items():
                if full_df.empty:
                    month_sf[name] = full_df
                    continue
                col_map = {c.upper(): c for c in full_df.columns}
                date_col = col_map.get('PRODDATE', 'ProdDate')
                dates = pd.to_datetime(full_df[date_col]).dt.date
                month_sf[name] = full_df[(dates >= month_start) & (dates <= month_end)]

            # Delete existing data for this month
            cursor.execute(
                "DELETE FROM PCE_CDA WHERE ProdDate BETWEEN ? AND ?",
                month_start, month_end,
            )
            deleted_cda = cursor.rowcount
            cursor.execute(
                "DELETE FROM PCE_Production WHERE [Date] BETWEEN ? AND ?",
                month_start, month_end,
            )
            deleted_prod = cursor.rowcount
            conn.commit()
            if deleted_cda > 0 or deleted_prod > 0:
                log(lf.detail(
                    f"Cleared {lf.num(deleted_cda)} CDA and "
                    f"{lf.num(deleted_prod)} Production records"
                ))

            # Build spine (vectorized cross-join)
            spine_df = _build_spine(mapping_df, date_range)

            # Merge all Snowflake data
            result_df = _merge_sf_data(spine_df, month_sf)

            # Condensate WH initial calc
            result_df['Condensate_WH_Production'] = (
                result_df['GasWH_Production'] * result_df['CGR_Ratio']
            )

            # Vectorized GasWH replacement
            result_df, _repl = _apply_gaswh_replacement(result_df)

            # Insert into PCE_CDA (vectorized tuple construction + executemany)
            rows = _df_to_insert_rows(result_df, _CDA_COLUMNS)
            batch_size = 5000
            rows_inserted = 0
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                cursor.executemany(_CDA_INSERT_SQL, batch)
                rows_inserted += len(batch)
            conn.commit()
            total_cda_records += rows_inserted
            log(lf.detail(f"Inserted {lf.num(rows_inserted)} records into PCE_CDA"))

            # Insert PCE_Production from CDA via server-side SELECT
            cursor.execute("""
                INSERT INTO PCE_Production (
                    [Date], [Well Name],
                    [Days Seq], [Day Seq UPRT],
                    [Gas WH Production (10³m³)], [Condensate WH (m³/d)],
                    [Gas S2 Production (10³m³)], [Gas Sales Production (10³m³)],
                    [Condensate Sales (m³/d)], [Gathered Gas (e³m³/d)],
                    [Gathered Condensate (m³/d)], [Sales CGR (m³/e³m³)],
                    [CGR (m³/e³m³)], [WGR (m³/e³m³)], [ECF],
                    [Hours On], [Tubing Pressure (kPa)], [Casing Pressure (kPa)],
                    [Choke Size],
                    [Alloc. Water Rate (m³)], [NGL (m³)],
                    [Formation Producer], [Layer Producer], [Fault Block],
                    [Pad Name], [Lateral Length], [Orientation]
                )
                SELECT
                    c.ProdDate,
                    c.[Well Name],
                    0, 0,
                    c.GasWH_Production,
                    c.Condensate_WH_Production,
                    c.[Gas - S2 Production],
                    c.[Gas - Sales Production],
                    c.[Condensate - Sales Production],
                    c.Gathered_Gas_Production,
                    c.Gathered_Condensate_Production,
                    c.[Sales CGR Ratio],
                    c.CGR_Ratio,
                    c.WGR_Ratio,
                    c.ECF_Ratio,
                    c.OnProdHours,
                    c.TubingPressure,
                    c.CasingPressure,
                    c.ChokeSize,
                    c.AllocatedWater_Rate,
                    c.NGL_Production,
                    c.[Formation Producer],
                    c.[Layer Producer],
                    c.[Fault Block],
                    c.[Pad Name],
                    c.[Lateral Length],
                    c.Orient
                FROM PCE_CDA c
                WHERE c.ProdDate BETWEEN ? AND ?
            """, month_start, month_end)
            prod_inserted = cursor.rowcount
            conn.commit()
            total_production_records += prod_inserted

            progress(int((month_idx + 1) / total_months * 100))

        # Recalculate sequences for affected wells
        cursor.execute("""
            SELECT DISTINCT [Well Name]
            FROM PCE_CDA
            WHERE ProdDate BETWEEN ? AND ?
        """, start_date.date(), end_date.date())
        affected_wells = [row[0] for row in cursor.fetchall()]
        log(lf.step(f"Recalculating sequences for {lf.num(len(affected_wells))} wells..."))

        for well_idx, well_name in enumerate(affected_wells):
            cursor.execute("""
                SELECT ProdDate, GasWH_Production
                FROM PCE_CDA
                WHERE [Well Name] = ?
                ORDER BY ProdDate
            """, well_name)
            well_data = cursor.fetchall()

            days_seq = list(range(1, len(well_data) + 1))
            gas_wh_values = [row[1] or 0 for row in well_data]

            day_seq_uprt = []
            counter = 1
            i = 0
            while i < len(gas_wh_values):
                day_seq_uprt.append(counter)
                if gas_wh_values[i] >= 1:
                    counter += 1
                    i += 1
                else:
                    j = i + 1
                    while j < len(gas_wh_values) and gas_wh_values[j] < 1:
                        day_seq_uprt.append(counter)
                        j += 1
                    i = j
                    counter += 1

            update_rows = [
                (days_seq[idx], day_seq_uprt[idx], well_name, date)
                for idx, (date, _) in enumerate(well_data)
            ]
            cursor.executemany("""
                UPDATE PCE_Production
                SET [Days Seq] = ?, [Day Seq UPRT] = ?
                WHERE [Well Name] = ? AND [Date] = ?
            """, update_rows)

            if (well_idx + 1) % 20 == 0:
                conn.commit()

        conn.commit()
        conn.close()

        total_time = time.time() - total_start
        summary = {
            'months_processed': total_months,
            'wells_updated': len(affected_wells),
            'cda_records': total_cda_records,
            'production_records': total_production_records,
            'duration': total_time,
        }

        log(lf.summary("COMPLETE", {
            "Months processed": total_months,
            "Wells updated": len(affected_wells),
            "PCE_CDA records": total_cda_records,
            "PCE_Production records": total_production_records,
            "Duration": lf.elapsed(total_time),
        }))
        return summary

    except Exception as e:
        error_msg = f"ERROR: {str(e)}"
        log(lf.error(str(e)))
        import traceback
        for line in traceback.format_exc().strip().split("\n"):
            log(lf.detail(line))
        return {"error": error_msg}


# ---------------------------------------------------------------------------
# run_quick_update  (Quick Update for selected range + full recalculation)
# ---------------------------------------------------------------------------

def run_quick_update(start_month, end_month, progress_callback=None, log_callback=None):
    """
    Quick update: processes only selected month range for PCE_CDA,
    then recalculates sequences/cumulatives/averages for affected wells.

    Returns:
        dict: Summary statistics
    """

    def log(message):
        if log_callback:
            log_callback(message)
        else:
            print(message)

    def progress(value):
        if progress_callback:
            progress_callback(value)

    log(lf.header(
        "QUICK UPDATE MODE - PRODVIEW/SNOWFLAKE DAILY PRODUCTION RETRIEVE",
        Range=f"{start_month} to {end_month}",
    ))

    total_start = time.time()

    try:
        from production_update import (
            calculate_sequences, calculate_cumulatives,
            calculate_monthly_averages, add_on_production_year,
            fetch_well_mapping, apply_well_names, filter_to_first_production,
        )

        start_date = datetime.strptime(start_month, "%b %Y")
        end_date = datetime.strptime(end_month, "%b %Y")

        if start_date > end_date:
            error_msg = "Start month must be before end month"
            log(lf.error(error_msg))
            return {"error": error_msg}

        start_date_first = start_date.replace(day=1)
        _, end_date_last_date = _month_boundaries(end_date)
        start_date_first_date = start_date_first.date()

        # SQL Server
        log(lf.step("Connecting to SQL Server..."))
        conn = get_sql_conn()
        cursor = conn.cursor()
        cursor.fast_executemany = True
        log(lf.success("Database connected"))

        # Well mapping
        log(lf.step("Fetching well mapping from PCE_WM..."))
        mapping_df = _fetch_well_mapping(cursor)
        log(lf.detail(f"Loaded {lf.num(len(mapping_df))} wells"))

        months = list(_generate_months(start_date_first, end_date))
        total_months = len(months)
        log(lf.detail(f"Found {lf.num(total_months)} months to process"))

        if total_months == 0:
            return {'months_processed': 0, 'wells_updated': 0,
                    'cda_records': 0, 'production_records': 0, 'duration': 0}

        # ---- Single Snowflake pull for entire range ----
        log(lf.step(f"Pulling all Snowflake data ({start_date_first_date} to {end_date_last_date})..."))
        sf = SnowflakeConnector()
        try:
            sf_data = _pull_all_snowflake_data(sf, start_date_first_date, end_date_last_date, log)
        finally:
            sf.close()

        total_sf_rows = sum(len(df) for df in sf_data.values())
        log(lf.success(f"Retrieved {lf.num(total_sf_rows)} total rows from Snowflake"))

        months_processed = 0
        total_cda_records = 0

        for month_idx, month_dt in enumerate(months):
            month_start, month_end = _month_boundaries(month_dt)
            month_name = month_dt.strftime('%B %Y')
            date_range = pd.date_range(start=month_start, end=month_end, freq='D').date

            log(lf.ruler())
            log(lf.step(f"Processing {month_name} ({month_idx + 1}/{total_months})"))

            # Slice Snowflake data for this month
            month_sf = {}
            for name, full_df in sf_data.items():
                if full_df.empty:
                    month_sf[name] = full_df
                    continue
                col_map = {c.upper(): c for c in full_df.columns}
                date_col = col_map.get('PRODDATE', 'ProdDate')
                dates = pd.to_datetime(full_df[date_col]).dt.date
                month_sf[name] = full_df[(dates >= month_start) & (dates <= month_end)]

            # Delete existing data
            try:
                cursor.execute(
                    "DELETE FROM PCE_CDA WHERE ProdDate BETWEEN ? AND ?",
                    month_start, month_end,
                )
                deleted_cda = cursor.rowcount
                cursor.execute(
                    "DELETE FROM PCE_Production WHERE [Date] BETWEEN ? AND ?",
                    month_start, month_end,
                )
                deleted_prod = cursor.rowcount
                conn.commit()
                log(lf.detail(
                    f"Cleared {lf.num(deleted_cda)} CDA / {lf.num(deleted_prod)} Production records"
                ))
            except Exception as e:
                conn.rollback()
                log(lf.error(f"Error deleting existing data: {e}"))
                raise

            # Build spine (vectorized)
            spine_df = _build_spine(mapping_df, date_range)
            log(lf.detail(f"Spine: {lf.num(len(spine_df))} rows"))

            # Merge all data sources
            result_df = _merge_sf_data(spine_df, month_sf)

            # Condensate WH + GasWH replacement (vectorized)
            result_df['Condensate_WH_Production'] = (
                result_df['GasWH_Production'] * result_df['CGR_Ratio']
            )
            result_df, _repl = _apply_gaswh_replacement(result_df)
            log(lf.detail(f"Merged result: {lf.num(len(result_df))} rows"))

            # Insert into PCE_CDA (vectorized + executemany)
            rows = _df_to_insert_rows(result_df, _CDA_COLUMNS)
            batch_size = 5000
            rows_inserted = 0
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                cursor.executemany(_CDA_INSERT_SQL, batch)
                rows_inserted += len(batch)

            try:
                conn.commit()
                total_cda_records += rows_inserted
                log(lf.success(f"Inserted {lf.num(rows_inserted)} records into PCE_CDA"))
            except Exception as e:
                conn.rollback()
                log(lf.error(f"Error committing PCE_CDA for {month_name}: {e}"))
                raise

            months_processed += 1
            progress(int((month_idx + 1) / total_months * 80))

        # ---------------------------------------------------------------
        # Recalculate sequences/cumulatives/averages for affected wells
        # ---------------------------------------------------------------
        cursor.execute("""
            SELECT DISTINCT [Well Name]
            FROM PCE_CDA
            WHERE ProdDate BETWEEN ? AND ?
        """, start_date_first_date, end_date_last_date)
        affected_wells = [row[0] for row in cursor.fetchall()]
        total_wells = len(affected_wells)
        log(lf.step(f"Recalculating for {lf.num(total_wells)} affected wells..."))

        composite_map, fallback_map = fetch_well_mapping()

        for well_idx, well_name in enumerate(affected_wells):
            if (well_idx + 1) % 10 == 0 or (well_idx + 1) == total_wells:
                log(lf.detail(f"Well {well_idx + 1}/{total_wells}: {well_name}"))

            well_df = pd.read_sql("""
                SELECT
                    [Well Name] as Source_Well_Name,
                    ProdDate as [Date],
                    [GasWH_Production] as [Gas WH Production (10³m³)],
                    [Condensate_WH_Production] as [Condensate WH (m³/d)],
                    [Gas - S2 Production] as [Gas S2 Production (10³m³)],
                    [Gas - Sales Production] as [Gas Sales Production (10³m³)],
                    [Condensate - Sales Production] as [Condensate Sales (m³/d)],
                    [Gathered_Gas_Production] as [Gathered Gas (e³m³/d)],
                    [Gathered_Condensate_Production] as [Gathered Condensate (m³/d)],
                    [Sales CGR Ratio] as [Sales CGR (m³/e³m³)],
                    [CGR_Ratio] as [CGR (m³/e³m³)],
                    [WGR_Ratio] as [WGR (m³/e³m³)],
                    [ECF_Ratio] as [ECF],
                    [OnProdHours] as [Hours On],
                    [TubingPressure] as [Tubing Pressure (kPa)],
                    [CasingPressure] as [Casing Pressure (kPa)],
                    [ChokeSize] as [Choke Size],
                    [AllocatedWater_Rate] as [Alloc. Water Rate (m³)],
                    [NGL_Production] as [NGL (m³)],
                    [Formation Producer],
                    [Layer Producer],
                    [Fault Block],
                    [Pad Name],
                    [Lateral Length],
                    [Orient] as [Orientation]
                FROM PCE_CDA
                WHERE [Well Name] = ?
                ORDER BY ProdDate
            """, conn, params=(well_name,))

            if well_df.empty:
                continue

            well_df = apply_well_names(well_df, composite_map, fallback_map)
            if well_df.empty:
                continue
            well_df = filter_to_first_production(well_df)
            if well_df.empty:
                continue

            well_df = calculate_sequences(well_df)
            well_df = calculate_cumulatives(well_df)
            well_df = calculate_monthly_averages(well_df)
            well_df = add_on_production_year(well_df)

            well_name_prod = well_df.iloc[0]['Well Name']
            cursor.execute(
                "DELETE FROM PCE_Production WHERE [Well Name] = ?",
                well_name_prod,
            )

            # Ensure required columns exist before building tuples
            for col in _PROD_COLUMNS:
                if col not in well_df.columns:
                    well_df[col] = np.nan

            rows = _df_to_insert_rows(well_df, _PROD_COLUMNS)
            if rows:
                for i in range(0, len(rows), 5000):
                    cursor.executemany(_PROD_INSERT_SQL, rows[i:i + 5000])

            if (well_idx + 1) % 10 == 0 or (well_idx + 1) == total_wells:
                conn.commit()
                log(lf.success(
                    f"Updated PCE_Production for {well_name}: "
                    f"{lf.num(len(rows))} records"
                ))

            well_progress = int((well_idx + 1) / total_wells * 20)
            progress(80 + well_progress)

        conn.commit()

        log(lf.success(
            f"Sequence/cumulative/average recalculation complete for {lf.num(total_wells)} wells"
        ))

        cursor.execute("""
            SELECT COUNT(*)
            FROM PCE_Production
            WHERE [Date] BETWEEN ? AND ?
        """, start_date_first_date, end_date_last_date)
        total_production_records = cursor.fetchone()[0]

        conn.close()

        total_time = time.time() - total_start
        summary = {
            'months_processed': months_processed,
            'wells_updated': total_wells,
            'cda_records': total_cda_records,
            'production_records': total_production_records,
            'duration': total_time,
        }

        log(lf.summary("QUICK UPDATE COMPLETE", {
            "Months processed": months_processed,
            "Wells updated": total_wells,
            "PCE_CDA records": total_cda_records,
            "PCE_Production records": total_production_records,
            "Duration": lf.elapsed(total_time),
        }))
        return summary

    except Exception as e:
        error_msg = f"ERROR: {str(e)}"
        log(lf.error(str(e)))
        import traceback
        for line in traceback.format_exc().strip().split("\n"):
            log(lf.detail(line))
        return {"error": error_msg}
