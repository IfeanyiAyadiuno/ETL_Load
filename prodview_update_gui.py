import time
from functools import partial
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date

import log_format as lf
from db_connection import get_sql_conn
from snowflake_connector import SnowflakeConnector


def _emit_log(log_callback, msg):
    (log_callback or print)(msg)


# ---------------------------------------------------------------------------
# Snowflake query definitions  (cgr+water combined into one query)
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
    "cgr_water": (
        "PRESSURESIDREC",
        """
            SELECT
                IDRECCOMP AS PressuresIDREC,
                CAST(DTTM AS DATE) AS ProdDate,
                CASE
                    WHEN RATEGAS IS NULL OR RATEGAS = 0 THEN NULL
                    ELSE (RATEHCLIQ / RATEGAS)
            END AS CGR_Ratio,
            VOLWATER AS AllocatedWater_Rate
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
}


def _pull_all_snowflake_data(sf, start_date, end_date, log):
    """Pull all Snowflake datasets in one shot for the full date range."""
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
        SELECT GasIDREC, PressuresIDREC, [Well Name],
               [Formation Producer], [Layer Producer], [Fault Block],
               [Pad Name], [Lateral Length], [Orient]
        FROM PCE_WM
        WHERE GasIDREC IS NOT NULL
          AND ([Exception] IS NULL OR [Exception] = '' OR [Exception] = 'N')
    """)
    cols = [
        'GasIDREC', 'PressuresIDREC', 'Well Name',
        'Formation Producer', 'Layer Producer', 'Fault Block',
        'Pad Name', 'Lateral Length', 'Orient',
    ]
    return pd.DataFrame.from_records(cursor.fetchall(), columns=cols)


def _build_spine(mapping_df, date_range):
    """Vectorized wells x dates cross-join."""
    dates_df = pd.DataFrame({'ProdDate': date_range, '_key': 1})
    mapping_aug = mapping_df.assign(_key=1)
    return mapping_aug.merge(dates_df, on='_key').drop(columns='_key')


def _prepare_sf_df(df, id_col, date_col, value_cols):
    """Clean and deduplicate a Snowflake result set."""
    if df.empty:
        return pd.DataFrame()

    col_map = {c.upper(): c for c in df.columns}
    result = pd.DataFrame()
    result['_join_key'] = df[col_map.get(id_col.upper(), id_col)].astype(str).str.strip()
    result['ProdDate'] = pd.to_datetime(df[col_map.get(date_col.upper(), date_col)]).dt.date

    for vc in value_cols:
        src = col_map.get(vc.upper(), vc)
        result[vc] = pd.to_numeric(df[src], errors='coerce') if src in df.columns else np.nan

    return (
        result.sort_values(['_join_key', 'ProdDate'])
        .drop_duplicates(subset=['_join_key', 'ProdDate'], keep='last')
    )


def _merge_sf_data(spine_df, sf_data):
    """Merge all Snowflake datasets onto the spine in one pass."""
    result = spine_df

    merge_specs = [
        ('ecf',       'GasIDREC',       ['ECF_Ratio']),
        ('gaswh',     'GasIDREC',       ['GasWH_Production', 'OnProdHours']),
        ('cgr_water', 'PressuresIDREC', ['CGR_Ratio', 'AllocatedWater_Rate']),
        ('wgr',       'PressuresIDREC', ['WGR_Ratio']),
        ('pressures', 'PressuresIDREC', ['TubingPressure', 'CasingPressure', 'ChokeSize']),
        ('alloc',     'PressuresIDREC', ['Gathered_Gas_Production', 'Gathered_Condensate_Production', 'NGL_Production']),
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
            result = result.merge(processed, on=[join_col, 'ProdDate'], how='left')

    return result


def _apply_gaswh_replacement(df):
    """Vectorized GasWH replacement: use Gathered when GasWH is missing/tiny."""
    if 'GasWH_Production' not in df.columns or 'Gathered_Gas_Production' not in df.columns:
        return df, 0
    gas = df['GasWH_Production']
    gathered = df['Gathered_Gas_Production']
    mask = gathered.notna() & (gas.isna() | (gas == 0) | ((gas > 0) & (gas <= 2)))
    df.loc[mask, 'GasWH_Production'] = gathered[mask]
    df['Condensate_WH_Production'] = df['GasWH_Production'] * df['CGR_Ratio']
    return df, int(mask.sum())


def _df_to_insert_rows(df, columns):
    """Vectorized NaN->None via astype(object) + itertuples."""
    sub = df[columns].astype(object)
    sub[sub.isna()] = None
    return list(sub.itertuples(index=False, name=None))


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


def _batch_executemany(cursor, sql, rows, batch_size=5000):
    """Execute INSERT/UPDATE in batches."""
    for i in range(0, len(rows), batch_size):
        cursor.executemany(sql, rows[i:i + batch_size])


# ---------------------------------------------------------------------------
# populate_wells_cda  (Reusable: populate PCE_CDA for a given set of wells)
# ---------------------------------------------------------------------------

def populate_wells_cda(mapping_df, start_date, end_date,
                       progress_callback=None, log_callback=None):
    """
    Populate PCE_CDA for the wells described in *mapping_df*.

    mapping_df must have columns:
        GasIDREC, PressuresIDREC, Well Name,
        Formation Producer, Layer Producer, Fault Block,
        Pad Name, Lateral Length, Orient

    Only PCE_CDA is touched; PCE_Production is NOT modified.

    Returns dict with summary stats or {"error": ...}.
    """
    log = partial(_emit_log, log_callback)

    def progress(val):
        if progress_callback:
            progress_callback(val)

    total_start = time.time()
    well_names = mapping_df['Well Name'].unique().tolist()

    log(lf.header("POPULATE PCE_CDA",
                   Wells=len(well_names),
                   Range=f"{start_date} to {end_date}"))

    try:
        log(lf.step("Pulling Snowflake data..."))
        sf = SnowflakeConnector()
        try:
            sf_data = _pull_all_snowflake_data(sf, start_date, end_date, log)
        finally:
            sf.close()
        progress(30)

        log(lf.step("Building spine and merging data..."))
        full_range = pd.date_range(start=start_date, end=end_date, freq='D').date
        spine_df = _build_spine(mapping_df, full_range)
        log(lf.detail(f"Spine: {lf.num(len(spine_df))} rows"))

        result_df = _merge_sf_data(spine_df, sf_data)
        result_df['Condensate_WH_Production'] = (
            result_df['GasWH_Production'] * result_df['CGR_Ratio']
        )
        result_df, _repl = _apply_gaswh_replacement(result_df)
        log(lf.detail(f"Merged: {lf.num(len(result_df))} rows"))
        progress(60)

        log(lf.step("Writing to PCE_CDA..."))
        conn = get_sql_conn()
        cursor = conn.cursor()
        cursor.fast_executemany = True

        # Delete existing CDA rows for these specific wells
        del_batch = 200
        for i in range(0, len(well_names), del_batch):
            batch = well_names[i:i + del_batch]
            ph = ','.join(['?'] * len(batch))
            cursor.execute(
                f"DELETE FROM PCE_CDA WHERE [Well Name] IN ({ph})"
                f" AND ProdDate BETWEEN ? AND ?",
                batch + [start_date, end_date],
            )
            conn.commit()
        
        rows = _df_to_insert_rows(result_df, _CDA_COLUMNS)
        _batch_executemany(cursor, _CDA_INSERT_SQL, rows)
        conn.commit()
        conn.close()
        progress(100)
        
        total_time = time.time() - total_start
        summary = {
            'wells': len(well_names),
            'cda_records': len(rows),
            'duration': total_time,
        }
        log(lf.summary("CDA POPULATE COMPLETE", {
            "Wells": len(well_names),
            "PCE_CDA records": len(rows),
            "Duration": lf.elapsed(total_time),
        }))
        return summary
        
    except Exception as e:
        log(lf.error(str(e)))
        import traceback
        for line in traceback.format_exc().strip().split("\n"):
            log(lf.detail(line))
        return {"error": f"ERROR: {e}"}


# ---------------------------------------------------------------------------
# run_prodview_update  (Full Rebuild of CDA for selected range)
# ---------------------------------------------------------------------------

def run_prodview_update(start_month, end_month, progress_callback=None, log_callback=None):
    log = partial(_emit_log, log_callback)

    def progress(val):
        if progress_callback:
            progress_callback(val)

    log(lf.header("PRODVIEW/SNOWFLAKE DAILY PRODUCTION RETRIEVE",
                   Range=f"{start_month} to {end_month}"))
    total_start = time.time()
    
    try:
        start_date = datetime.strptime(start_month, "%b %Y")
        end_date = datetime.strptime(end_month, "%b %Y")
        if start_date > end_date:
            log(lf.error("Start month must be before end month"))
            return {"error": "Start month must be before end month"}

        conn = get_sql_conn()
        cursor = conn.cursor()
        cursor.fast_executemany = True
        log(lf.success("Database connected"))
        
        mapping_df = _fetch_well_mapping(cursor)
        log(lf.detail(f"Loaded {lf.num(len(mapping_df))} wells"))

        overall_start, _ = _month_boundaries(start_date)
        _, overall_end = _month_boundaries(end_date)

        # Single Snowflake pull
        log(lf.step(f"Pulling Snowflake data ({overall_start} to {overall_end})..."))
        sf = SnowflakeConnector()
        try:
            sf_data = _pull_all_snowflake_data(sf, overall_start, overall_end, log)
        finally:
            sf.close()

        # Single-pass: one spine, one merge, one DELETE, one INSERT
        log(lf.step("Building full-range spine and merging data..."))
        full_range = pd.date_range(start=overall_start, end=overall_end, freq='D').date
        spine_df = _build_spine(mapping_df, full_range)
        log(lf.detail(f"Spine: {lf.num(len(spine_df))} rows"))
        progress(20)

        result_df = _merge_sf_data(spine_df, sf_data)
        result_df['Condensate_WH_Production'] = result_df['GasWH_Production'] * result_df['CGR_Ratio']
        result_df, _repl = _apply_gaswh_replacement(result_df)
        log(lf.detail(f"Merged: {lf.num(len(result_df))} rows"))
        progress(40)

        # Delete existing data for full range
        cursor.execute("DELETE FROM PCE_CDA WHERE ProdDate BETWEEN ? AND ?",
                        overall_start, overall_end)
        cursor.execute("DELETE FROM PCE_Production WHERE [Date] BETWEEN ? AND ?",
                        overall_start, overall_end)
        conn.commit()

        # Insert CDA
        log(lf.step("Inserting into PCE_CDA..."))
        rows = _df_to_insert_rows(result_df, _CDA_COLUMNS)
        _batch_executemany(cursor, _CDA_INSERT_SQL, rows)
        conn.commit()
        total_cda = len(rows)
        log(lf.detail(f"Inserted {lf.num(total_cda)} CDA records"))
        progress(60)

        # Insert Production from CDA via server-side SELECT
        cursor.execute("""
            INSERT INTO PCE_Production (
                [Date], [Well Name], [Days Seq], [Day Seq UPRT],
                [Gas WH Production (10³m³)], [Condensate WH (m³/d)],
                [Gas S2 Production (10³m³)], [Gas Sales Production (10³m³)],
                [Condensate Sales (m³/d)], [Gathered Gas (e³m³/d)],
                [Gathered Condensate (m³/d)], [Sales CGR (m³/e³m³)],
                [CGR (m³/e³m³)], [WGR (m³/e³m³)], [ECF],
                [Hours On], [Tubing Pressure (kPa)], [Casing Pressure (kPa)],
                [Choke Size], [Alloc. Water Rate (m³)], [NGL (m³)],
                [Formation Producer], [Layer Producer], [Fault Block],
                [Pad Name], [Lateral Length], [Orientation]
            )
            SELECT
                c.ProdDate, c.[Well Name], 0, 0,
                c.GasWH_Production, c.Condensate_WH_Production,
                c.[Gas - S2 Production], c.[Gas - Sales Production],
                c.[Condensate - Sales Production], c.Gathered_Gas_Production,
                c.Gathered_Condensate_Production, c.[Sales CGR Ratio],
                c.CGR_Ratio, c.WGR_Ratio, c.ECF_Ratio,
                c.OnProdHours, c.TubingPressure, c.CasingPressure,
                c.ChokeSize, c.AllocatedWater_Rate, c.NGL_Production,
                c.[Formation Producer], c.[Layer Producer], c.[Fault Block],
                c.[Pad Name], c.[Lateral Length], c.Orient
            FROM PCE_CDA c
            WHERE c.ProdDate BETWEEN ? AND ?
        """, overall_start, overall_end)
        total_prod = cursor.rowcount
        conn.commit()
        progress(70)

        # SQL-based sequence recalculation (replaces Python per-well loop)
        log(lf.step("Recalculating sequences via SQL..."))
        cursor.execute("""
            WITH seq AS (
                SELECT [Well Name], [Date],
                       ROW_NUMBER() OVER (PARTITION BY [Well Name] ORDER BY [Date]) AS ds,
                       SUM(CASE WHEN [Gas WH Production (10³m³)] > 0 THEN 1 ELSE 0 END)
                           OVER (PARTITION BY [Well Name] ORDER BY [Date]
                                 ROWS UNBOUNDED PRECEDING) AS uprt_raw
                FROM PCE_Production
                WHERE [Date] BETWEEN ? AND ?
            )
            UPDATE p SET
                p.[Days Seq] = s.ds,
                p.[Day Seq UPRT] = CASE WHEN s.uprt_raw < 1 THEN 1 ELSE s.uprt_raw END
            FROM PCE_Production p
            INNER JOIN seq s ON p.[Well Name] = s.[Well Name] AND p.[Date] = s.[Date]
        """, overall_start, overall_end)
        conn.commit()
        log(lf.success(f"Sequences recalculated for date range"))
        progress(90)

        affected_wells_count = mapping_df['Well Name'].nunique()
        conn.close()

        total_time = time.time() - total_start
        summary = {
            'months_processed': (end_date.year - start_date.year) * 12
                                + end_date.month - start_date.month + 1,
            'wells_updated': affected_wells_count,
            'cda_records': total_cda,
            'production_records': total_prod,
            'duration': total_time,
        }
        log(lf.summary("COMPLETE", {
            "Wells": affected_wells_count,
            "PCE_CDA records": total_cda,
            "PCE_Production records": total_prod,
            "Duration": lf.elapsed(total_time),
        }))
        return summary

    except Exception as e:
        log(lf.error(str(e)))
        import traceback
        for line in traceback.format_exc().strip().split("\n"):
            log(lf.detail(line))
        return {"error": f"ERROR: {e}"}


# ---------------------------------------------------------------------------
# run_quick_update
# ---------------------------------------------------------------------------

_CDA_SELECT_SQL = """
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
        [Formation Producer], [Layer Producer], [Fault Block],
        [Pad Name], [Lateral Length],
                    [Orient] as [Orientation]
                FROM PCE_CDA
    ORDER BY [Well Name], ProdDate
"""


def run_quick_update(start_month, end_month, progress_callback=None, log_callback=None):
    log = partial(_emit_log, log_callback)

    def progress(val):
        if progress_callback:
            progress_callback(val)

    log(lf.header("QUICK UPDATE MODE - PRODVIEW/SNOWFLAKE DAILY PRODUCTION RETRIEVE",
                   Range=f"{start_month} to {end_month}"))
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
            log(lf.error("Start month must be before end month"))
            return {"error": "Start month must be before end month"}

        start_first = start_date.replace(day=1).date()
        _, end_last = _month_boundaries(end_date)

        conn = get_sql_conn()
        cursor = conn.cursor()
        cursor.fast_executemany = True
        log(lf.success("Database connected"))

        mapping_df = _fetch_well_mapping(cursor)
        log(lf.detail(f"Loaded {lf.num(len(mapping_df))} wells"))

        # Single Snowflake pull for entire range
        log(lf.step(f"Pulling Snowflake data ({start_first} to {end_last})..."))
        sf = SnowflakeConnector()
        try:
            sf_data = _pull_all_snowflake_data(sf, start_first, end_last, log)
        finally:
            sf.close()
        log(lf.success(f"Retrieved {lf.num(sum(len(d) for d in sf_data.values()))} total rows"))
        progress(20)

        # Single-pass: one spine, one merge, one DELETE, one INSERT
        log(lf.step("Building spine and merging data for full range..."))
        full_range = pd.date_range(start=start_first, end=end_last, freq='D').date
        spine_df = _build_spine(mapping_df, full_range)

        result_df = _merge_sf_data(spine_df, sf_data)
        result_df['Condensate_WH_Production'] = result_df['GasWH_Production'] * result_df['CGR_Ratio']
        result_df, _repl = _apply_gaswh_replacement(result_df)
        log(lf.detail(f"Merged: {lf.num(len(result_df))} rows"))
        progress(35)

        # Delete + Insert CDA for full range
        log(lf.step("Replacing PCE_CDA data..."))
        cursor.execute("DELETE FROM PCE_CDA WHERE ProdDate BETWEEN ? AND ?",
                        start_first, end_last)
        cursor.execute("DELETE FROM PCE_Production WHERE [Date] BETWEEN ? AND ?",
                        start_first, end_last)
        conn.commit()

        rows = _df_to_insert_rows(result_df, _CDA_COLUMNS)
        _batch_executemany(cursor, _CDA_INSERT_SQL, rows)
        conn.commit()
        total_cda = len(rows)
        log(lf.success(f"Inserted {lf.num(total_cda)} records into PCE_CDA"))
        progress(55)

        # ---------------------------------------------------------------
        # Bulk recalculation for ALL affected wells
        # ---------------------------------------------------------------
        log(lf.step("Loading all CDA data for affected wells..."))
        composite_map, fallback_map = fetch_well_mapping()

        # Single bulk query for ALL wells instead of N+1
        all_cda = pd.read_sql(_CDA_SELECT_SQL, conn)
        log(lf.detail(f"Loaded {lf.num(len(all_cda))} total CDA rows"))
        progress(60)

        if not all_cda.empty:
            all_cda = apply_well_names(all_cda, composite_map, fallback_map)
        if not all_cda.empty:
            all_cda = filter_to_first_production(all_cda)
        if not all_cda.empty:
            all_cda = calculate_sequences(all_cda)
            all_cda = calculate_cumulatives(all_cda)
            all_cda = calculate_monthly_averages(all_cda)
            all_cda = add_on_production_year(all_cda)
        progress(75)

        if not all_cda.empty:
            # Single bulk DELETE + INSERT for all wells at once
            log(lf.step("Rebuilding PCE_Production..."))
            affected_well_names = all_cda['Well Name'].unique().tolist()

            # Batched DELETE for all affected well names
            del_batch = 200
            for i in range(0, len(affected_well_names), del_batch):
                batch = affected_well_names[i:i + del_batch]
                ph = ','.join(['?'] * len(batch))
                cursor.execute(f"DELETE FROM PCE_Production WHERE [Well Name] IN ({ph})", batch)
            conn.commit()

            for col in _PROD_COLUMNS:
                if col not in all_cda.columns:
                    all_cda[col] = np.nan

            prod_rows = _df_to_insert_rows(all_cda, _PROD_COLUMNS)
            _batch_executemany(cursor, _PROD_INSERT_SQL, prod_rows)
            conn.commit()
            total_wells = len(affected_well_names)
            total_prod = len(prod_rows)
            log(lf.success(
                f"Inserted {lf.num(total_prod)} records for {lf.num(total_wells)} wells"
            ))
        else:
            total_wells = 0
            total_prod = 0

        progress(95)
        conn.close()
        
        total_time = time.time() - total_start
        months_count = (end_date.year - start_date.year) * 12 + end_date.month - start_date.month + 1
        summary = {
            'months_processed': months_count,
            'wells_updated': total_wells,
            'cda_records': total_cda,
            'production_records': total_prod,
            'duration': total_time,
        }
        log(lf.summary("QUICK UPDATE COMPLETE", {
            "Completed": lf.timestamp(),
            "Months processed": months_count,
            "Wells updated": total_wells,
            "PCE_CDA records": total_cda,
            "PCE_Production records": total_prod,
            "Duration": lf.elapsed(total_time),
        }))
        return summary
        
    except Exception as e:
        log(lf.error(str(e)))
        import traceback
        for line in traceback.format_exc().strip().split("\n"):
            log(lf.detail(line))
        return {"error": f"ERROR: {e}"}
