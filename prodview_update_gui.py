"""
Prodview / Snowflake update logic.

Pulls daily production data from Snowflake (Prodview), reshapes it through
``PCE_WM`` mappings, and lands the result in ``PCE_CDA`` and ``PCE_Production``
on SQL Server. Used by the Prodview dialog (Snowflake → CDA + production
rebuild and Quick Update modes); runs inside ``QThread`` workers in the GUI.
"""

import time
from functools import partial
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

import log_format as lf
from db_connection import get_sql_conn
from pce_production_schema import (
    PCE_PRODUCTION_INSERT_COLUMNS,
    SQL_INSERT_BATCH_SIZE,
    batch_executemany,
    build_production_insert_sql,
    df_to_insert_rows,
)
from prodview_date_bounds import (
    full_rebuild_snowflake_range,
    prodview_effective_end_date,
    rolling_window_snowflake_range,
)
# pyodbc fast_executemany batch size for large CDA/Production inserts
# (re-exported from pce_production_schema for backward compatibility)


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
                VOLPRODGATHWATER AS Gathered_Water_Production,
                VOLNEWPRODALLOCNGL AS NGL_Production
            FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvunitallocmonthday
        WHERE DTTM >= %s AND DTTM <= %s
        """,
    ),
}

_SF_FIRST_PROD_GASWH_SQL = """
    SELECT
        IDRECPARENT AS GasIDREC,
        MIN(CAST(DTTM AS DATE)) AS FirstProdDate
    FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvUnitMeterOrificeEntry
    WHERE CAST(DTTM AS DATE) <= %s
      AND VOLENTERGAS > 2
    GROUP BY IDRECPARENT
"""

_SF_FIRST_PROD_GATHERED_SQL = """
    SELECT
        IDRECCOMP AS PressuresIDREC,
        MIN(CAST(DTTM AS DATE)) AS FirstProdDate
    FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvunitallocmonthday
    WHERE CAST(DTTM AS DATE) <= %s
      AND VOLPRODGATHGAS > 0
    GROUP BY IDRECCOMP
"""


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


def _cda_effective_production_mask_sql():
    """SQL predicate matching production_update.filter_to_first_production."""
    return """
        (ISNULL(GasWH_Production, 0) > 2)
        OR (
            ISNULL(GasWH_Production, 0) <= 2
            AND ISNULL(Gathered_Gas_Production, 0) > 0
        )
    """


def fetch_cda_first_production_by_well(conn=None) -> pd.Series:
    """
    Per-well first ProdDate with non-zero effective production (Gas WH or Gathered Gas).
    Same rules as filter_to_first_production on PCE_Production rebuild.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_sql_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT [Well Name], MIN(CAST(ProdDate AS DATE)) AS FirstProdDate
            FROM dbo.PCE_CDA
            WHERE {_cda_effective_production_mask_sql()}
            GROUP BY [Well Name]
            """
        )
        rows = cur.fetchall()
        if not rows:
            return pd.Series(dtype="object")
        out = {}
        for wn, d in rows:
            if wn is None or d is None:
                continue
            out[str(wn).strip()] = d.date() if hasattr(d, "date") else d
        return pd.Series(out)
    finally:
        if own_conn and conn is not None:
            conn.close()


def _first_production_by_well_from_snowflake_frames(
    mapping_df: pd.DataFrame,
    gaswh_first: pd.DataFrame,
    alloc_first: pd.DataFrame,
) -> pd.Series:
    """Map Snowflake per-ID first production dates to PCE_WM well names."""
    if mapping_df.empty:
        return pd.Series(dtype="object")

    wm = mapping_df.copy()
    wm["_gas_key"] = _normalize_join_key(wm["GasIDREC"])
    wm["_pres_key"] = _normalize_join_key(wm["PressuresIDREC"])
    gas_to_well = (
        wm.drop_duplicates("_gas_key", keep="first")
        .set_index("_gas_key")["Well Name"]
        .to_dict()
    )
    pres_to_well = (
        wm.drop_duplicates("_pres_key", keep="first")
        .set_index("_pres_key")["Well Name"]
        .to_dict()
    )

    well_dates: dict = {}

    def _apply(df, id_col, lookup):
        if df is None or df.empty:
            return
        col_map = {c.upper(): c for c in df.columns}
        id_key = col_map.get(id_col.upper())
        date_key = col_map.get("FIRSTPRODDATE")
        if not id_key or not date_key:
            return
        for _, row in df.iterrows():
            raw_id = row[id_key]
            if raw_id is None or pd.isna(raw_id):
                continue
            key = _normalize_join_key(pd.Series([raw_id])).iloc[0]
            well_name = lookup.get(key)
            if not well_name:
                continue
            wn = str(well_name).strip()
            d = row[date_key]
            if d is None or pd.isna(d):
                continue
            prod_date = d.date() if hasattr(d, "date") else d
            prev = well_dates.get(wn)
            if prev is None or prod_date < prev:
                well_dates[wn] = prod_date

    _apply(gaswh_first, "GasIDREC", gas_to_well)
    _apply(alloc_first, "PressuresIDREC", pres_to_well)
    return pd.Series(well_dates)


def fetch_first_production_by_well_from_snowflake(
    mapping_df: pd.DataFrame,
    end_date,
    *,
    log=None,
) -> pd.Series:
    """
    Per-well first production from Snowflake (Gas WH > 2 or gathered gas > 0).

    Used by Full Rebuild when PCE_CDA is empty or must not define the spine.
    """
    from snowflake_connector import SnowflakeConnector

    end_s = str(end_date)
    sf = SnowflakeConnector()
    try:
        gaswh_first = sf.query(_SF_FIRST_PROD_GASWH_SQL, (end_s,))
        alloc_first = sf.query(_SF_FIRST_PROD_GATHERED_SQL, (end_s,))
    finally:
        sf.close()

    if log:
        log(
            lf.detail(
                f"Snowflake first production: {lf.num(len(gaswh_first))} gas IDs, "
                f"{lf.num(len(alloc_first))} gathered IDs"
            )
        )

    return _first_production_by_well_from_snowflake_frames(
        mapping_df, gaswh_first, alloc_first
    )


def _build_spine_per_well_starts(mapping_df, first_prod_by_well, end_date, default_start):
    """Spine rows from each well's first production date through end_date."""
    end = pd.Timestamp(end_date)
    default = pd.Timestamp(default_start)
    if isinstance(first_prod_by_well, pd.Series):
        lookup = first_prod_by_well.to_dict()
    else:
        lookup = dict(first_prod_by_well or {})

    def _date_list(well_name):
        wn = str(well_name).strip()
        start = lookup.get(wn)
        start = pd.Timestamp(start) if start is not None else default
        if start > end:
            return []
        return list(pd.date_range(start, end, freq="D").date)

    m = mapping_df.copy()
    m["_dates"] = m["Well Name"].map(_date_list)
    m = m.loc[m["_dates"].map(len) > 0].explode("_dates", ignore_index=True)
    m = m.rename(columns={"_dates": "ProdDate"})
    return m


# Snowflake may return the SQL alias or the raw VOL* name (case-insensitive).
_SF_VALUE_ALIASES = {
    'Gathered_Water_Production': (
        'GATHERED_WATER_PRODUCTION',
        'GATHERED_WATER',
        'VOLPRODGATHWATER',
    ),
}


def _normalize_join_key(series):
    """Match WM PressuresIDREC to Snowflake IDRECCOMP (avoid '12345' vs '12345.0')."""
    num = pd.to_numeric(series, errors='coerce')
    out = series.astype(str).str.strip()
    valid = num.notna()
    if valid.any():
        out = out.copy()
        out.loc[valid] = num.loc[valid].astype('int64').astype(str)
    return out.str.replace(r'\.0$', '', regex=True)


def _resolve_sf_value_column(col_map, logical_name):
    for key in (logical_name.upper(), *_SF_VALUE_ALIASES.get(logical_name, ())):
        if key in col_map:
            return col_map[key]
    return None


def _prepare_sf_df(df, id_col, date_col, value_cols):
    """Clean and deduplicate a Snowflake result set."""
    if df.empty:
        return pd.DataFrame()

    col_map = {c.upper(): c for c in df.columns}
    result = pd.DataFrame()
    id_src = col_map.get(id_col.upper(), id_col)
    result['_join_key'] = _normalize_join_key(df[id_src])
    result['ProdDate'] = pd.to_datetime(df[col_map.get(date_col.upper(), date_col)]).dt.date

    for vc in value_cols:
        src = _resolve_sf_value_column(col_map, vc)
        result[vc] = (
            pd.to_numeric(df[src], errors='coerce') if src is not None else np.nan
        )

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
        (
            'alloc',
            'PressuresIDREC',
            [
                'Gathered_Gas_Production',
                'Gathered_Condensate_Production',
                'Gathered_Water_Production',
                'NGL_Production',
            ],
        ),
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
            result[join_col] = _normalize_join_key(result[join_col])
            result = result.merge(processed, on=[join_col, 'ProdDate'], how='left')

    return result


def _log_gathered_water_merge_stats(result_df, log):
    """Warn when Snowflake merge left gathered water empty but gas is populated."""
    if 'Gathered_Gas_Production' not in result_df.columns:
        return
    if 'Gathered_Water_Production' not in result_df.columns:
        return
    gas = pd.to_numeric(result_df['Gathered_Gas_Production'], errors='coerce').fillna(0)
    water = pd.to_numeric(result_df['Gathered_Water_Production'], errors='coerce').fillna(0)
    gas_nz = int((gas != 0).sum())
    water_nz = int((water != 0).sum())
    log(lf.detail(f"  Gathered water non-zero after merge: {lf.num(water_nz)} rows (gas: {lf.num(gas_nz)})"))
    if gas_nz > 0 and water_nz == 0:
        log(
            lf.warn(
                "Gathered gas is populated but gathered water is all zero/NULL after "
                "Snowflake merge. On the Windows PC, run: "
                "python scripts/diagnose_gathered_water_snowflake.py "
                "(checks VOLPRODGATHWATER in Snowflake). Then run Prodview "
                "Snowflake → CDA + production rebuild (not Full rebuild only)."
            )
        )


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
    return df_to_insert_rows(df, columns)


_PROD_INSERT_SQL = build_production_insert_sql()

_PROD_COLUMNS = list(PCE_PRODUCTION_INSERT_COLUMNS)

_CDA_INSERT_SQL = """
            INSERT INTO PCE_CDA (
                [GasIDREC], [PressuresIDREC], [Well Name], [ProdDate],
                [GasWH_Production], [Condensate_WH_Production],
                [WGR_Ratio], [CGR_Ratio], [ECF_Ratio],
                [OnProdHours], [TubingPressure], [CasingPressure], [ChokeSize],
                [Gathered_Gas_Production], [Gathered_Condensate_Production],
                [Gathered_Water_Production], [NGL_Production], [AllocatedWater_Rate],
                [Formation Producer], [Layer Producer], [Fault Block], [Pad Name],
                [Lateral Length], [Orient]
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

_CDA_COLUMNS = [
    'GasIDREC', 'PressuresIDREC', 'Well Name', 'ProdDate',
    'GasWH_Production', 'Condensate_WH_Production',
    'WGR_Ratio', 'CGR_Ratio', 'ECF_Ratio',
    'OnProdHours', 'TubingPressure', 'CasingPressure', 'ChokeSize',
    'Gathered_Gas_Production', 'Gathered_Condensate_Production',
    'Gathered_Water_Production', 'NGL_Production', 'AllocatedWater_Rate',
    'Formation Producer', 'Layer Producer', 'Fault Block', 'Pad Name',
    'Lateral Length', 'Orient',
]


def _month_boundaries(dt):
    """Return (first_day, last_day) as date objects for the month of *dt*."""
    first = dt.replace(day=1)
    if first.month == 12:
        last = datetime(first.year + 1, 1, 1) - timedelta(days=1)
    else:
        last = datetime(first.year, first.month + 1, 1) - timedelta(days=1)
    return first.date(), last.date()


def _batch_executemany(cursor, sql, rows, **kwargs):
    return batch_executemany(cursor, sql, rows, **kwargs)


def query_pce_cda_min_date(conn=None):
    """Return MIN(CAST(ProdDate AS DATE)) from PCE_CDA, or None if empty."""
    own_conn = conn is None
    if own_conn:
        conn = get_sql_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT MIN(CAST(ProdDate AS DATE)) FROM dbo.PCE_CDA")
        row = cur.fetchone()
        val = row[0] if row else None
        if val is None:
            return None
        if hasattr(val, "date"):
            return val.date()
        return val
    finally:
        if own_conn and conn is not None:
            conn.close()


def query_pce_cda_max_date(conn=None):
    """Return MAX(CAST(ProdDate AS DATE)) from PCE_CDA, or None if empty."""
    own_conn = conn is None
    if own_conn:
        conn = get_sql_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT MAX(CAST(ProdDate AS DATE)) FROM dbo.PCE_CDA")
        row = cur.fetchone()
        val = row[0] if row else None
        if val is None:
            return None
        if hasattr(val, "date"):
            return val.date()
        return val
    finally:
        if own_conn and conn is not None:
            conn.close()


def refresh_pce_cda_from_snowflake(
    start_date,
    end_date,
    *,
    log_callback=None,
    conn=None,
    progress_callback=None,
    well_first_production_start=None,
    default_spine_start=None,
    replace_entire_cda=False,
):
    """
    Pull Snowflake for [start_date, end_date] and replace matching PCE_CDA rows.

    When *well_first_production_start* is set (Full Rebuild), the spine uses each
    well's first production date through *end_date* instead of every calendar day
    from *start_date* for every well.

    When *replace_entire_cda* is True (Full Rebuild), deletes all PCE_CDA rows
    before insert instead of only the query date range.

    Does not modify PCE_Production. Returns number of CDA rows inserted.
    """
    log = partial(_emit_log, log_callback)

    def progress(val):
        if progress_callback:
            progress_callback(val)

    if start_date > end_date:
        log(lf.detail("Snowflake CDA refresh skipped (empty date range)."))
        return 0

    own_conn = conn is None
    if own_conn:
        conn = get_sql_conn()
    try:
        cursor = conn.cursor()
        cursor.fast_executemany = True

        mapping_df = _fetch_well_mapping(cursor)
        log(lf.detail(f"Loaded {lf.num(len(mapping_df))} wells for Snowflake spine"))

        log(lf.step(f"Pulling Snowflake data ({start_date} to {end_date})…"))
        from snowflake_connector import SnowflakeConnector

        sf = SnowflakeConnector()
        try:
            sf_data = _pull_all_snowflake_data(sf, start_date, end_date, log)
        finally:
            sf.close()
        log(lf.success(f"Retrieved {lf.num(sum(len(d) for d in sf_data.values()))} total rows"))
        progress(15)

        log(lf.step("Building spine and merging Snowflake data…"))
        if well_first_production_start is not None:
            spine_default = default_spine_start or start_date
            spine_df = _build_spine_per_well_starts(
                mapping_df,
                well_first_production_start,
                end_date,
                spine_default,
            )
            log(
                lf.detail(
                    f"Spine: per-well first production through {end_date} "
                    f"({lf.num(len(spine_df))} rows; Snowflake query {start_date}–{end_date})"
                )
            )
        else:
            full_range = pd.date_range(start=start_date, end=end_date, freq="D").date
            spine_df = _build_spine(mapping_df, full_range)
        result_df = _merge_sf_data(spine_df, sf_data)
        _log_gathered_water_merge_stats(result_df, log)
        result_df["Condensate_WH_Production"] = (
            result_df["GasWH_Production"] * result_df["CGR_Ratio"]
        )
        result_df, _repl = _apply_gaswh_replacement(result_df)
        before_cap = len(result_df)
        result_df = result_df.loc[
            pd.to_datetime(result_df["ProdDate"]).dt.normalize().dt.date <= end_date
        ].copy()
        if len(result_df) < before_cap:
            log(
                lf.detail(
                    f"Dropped {lf.num(before_cap - len(result_df))} row(s) after {end_date} "
                    "(Snowflake spine cap)"
                )
            )
        log(lf.detail(f"Merged: {lf.num(len(result_df))} rows"))
        progress(30)

        if replace_entire_cda:
            log(lf.step("Replacing all PCE_CDA rows (full rebuild)…"))
            log(lf.detail("Deleting all existing PCE_CDA rows…"))
            cursor.execute("DELETE FROM PCE_CDA")
        else:
            log(lf.step(f"Replacing PCE_CDA rows ({start_date} to {end_date})…"))
            log(lf.detail("Deleting existing PCE_CDA rows in range…"))
            cursor.execute(
                "DELETE FROM PCE_CDA WHERE ProdDate BETWEEN ? AND ?",
                start_date,
                end_date,
            )
        deleted = cursor.rowcount
        conn.commit()
        if deleted is not None and deleted >= 0:
            log(lf.detail(f"Deleted {lf.num(deleted)} PCE_CDA row(s)"))

        rows = _df_to_insert_rows(result_df, _CDA_COLUMNS)
        log(lf.detail(f"Inserting {lf.num(len(rows))} PCE_CDA row(s)…"))
        _batch_executemany(
            cursor,
            _CDA_INSERT_SQL,
            rows,
            log=log,
            label="PCE_CDA insert",
            progress=progress,
            progress_lo=30,
            progress_hi=44,
        )
        conn.commit()
        n = len(rows)
        log(lf.success(f"Inserted {lf.num(n)} records into PCE_CDA"))
        progress(45)
        return n
    finally:
        if own_conn and conn is not None:
            conn.close()


def refresh_rolling_window_cda(
    *,
    log_callback=None,
    conn=None,
    progress_callback=None,
    data_lag_days=None,
):
    """
    Replace PCE_CDA rows for the Prodview rolling Snowflake window (~18 months).

    Used by Quick Update. Returns (start_date, end_date, rows_inserted).
    """
    start_date, end_date = rolling_window_snowflake_range(data_lag_days)
    log = partial(_emit_log, log_callback)
    log(
        lf.step(
            f"Snowflake → PCE_CDA ({start_date} through {end_date}) "
            "— rolling window"
        )
    )
    n = refresh_pce_cda_from_snowflake(
        start_date,
        end_date,
        log_callback=log_callback,
        conn=conn,
        progress_callback=progress_callback,
    )
    return start_date, end_date, n


def refresh_full_rebuild_cda(
    *,
    log_callback=None,
    conn=None,
    progress_callback=None,
    data_lag_days=None,
):
    """
    Replace PCE_CDA from Snowflake per well from first production date (Snowflake)
    through effective end. Used by Full Rebuild before rebuilding all PCE_Production.
    Returns (snowflake_query_start, end_date, rows_inserted).
    """
    from prodview_date_bounds import full_rebuild_snowflake_range, quick_update_start_date

    log = partial(_emit_log, log_callback)
    end_date = prodview_effective_end_date(data_lag_days)
    default_start = quick_update_start_date(end_date)

    own_conn = conn is None
    if own_conn:
        conn = get_sql_conn()
    try:
        cursor = conn.cursor()
        mapping_df = _fetch_well_mapping(cursor)
        log(
            lf.detail(
                f"Resolving per-well first production from Snowflake "
                f"({lf.num(len(mapping_df))} wells)…"
            )
        )
        first_prod = fetch_first_production_by_well_from_snowflake(
            mapping_df, end_date, log=log
        )
        if first_prod.empty:
            log(
                lf.detail(
                    "No per-well first production in Snowflake; using rolling window "
                    f"start {default_start} for all wells."
                )
            )
            query_start = default_start
            spine_first_prod = None
        else:
            query_start = min(first_prod.values)
            spine_first_prod = first_prod
            log(
                lf.detail(
                    f"Per-well first production: {lf.num(len(first_prod))} wells; "
                    f"Snowflake query window {query_start} through {end_date}."
                )
            )
        query_start, end_date = full_rebuild_snowflake_range(query_start, end_date)

        log(
            lf.step(
                f"Snowflake → PCE_CDA ({query_start} through {end_date}) "
                "— per-well first production"
            )
        )
        n = refresh_pce_cda_from_snowflake(
            query_start,
            end_date,
            log_callback=log_callback,
            conn=conn,
            progress_callback=progress_callback,
            well_first_production_start=spine_first_prod,
            default_spine_start=default_start,
            replace_entire_cda=True,
        )
        return query_start, end_date, n
    finally:
        if own_conn and conn is not None:
            conn.close()


# ---------------------------------------------------------------------------
# run_quick_update
# ---------------------------------------------------------------------------


def run_quick_update(
    progress_callback=None,
    log_callback=None,
    data_lag_days=None,
    cancel_event=None,
):
    log = partial(_emit_log, log_callback)

    def progress(val):
        if progress_callback:
            progress_callback(val)

    def aborted():
        return cancel_event is not None and cancel_event.is_set()

    def cancelled_summary():
        return {
            "cancelled": True,
            "duration": time.time() - total_start,
            "date_range_start": str(start_first),
            "date_range_end": str(end_last),
        }

    start_first, end_last = rolling_window_snowflake_range(data_lag_days)
    if start_first > end_last:
        log(lf.error("Snowflake rolling-window date range is empty"))
        return {"error": "Snowflake rolling-window date range is empty"}

    log(lf.header(
        "SNOWFLAKE → CDA + PRODUCTION — PRODVIEW/SNOWFLAKE DAILY PRODUCTION RETRIEVE",
        Range=f"{start_first} through {end_last} (rolling 18 months)",
    ))
    total_start = time.time()
    timer = lf.StepTimer(log_fn=log)
    conn = None
    total_wells = 0
    total_prod = 0
    total_cda = 0

    try:
        if aborted():
            log(lf.warn("Cancelled before start."))
            return cancelled_summary()
        from production_update import (
            apply_gathered_prd_month_labels,
            apply_uwi_from_well_master,
            calculate_sequences,
            calculate_cumulatives,
            calculate_monthly_averages,
            add_on_production_year,
            fetch_cda_data,
            fetch_well_mapping,
            fetch_well_master_lookups,
            apply_well_names,
            filter_to_first_production,
            apply_pad_name_from_well_master,
            query_wells_with_cda_in_range,
            sync_production_wm_metadata_from_wm_sql,
            sync_wm_uwi_to_downstream_sql,
        )

        conn = get_sql_conn()
        cursor = conn.cursor()
        cursor.fast_executemany = True
        log(lf.success("Database connected"))

        log(lf.step(f"Trimming CDA and production after {end_last}…"))
        cursor.execute("DELETE FROM PCE_CDA WHERE ProdDate > ?", (end_last,))
        cursor.execute(
            """
            DELETE FROM PCE_Production
            WHERE [Date] > ?
              AND [Well Name] NOT LIKE '% - TC'
              AND [Well Name] NOT LIKE 'YE2%'
            """,
            (end_last,),
        )
        conn.commit()

        cursor.execute(
            """
            DELETE FROM PCE_Production
            WHERE [Date] BETWEEN ? AND ?
              AND [Well Name] NOT LIKE '% - TC'
              AND [Well Name] NOT LIKE 'YE2%'
            """,
            start_first,
            end_last,
        )
        conn.commit()
        timer.mark("Trim future rows + clear rolling window production")

        start_first, end_last, total_cda = refresh_rolling_window_cda(
            log_callback=log_callback,
            conn=conn,
            progress_callback=progress_callback,
            data_lag_days=data_lag_days,
        )
        progress(55)
        timer.mark("Snowflake → PCE_CDA refresh")

        if aborted():
            log(lf.warn("Cancelled after Snowflake CDA refresh."))
            return cancelled_summary()

        log(lf.step("Loading CDA for wells in rolling window..."))
        wm_lookups = fetch_well_master_lookups(conn)
        composite_map = wm_lookups["composite_map"]
        fallback_map = wm_lookups["fallback_map"]
        wells_in_window = query_wells_with_cda_in_range(cursor, start_first, end_last)
        all_cda = fetch_cda_data(
            well_names=wells_in_window,
            end_cap=end_last,
            conn=conn,
            log=log,
        )
        progress(60)
        timer.mark("Load PCE_CDA for rolling-window wells")

        if not all_cda.empty:
            all_cda = apply_well_names(all_cda, composite_map, fallback_map)
            all_cda = apply_pad_name_from_well_master(all_cda, wm_lookups["pad_lookup"])
            all_cda = apply_uwi_from_well_master(all_cda, wm_lookups["uwi_lookup"])
        if not all_cda.empty:
            all_cda = filter_to_first_production(all_cda)
        if not all_cda.empty:
            all_cda = calculate_sequences(all_cda)
            all_cda = calculate_cumulatives(all_cda)
            all_cda = calculate_monthly_averages(all_cda)
            all_cda = add_on_production_year(all_cda)
        progress(75)
        timer.mark("Production pandas calcs (seq / cum / avgs)")

        if aborted():
            log(lf.warn("Cancelled before PCE_Production rebuild."))
            return cancelled_summary()

        if not all_cda.empty:
            log(lf.step("Rebuilding PCE_Production..."))
            affected_well_names = all_cda['Well Name'].unique().tolist()

            del_batch = 200
            for i in range(0, len(affected_well_names), del_batch):
                batch = affected_well_names[i:i + del_batch]
                ph = ','.join(['?'] * len(batch))
                cursor.execute(f"DELETE FROM PCE_Production WHERE [Well Name] IN ({ph})", batch)
            conn.commit()

            for col in _PROD_COLUMNS:
                if col not in all_cda.columns:
                    all_cda[col] = np.nan
            all_cda = apply_gathered_prd_month_labels(all_cda)

            prod_rows = _df_to_insert_rows(all_cda, _PROD_COLUMNS)
            log(lf.detail(f"Inserting {lf.num(len(prod_rows))} PCE_Production row(s)…"))
            _batch_executemany(
                cursor,
                _PROD_INSERT_SQL,
                prod_rows,
                log=log,
                label="PCE_Production insert",
                progress=progress,
                progress_lo=76,
                progress_hi=88,
            )
            conn.commit()
            total_wells = len(affected_well_names)
            total_prod = len(prod_rows)
            log(lf.success(
                f"Inserted {lf.num(total_prod)} records for {lf.num(total_wells)} wells"
            ))
        else:
            total_wells = 0
            total_prod = 0
        timer.mark("PCE_Production delete + insert")

        if aborted():
            log(lf.warn("Cancelled after PCE_Production rebuild."))
            return cancelled_summary()

        from pce_rebuild_pipeline import run_post_production_rebuild_steps

        if not run_post_production_rebuild_steps(
            log,
            conn=conn,
            date_window=(start_first, end_last),
            cancel_event=cancel_event,
        ):
            return cancelled_summary()
        timer.mark("Post-production rebuild steps")

        progress(95)

        total_time = time.time() - total_start
        summary = {
            "date_range_start": str(start_first),
            "date_range_end": str(end_last),
            "wells_updated": total_wells,
            "cda_records": total_cda,
            "production_records": total_prod,
            "duration": total_time,
        }
        log(lf.summary("SNOWFLAKE + PRODUCTION COMPLETE", {
            "Completed": lf.timestamp(),
            "Date range": f"{start_first} → {end_last}",
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
    finally:
        if conn is not None:
            conn.close()
