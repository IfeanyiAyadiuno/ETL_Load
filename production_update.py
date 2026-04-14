import time

import pandas as pd
import numpy as np
from datetime import datetime
import warnings

import log_format as lf
from db_connection import get_sql_conn, SQL_DATABASE, SQL_SERVER

warnings.filterwarnings('ignore', category=FutureWarning)


def _refresh_cda_sales_from_allocation_factors(log=print, cancel_event=None):
    """
    Repaint Gas S2, gas sales, condensate sales, and Sales CGR on PCE_CDA using
    Allocation_Factors (same logic as PA + Public Sales ratio passes).

    Runs every distinct month in Allocation_Factors. Returns False if cancelled
    mid-loop; True otherwise (including when AF is empty).
    """
    from sales_allocation_updates import (
        apply_full_sales_ratios_for_month,
        apply_valnav_allocation_to_cda_and_production,
    )

    def aborted():
        return cancel_event is not None and cancel_event.is_set()

    with get_sql_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT DISTINCT MonthStartDate FROM Allocation_Factors ORDER BY MonthStartDate"
        )
        months = [row[0] for row in cursor.fetchall()]

    if not months:
        log(
            lf.warn(
                "No Allocation_Factors rows; PCE_CDA Gas S2 / sales columns are unchanged."
            )
        )
        return True

    log(
        lf.step(
            f"Refreshing PCE_CDA from Allocation_Factors ({lf.num(len(months))} months: "
            "Gas S2, gas sales, condensate sales, Sales CGR)…"
        )
    )

    n = len(months)
    with get_sql_conn() as conn:
        for i, month_start in enumerate(months):
            if aborted():
                log(lf.warn("Cancelled during PCE_CDA sales refresh."))
                return False
            apply_valnav_allocation_to_cda_and_production(conn, month_start, log=log)
            apply_full_sales_ratios_for_month(conn, month_start, log=log)
            if n <= 24 or (i + 1) % 12 == 0 or (i + 1) == n:
                log(lf.detail(f"CDA sales refresh progress: {i + 1}/{n} months"))

    return True


def clear_pce_production():
    """Clear all data from PCE_Production table"""
    with get_sql_conn() as conn:
        cursor = conn.cursor()
        # Delete all rows
        cursor.execute("DELETE FROM PCE_Production")
        deleted = cursor.rowcount
        # Reset identity/ID column so new rows start from 1 again (if table has an IDENTITY)
        try:
            cursor.execute("DBCC CHECKIDENT('PCE_Production', RESEED, 0)")
        except Exception:
            # If the table has no IDENTITY or permissions are limited, ignore the error
            pass
        conn.commit()
        print(lf.success(f"Cleared PCE_Production: {lf.num(deleted)} records deleted (identity reseeded where applicable)"))
        return deleted

def fetch_cda_data():
    """Fetch all daily production data from PCE_CDA ordered by well and date"""
    query = """
    SELECT 
        [Well Name] as [Source_Well_Name],
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
    ORDER BY [Well Name], ProdDate
    """
    
    with get_sql_conn() as conn:
        df = pd.read_sql(query, conn)
    
    print(lf.detail(f"Loaded {lf.num(len(df))} rows from PCE_CDA"))
    return df

def fetch_well_mapping():
    """Fetch well name mappings from PCE_WM (Composite Name and Well Name)"""
    query = """
    SELECT 
        [Well Name] as SourceWell,
        [Composite Name],
        [Well Name] as FallbackWell
    FROM PCE_WM
    WHERE [Well Name] IS NOT NULL
      AND ([Exception] IS NULL OR [Exception] = '' OR [Exception] = 'N')
    """
    
    with get_sql_conn() as conn:
        df = pd.read_sql(query, conn)
    
    df['SourceWell'] = df['SourceWell'].astype(str).str.strip()
    df['FallbackWell'] = df['FallbackWell'].astype(str).str.strip()

    valid_composite = (
        df['Composite Name'].notna()
        & df['Composite Name'].astype(str).str.strip().ne('')
    )
    composite_stripped = df['Composite Name'].astype(str).str.strip()
    composite_map = dict(zip(
        df.loc[valid_composite, 'SourceWell'],
        composite_stripped[valid_composite],
    ))
    fallback_map = dict(zip(df['SourceWell'], df['FallbackWell']))
    
    print(lf.detail(f"Loaded {lf.num(len(fallback_map))} well name mappings"))
    
    return composite_map, fallback_map

def apply_well_names(df, composite_map, fallback_map):
    """
    Apply well name mapping: use Composite Name if available, otherwise use Well Name.
    Vectorized via .map() instead of per-row .apply().
    """
    original_count = len(df)
    stripped = df['Source_Well_Name'].astype(str).str.strip()

    mapped = stripped.map(composite_map)
    still_missing = mapped.isna()
    mapped[still_missing] = stripped[still_missing].map(fallback_map)

    df['Well Name'] = mapped

    unmapped_count = df['Well Name'].isna().sum()
    if unmapped_count > 0:
        print(lf.warn(f"{lf.num(unmapped_count)} rows ({unmapped_count/original_count*100:.1f}%) have no well name mapping"))
        unmapped_sources = stripped[df['Well Name'].isna()].unique()
        if len(unmapped_sources) > 0:
            print(lf.detail(f"Unmapped source wells: {', '.join(list(unmapped_sources)[:10])}"))
            if len(unmapped_sources) > 10:
                print(lf.detail(f"... and {lf.num(len(unmapped_sources) - 10)} more"))
        df = df.dropna(subset=['Well Name'])
        print(lf.detail(f"Remaining rows: {lf.num(len(df))}"))

    df = df.drop(columns=['Source_Well_Name'])
    print(lf.success("Well name mapping complete"))
    return df

def filter_to_first_production(df):
    """
    For each well, keep only rows from the first non-zero production onward.
    Vectorized: replaces per-well loops + per-row .loc writes with boolean masks.
    """
    original_count = len(df)
    df = df.sort_values(['Well Name', 'Date']).reset_index(drop=True)

    gas_wh = df['Gas WH Production (10³m³)'].fillna(0)
    gathered = df['Gathered Gas (e³m³/d)'].fillna(0)

    # VBA logic: if Gas WH <= 2, use Gathered Gas instead
    replace_mask = (gas_wh <= 2)
    effective = gas_wh.copy()
    effective[replace_mask] = gathered[replace_mask]

    # Apply replacement to the actual column as well
    df.loc[replace_mask, 'Gas WH Production (10³m³)'] = gathered[replace_mask]

    # Find first production date per well (first date where effective > 0)
    has_production = effective > 0
    first_prod_dates = (
        df.loc[has_production]
        .groupby('Well Name')['Date']
        .min()
    )

    if first_prod_dates.empty:
        print(lf.warn("No wells with production data found!"))
        return pd.DataFrame()

    df['_first_prod'] = df['Well Name'].map(first_prod_dates)
    keep_mask = df['_first_prod'].notna() & (df['Date'] >= df['_first_prod'])
    df_filtered = df.loc[keep_mask].drop(columns=['_first_prod']).reset_index(drop=True)

    wells_with_data = first_prod_dates.shape[0]
    rows_removed = original_count - len(df_filtered)
    print(lf.detail(
        f"Filtered to first production: {lf.num(wells_with_data)} wells, "
        f"{lf.num(len(df_filtered))} rows ({lf.num(rows_removed)} removed)"
    ))
    return df_filtered

def calculate_sequences(df):
    """
    Calculate Days Seq and Day Seq UPRT for each well.
    Vectorized via groupby().cumcount() and cumsum().
    """
    df = df.sort_values(['Well Name', 'Date']).reset_index(drop=True)

    # Days Seq: simple per-well counter starting at 1
    df['Days Seq'] = df.groupby('Well Name').cumcount() + 1

    # Day Seq UPRT: cumulative count of production days (>0), floored at 1
    gas_positive = (
        pd.to_numeric(df['Gas WH Production (10³m³)'], errors='coerce')
        .fillna(0)
        .gt(0)
        .astype(int)
    )
    df['Day Seq UPRT'] = gas_positive.groupby(df['Well Name']).cumsum().clip(lower=1)

    total_wells = df['Well Name'].nunique()
    print(lf.detail(f"Sequences calculated for {lf.num(total_wells)} wells"))
    return df

def calculate_cumulatives(df):
    """
    Calculate cumulative totals for each well.

    For each well, cumulative columns are simple running totals
    over time that NEVER reset within a well and always reflect
    the sum of the daily values up to and including that date.
    """
    # Ensure data is sorted by well and date so running totals are stable
    df = df.sort_values(['Well Name', 'Date']).reset_index(drop=True)

    # List of source columns and their cumulative target columns
    cumulatives = [
        ('Gas WH Production (10³m³)', 'Gas WH Cumulative Production (10³m³)'),
        ('Gas S2 Production (10³m³)', 'Gas S2 Cumulative Production (10³m³)'),
        ('Gas Sales Production (10³m³)', 'Gas Sales Cumulative Production (10³m³)'),
        ('Condensate Sales (m³/d)', 'Condensate Sales Cumulative Production (m³)'),
        ('Condensate WH (m³/d)', 'Condensate WH Cumulative Production (m³)'),
        ('Gathered Gas (e³m³/d)', 'Gas Gathered Cumulative (e³m³)'),
        ('Gathered Condensate (m³/d)', 'Condensate Gathered Cumulative (m³)'),
    ]

    # For each pair, compute per‑well running total that never resets
    for source_col, target_col in cumulatives:
        # Convert to numeric and fill NaNs with zero for clean sums
        values = pd.to_numeric(df[source_col], errors='coerce').fillna(0.0)
        df[target_col] = values.groupby(df['Well Name']).cumsum()

    return df

def calculate_monthly_averages(df):
    """
    Calculate monthly averages per well.
    Vectorized via groupby().transform('mean') -- replaces triple-nested loop.
    """
    print(lf.step("Calculating monthly averages..."))
    df['_YM'] = pd.to_datetime(df['Date']).dt.to_period('M')

    monthly_avgs = [
        ('Gas WH Production (10³m³)', 'Gas WH Avg (10³m³)'),
        ('Gas S2 Production (10³m³)', 'Gas S2 Avg (10³m³)'),
        ('Gathered Gas (e³m³/d)', 'Gas Gathered Avg (e³m³/d)'),
        ('Gathered Condensate (m³/d)', 'Condensate Gathered Avg (m³/d)'),
    ]

    group_keys = [df['Well Name'], df['_YM']]
    for source_col, avg_col in monthly_avgs:
        numeric = pd.to_numeric(df[source_col], errors='coerce').fillna(0)
        df[avg_col] = numeric.groupby(group_keys).transform('mean')

    df = df.drop(columns=['_YM'])
    print(lf.detail(f"Monthly averages calculated for {lf.num(df['Well Name'].nunique())} wells (vectorized)"))
    return df

def add_on_production_year(df):
    """
    Add On Production Year column (year of first production date per well).
    Vectorized via groupby().transform('min').
    """
    first_dates = pd.to_datetime(
        df.groupby('Well Name')['Date'].transform('min')
    )
    df['On Production Year'] = first_dates.dt.year
    return df

_INSERT_COLS = [
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

def insert_pce_production(df):
    """
    Insert dataframe into PCE_Production table.
    Vectorized NaN->None conversion + executemany batches.
    """
    if df.empty:
        print(lf.detail("No rows to insert"))
        return 0

    insert_sql = """
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

    # Ensure int columns are cast properly before NaN->None conversion
    df = df.copy()
    df['Days Seq'] = pd.to_numeric(df['Days Seq'], errors='coerce').fillna(0).astype(int)
    df['Day Seq UPRT'] = pd.to_numeric(df['Day Seq UPRT'], errors='coerce').fillna(0).astype(int)
    df['On Production Year'] = pd.to_numeric(df['On Production Year'], errors='coerce')

    sub = df[_INSERT_COLS].astype(object)
    sub[sub.isna()] = None
    rows_to_insert = list(sub.itertuples(index=False, name=None))

    batch_size = 5000
    total_inserted = 0
    duplicate_skipped = 0

    with get_sql_conn() as conn:
        cursor = conn.cursor()
        cursor.fast_executemany = True

        total_rows = len(rows_to_insert)

        for i in range(0, total_rows, batch_size):
            batch = rows_to_insert[i:i + batch_size]
            try:
                cursor.executemany(insert_sql, batch)
                total_inserted += len(batch)
            except Exception as batch_e:
                print(lf.warn(f"Batch at row {i} failed ({batch_e}); row-by-row fallback."))
                for j, row in enumerate(batch):
                    try:
                        cursor.execute(insert_sql, row)
                        total_inserted += 1
                    except Exception as row_e:
                        if "Violation of UNIQUE KEY" in str(row_e):
                            duplicate_skipped += 1
                        else:
                            print(lf.error(f"Error inserting row {i+j}: {row_e}"))

            conn.commit()

            if (i + len(batch)) % 50000 == 0 or (i + len(batch)) == total_rows:
                print(lf.detail(f"Insert progress: {lf.num(i + len(batch))}/{lf.num(total_rows)} rows"))

    print(lf.success(f"Inserted {lf.num(total_inserted)} rows into PCE_Production"))
    if duplicate_skipped > 0:
        print(lf.warn(f"Skipped {lf.num(duplicate_skipped)} duplicate rows"))

    return total_inserted

def main(cancel_event=None):
    """
    Rebuild PCE_Production from PCE_CDA.

    First repaints Gas S2, gas sales, condensate sales, and Sales CGR on PCE_CDA
    from Allocation_Factors (when AF rows exist), then clears and repopulates
    PCE_Production from CDA.

    Optional cancel_event (threading.Event): checked between major steps for
    best-effort cooperative cancel.
    """
    t0 = time.time()

    def aborted():
        return cancel_event is not None and cancel_event.is_set()

    def _duration():
        return time.time() - t0

    base_meta = {"mode": "full_rebuild", "duration_seconds": _duration()}

    print(lf.header("PCE_Production population", Started=lf.timestamp()))
    if aborted():
        print(lf.warn("Cancelled before start."))
        return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

    # Step 1: Ensure CDA sales / S2 columns match Allocation_Factors before copying to Production
    if not _refresh_cda_sales_from_allocation_factors(log=print, cancel_event=cancel_event):
        return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

    if aborted():
        print(lf.warn("Cancelled after PCE_CDA sales refresh."))
        return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

    # Step 2: Clear existing data
    clear_pce_production()
    if aborted():
        print(lf.warn("Cancelled after clearing PCE_Production."))
        return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

    # Step 3: Fetch well name mappings
    composite_map, fallback_map = fetch_well_mapping()
    if aborted():
        print(lf.warn("Cancelled after loading well mappings."))
        return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

    # Step 4: Fetch CDA data
    df = fetch_cda_data()

    if df.empty:
        print(lf.warn("No data to process. Exiting."))
        return {
            **base_meta,
            "skipped": True,
            "reason": "No rows in PCE_CDA",
            "duration_seconds": _duration(),
        }

    # Step 5: Apply well name mappings (composite name with fallback to well name)
    df = apply_well_names(df, composite_map, fallback_map)

    if df.empty:
        print(lf.warn("No data after well name mapping. Exiting."))
        return {
            **base_meta,
            "skipped": True,
            "reason": "No data after well name mapping",
            "duration_seconds": _duration(),
        }

    # Step 6: Filter to first production date for each well
    df = filter_to_first_production(df)

    if df.empty:
        print(lf.warn("No data after filtering. Exiting."))
        return {
            **base_meta,
            "skipped": True,
            "reason": "No data after first-production filter",
            "duration_seconds": _duration(),
        }

    if aborted():
        print(lf.warn("Cancelled before sequence calculations."))
        return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

    # Step 7: Calculate sequences with corrected Day Seq UPRT logic
    df = calculate_sequences(df)

    # Step 8: Calculate cumulatives
    df = calculate_cumulatives(df)

    # Step 9: Calculate monthly averages
    df = calculate_monthly_averages(df)

    # Step 10: Add On Production Year
    df = add_on_production_year(df)

    if aborted():
        print(lf.warn("Cancelled before inserting into PCE_Production."))
        return {**base_meta, "cancelled": True, "duration_seconds": _duration()}

    # Step 11: Insert into PCE_Production
    rows_inserted = insert_pce_production(df)

    wells_processed = len(df["Well Name"].unique())
    total_records = len(df)

    # Step 12: Final summary
    print(lf.summary("Complete", {
        "Completed": lf.timestamp(),
        "Wells processed": wells_processed,
        "Total records": total_records,
        "Records inserted": rows_inserted,
        "Destination": f"{SQL_SERVER}.{SQL_DATABASE}.PCE_Production",
    }))

    return {
        **base_meta,
        "wells_processed": wells_processed,
        "total_records": total_records,
        "records_inserted": rows_inserted,
        "duration_seconds": _duration(),
    }


if __name__ == "__main__":
    out = main()
    if out:
        print(lf.detail(f"Exit summary: {out}"))