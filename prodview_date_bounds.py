"""
Shared calendar bounds for Prodview / Snowflake CDA and production rebuilds.

End date is always ``today - PRODVIEW_DATA_LAG_DAYS`` so we do not load or retain
daily rows for dates that have not occurred yet (avoids end-of-month padding).
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Tuple

import pandas as pd

# Days back from local calendar ``date.today()`` for the last included production day.
PRODVIEW_DATA_LAG_DAYS = 2

# Rolling lookback for Prodview Snowflake mode (calendar months from effective end).
QUICK_UPDATE_LOOKBACK_MONTHS = 18


def _today() -> date:
    """Hook for tests via ``unittest.mock.patch``."""
    return date.today()


def prodview_effective_end_date() -> date:
    """Last calendar day to include in CDA / production (exclusive of very recent days)."""
    return _today() - timedelta(days=PRODVIEW_DATA_LAG_DAYS)


def quick_update_start_date(effective_end: date | None = None) -> date:
    """
    First calendar day for Prodview Snowflake rolling window: *effective_end* minus QUICK_UPDATE_LOOKBACK_MONTHS
    on a month-aligned offset (same as ``pd.DateOffset(months=...)``).
    """
    end = effective_end or prodview_effective_end_date()
    return (pd.Timestamp(end) - pd.DateOffset(months=QUICK_UPDATE_LOOKBACK_MONTHS)).date()


def quick_update_date_range() -> Tuple[date, date]:
    """Inclusive (start, end) for Snowflake + CDA replace window (`run_quick_update`)."""
    end = prodview_effective_end_date()
    start = quick_update_start_date(end)
    return start, end


def rolling_window_snowflake_range() -> Tuple[date, date]:
    """Inclusive range for Prodview Snowflake → PCE_CDA (Quick Update only, ~18 months)."""
    return quick_update_date_range()


def full_rebuild_snowflake_range(
    query_start: date | None = None,
    effective_end: date | None = None,
) -> Tuple[date, date]:
    """
    Inclusive Snowflake API query window for Full Rebuild.

    *query_start* should be the minimum per-well first production date (or rolling
    fallback). The spine itself is built per well from each well's first production.
    """
    end = effective_end or prodview_effective_end_date()
    start = query_start if query_start is not None else quick_update_start_date(end)
    if start > end:
        start = end
    return start, end


def merge_inclusive_date_ranges(
    a: Tuple[date, date] | None,
    b: Tuple[date, date] | None,
) -> Tuple[date, date] | None:
    """Widest inclusive span covering both ranges (or the one that is set)."""
    if a is None:
        return b
    if b is None:
        return a
    return min(a[0], b[0]), max(a[1], b[1])


def query_cda_gathered_nonzero_counts(
    start: date,
    end: date,
    *,
    conn=None,
) -> Tuple[int, int]:
    """
    Return (gathered_gas_nonzero_rows, gathered_water_nonzero_rows) in PCE_CDA
    for ProdDate in [start, end]. Used to detect post-migration backfill need.
    """
    from db_connection import get_sql_conn

    own_conn = conn is None
    if own_conn:
        conn = get_sql_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                SUM(CASE WHEN Gathered_Gas_Production IS NOT NULL
                          AND Gathered_Gas_Production <> 0 THEN 1 ELSE 0 END),
                SUM(CASE WHEN Gathered_Water_Production IS NOT NULL
                          AND Gathered_Water_Production <> 0 THEN 1 ELSE 0 END)
            FROM dbo.PCE_CDA
            WHERE ProdDate BETWEEN ? AND ?
            """,
            start,
            end,
        )
        row = cur.fetchone() or (0, 0)
        return int(row[0] or 0), int(row[1] or 0)
    finally:
        if own_conn and conn is not None:
            conn.close()


def gathered_water_backfill_range(
    gas_nonzero_rows: int,
    water_nonzero_rows: int,
    effective_end: date | None = None,
) -> Tuple[date, date] | None:
    """
    Re-pull Snowflake for the rolling window when CDA has gathered gas but no
    gathered water (typical after adding Gathered_Water_Production to an existing DB).
    """
    if gas_nonzero_rows <= 0 or water_nonzero_rows > 0:
        return None
    end = effective_end or prodview_effective_end_date()
    return quick_update_start_date(end), end


def snowflake_cda_gap_range(
    cda_max: date | None,
    effective_end: date | None = None,
) -> Tuple[date, date] | None:
    """
    Inclusive Snowflake refresh range to bring PCE_CDA up to *effective_end*.

    Returns None when CDA is already current through *effective_end*.
    When *cda_max* is unknown (empty table), uses the full rolling window start.
    """
    end = effective_end or prodview_effective_end_date()
    if cda_max is None:
        return quick_update_start_date(end), end
    if cda_max >= end:
        return None
    return cda_max + timedelta(days=1), end
