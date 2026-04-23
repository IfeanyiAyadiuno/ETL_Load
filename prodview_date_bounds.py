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

# Rolling lookback for Quick Update (calendar months from effective end).
QUICK_UPDATE_LOOKBACK_MONTHS = 18


def _today() -> date:
    """Hook for tests via ``unittest.mock.patch``."""
    return date.today()


def prodview_effective_end_date() -> date:
    """Last calendar day to include in CDA / production (exclusive of very recent days)."""
    return _today() - timedelta(days=PRODVIEW_DATA_LAG_DAYS)


def quick_update_start_date(effective_end: date | None = None) -> date:
    """
    First calendar day for Quick Update: *effective_end* minus QUICK_UPDATE_LOOKBACK_MONTHS
    on a month-aligned offset (same as ``pd.DateOffset(months=...)``).
    """
    end = effective_end or prodview_effective_end_date()
    return (pd.Timestamp(end) - pd.DateOffset(months=QUICK_UPDATE_LOOKBACK_MONTHS)).date()


def quick_update_date_range() -> Tuple[date, date]:
    """Inclusive (start, end) for Quick Update Snowflake + CDA replace window."""
    end = prodview_effective_end_date()
    start = quick_update_start_date(end)
    return start, end
