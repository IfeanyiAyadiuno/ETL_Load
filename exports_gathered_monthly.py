"""
Monthly gathered production export: active PCE_WM wells × calendar months.

Sums daily gathered rate columns from PCE_Production per month; optional imperial conversion.
"""

from __future__ import annotations

from calendar import monthrange
from datetime import date
from typing import Callable, List, Optional, Tuple

import pandas as pd

from pce_frcst_prd_rebuild import E3M3_TO_MCF, M3_TO_BBL_COND, M3_TO_BBL_WATER

UNITS_METRIC = "metric"
UNITS_IMPERIAL = "imperial"

_MONTH_ABBREV = {
    1: "Jan",
    2: "Feb",
    3: "Mar",
    4: "Apr",
    5: "May",
    6: "Jun",
    7: "Jul",
    8: "Aug",
    9: "Sep",
    10: "Oct",
    11: "Nov",
    12: "Dec",
}
_ABBREV_TO_MONTH = {v: k for k, v in _MONTH_ABBREV.items()}

_COL_GAS = "SumGas"
_COL_COND = "SumCond"
_COL_WATER = "SumWater"

_METRIC_HEADERS = {
    _COL_GAS: "Gathered Gas (e³m³)",
    _COL_COND: "Gathered Condensate (m³)",
    _COL_WATER: "Gathered Water (m³)",
}
_IMPERIAL_HEADERS = {
    _COL_GAS: "Gathered Gas (Mcf)",
    _COL_COND: "Gathered Condensate (bbl)",
    _COL_WATER: "Gathered Water (bbl)",
}


class ProductionDataEmptyError(Exception):
    """Raised when PCE_Production has no rows to bound month pickers."""


def parse_month_label(label: str) -> Tuple[int, int]:
    """Parse ``Jan 2024`` into ``(year, month)``."""
    parts = label.strip().split()
    if len(parts) != 2:
        raise ValueError(f"Invalid month label: {label!r}")
    abbrev, year_s = parts[0], parts[1]
    month = _ABBREV_TO_MONTH.get(abbrev)
    if month is None:
        raise ValueError(f"Invalid month abbreviation: {abbrev!r}")
    year = int(year_s)
    if year < 1900 or year > 2100:
        raise ValueError(f"Invalid year in month label: {label!r}")
    return year, month


def month_label(year: int, month: int) -> str:
    return f"{_MONTH_ABBREV[month]} {year}"


def first_day_of_month(year: int, month: int) -> date:
    return date(year, month, 1)


def month_labels_between(min_month: date, max_month: date) -> List[str]:
    """Inclusive list of ``Jan 2024`` labels from first day of min through max month."""
    if max_month < min_month:
        return []
    labels: List[str] = []
    y, m = min_month.year, min_month.month
    end_y, end_m = max_month.year, max_month.month
    while (y, m) <= (end_y, end_m):
        labels.append(month_label(y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return labels


def month_ordinal(year: int, month: int) -> int:
    return year * 12 + month


def validate_month_range(from_label: str, to_label: str) -> None:
    fy, fm = parse_month_label(from_label)
    ty, tm = parse_month_label(to_label)
    if month_ordinal(fy, fm) > month_ordinal(ty, tm):
        raise ValueError(f"From month ({from_label}) must be on or before To month ({to_label}).")


def query_production_month_bounds(conn) -> Tuple[date, date]:
    """Return first day of earliest and latest calendar months in PCE_Production."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            MIN(CAST([Date] AS DATE)),
            MAX(CAST([Date] AS DATE))
        FROM dbo.PCE_Production
        """
    )
    row = cur.fetchone()
    if not row or row[0] is None or row[1] is None:
        raise ProductionDataEmptyError("PCE_Production has no production dates.")

    min_d = row[0] if isinstance(row[0], date) else row[0].date()
    max_d = row[1] if isinstance(row[1], date) else row[1].date()
    return first_day_of_month(min_d.year, min_d.month), first_day_of_month(
        max_d.year, max_d.month
    )


def _last_day_of_month(year: int, month: int) -> date:
    return date(year, month, monthrange(year, month)[1])


def apply_imperial_volumes(
    sum_gas: float, sum_cond: float, sum_water: float
) -> Tuple[float, float, float]:
    """Convert monthly metric sums to imperial totals."""
    return (
        float(sum_gas) * E3M3_TO_MCF,
        float(sum_cond) * M3_TO_BBL_COND,
        float(sum_water) * M3_TO_BBL_WATER,
    )


def _fetch_gathered_monthly_rows(
    conn,
    from_start: date,
    to_start: date,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> pd.DataFrame:
    """Query WM × month spine with aggregated gathered sums."""
    if progress_cb:
        progress_cb("Querying gathered monthly totals…")

    sql = """
    WITH MonthSpine AS (
        SELECT CAST(? AS DATE) AS MonthStart
        UNION ALL
        SELECT DATEADD(MONTH, 1, MonthStart)
        FROM MonthSpine
        WHERE DATEADD(MONTH, 1, MonthStart) <= CAST(? AS DATE)
    ),
    AggByProd AS (
        SELECT
            RTRIM(CAST(p.[Well Name] AS NVARCHAR(4000))) AS ProdWellName,
            YEAR(CAST(p.[Date] AS DATE)) AS ProdYear,
            MONTH(CAST(p.[Date] AS DATE)) AS ProdMonth,
            SUM(ISNULL(CAST(p.[Gathered Gas (e³m³/d)] AS FLOAT), 0)) AS SumGas,
            SUM(ISNULL(CAST(p.[Gathered Condensate (m³/d)] AS FLOAT), 0)) AS SumCond,
            SUM(ISNULL(CAST(p.[Gath. Water Rate (m³/d)] AS FLOAT), 0)) AS SumWater
        FROM dbo.PCE_Production AS p
        WHERE CAST(p.[Date] AS DATE) >= CAST(? AS DATE)
          AND CAST(p.[Date] AS DATE) <= CAST(? AS DATE)
        GROUP BY
            RTRIM(CAST(p.[Well Name] AS NVARCHAR(4000))),
            YEAR(CAST(p.[Date] AS DATE)),
            MONTH(CAST(p.[Date] AS DATE))
    ),
    Agg AS (
        SELECT
            wm.[Well Name] AS WmWellName,
            a.ProdYear,
            a.ProdMonth,
            SUM(a.SumGas) AS SumGas,
            SUM(a.SumCond) AS SumCond,
            SUM(a.SumWater) AS SumWater
        FROM AggByProd AS a
        INNER JOIN dbo.PCE_WM AS wm ON (
            RTRIM(CAST(wm.[Well Name] AS NVARCHAR(4000))) = a.ProdWellName
            OR (
                NULLIF(RTRIM(CAST(wm.[Composite Name] AS NVARCHAR(4000))), N'') IS NOT NULL
                AND RTRIM(CAST(wm.[Composite Name] AS NVARCHAR(4000))) = a.ProdWellName
            )
        )
        WHERE (
            wm.[Exception] IS NULL
            OR wm.[Exception] = N''
            OR wm.[Exception] = N'N'
        )
        GROUP BY wm.[Well Name], a.ProdYear, a.ProdMonth
    )
    SELECT
        wm.[Value Navigator UWI] AS UWI,
        wm.[Composite Name] AS [Composite Name],
        CONVERT(VARCHAR(7), ms.MonthStart, 120) AS [Month],
        COALESCE(a.SumGas, 0) AS SumGas,
        COALESCE(a.SumCond, 0) AS SumCond,
        COALESCE(a.SumWater, 0) AS SumWater
    FROM dbo.PCE_WM AS wm
    CROSS JOIN MonthSpine AS ms
    LEFT JOIN Agg AS a
        ON RTRIM(CAST(a.WmWellName AS NVARCHAR(4000))) = RTRIM(CAST(wm.[Well Name] AS NVARCHAR(4000)))
       AND a.ProdYear = YEAR(ms.MonthStart)
       AND a.ProdMonth = MONTH(ms.MonthStart)
    WHERE (
        wm.[Exception] IS NULL
        OR wm.[Exception] = N''
        OR wm.[Exception] = N'N'
    )
    ORDER BY wm.[Value Navigator UWI], ms.MonthStart
    OPTION (MAXRECURSION 500)
    """

    cur = conn.cursor()
    cur.execute(
        sql,
        from_start,
        to_start,
        from_start,
        _last_day_of_month(to_start.year, to_start.month),
    )
    columns = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return pd.DataFrame.from_records(rows, columns=columns)


def finalize_export_dataframe(df: pd.DataFrame, units: str) -> pd.DataFrame:
    """Rename volume columns and apply imperial conversion if requested."""
    if df.empty:
        out = df.copy()
        headers = _IMPERIAL_HEADERS if units == UNITS_IMPERIAL else _METRIC_HEADERS
        return out.rename(columns=headers)

    out = df.copy()
    if units == UNITS_IMPERIAL:
        converted = [
            apply_imperial_volumes(r[_COL_GAS], r[_COL_COND], r[_COL_WATER])
            for _, r in out.iterrows()
        ]
        out[_COL_GAS] = [c[0] for c in converted]
        out[_COL_COND] = [c[1] for c in converted]
        out[_COL_WATER] = [c[2] for c in converted]
        headers = _IMPERIAL_HEADERS
    else:
        headers = _METRIC_HEADERS

    return out.rename(
        columns={
            "UWI": "UWI",
            "Composite Name": "Composite Name",
            "Month": "Month",
            **headers,
        }
    )


def run_gathered_monthly_export(
    conn,
    from_month_label: str,
    to_month_label: str,
    units: str,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> pd.DataFrame:
    """
    Build export DataFrame for active WM wells and the inclusive month range.

    ``units`` is ``metric`` or ``imperial``.
    """
    validate_month_range(from_month_label, to_month_label)
    if units not in (UNITS_METRIC, UNITS_IMPERIAL):
        raise ValueError(f"Invalid units: {units!r}")

    fy, fm = parse_month_label(from_month_label)
    ty, tm = parse_month_label(to_month_label)
    from_start = first_day_of_month(fy, fm)
    to_start = first_day_of_month(ty, tm)

    raw = _fetch_gathered_monthly_rows(conn, from_start, to_start, progress_cb)
    return finalize_export_dataframe(raw, units)


def write_excel(df: pd.DataFrame, path: str) -> None:
    """Write export DataFrame to an Excel workbook."""
    if not path.lower().endswith(".xlsx"):
        path = f"{path}.xlsx"
    df.to_excel(path, index=False, engine="openpyxl")
