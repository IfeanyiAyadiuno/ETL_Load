"""
Compare gathered sums in PCE_Production vs export WM join for one calendar month.

Usage (from repo root, with SQL reachable):
  python scripts/diagnose_gathered_monthly_export.py --month "Jan 2025"
"""

from __future__ import annotations

import argparse

from db_connection import get_sql_conn
from exports_gathered_monthly import parse_month_label, first_day_of_month, _last_day_of_month


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", required=True, help='e.g. "Jan 2025"')
    args = parser.parse_args()
    y, m = parse_month_label(args.month)
    start = first_day_of_month(y, m)
    end = _last_day_of_month(y, m)

    conn = get_sql_conn()
    cur = conn.cursor()

    print(f"Month: {args.month} ({start} .. {end})\n")

    cur.execute(
        """
        SELECT COUNT(*) AS rows,
               COUNT(DISTINCT RTRIM([Well Name])) AS wells,
               SUM(ISNULL(CAST([Gathered Gas (e³m³/d)] AS FLOAT), 0)) AS gas,
               SUM(ISNULL(CAST([Gathered Condensate (m³/d)] AS FLOAT), 0)) AS cond,
               SUM(ISNULL(CAST([Gath. Water Rate (m³/d)] AS FLOAT), 0)) AS water
        FROM dbo.PCE_Production
        WHERE CAST([Date] AS DATE) BETWEEN ? AND ?
        """,
        start,
        end,
    )
    row = cur.fetchone()
    print("PCE_Production (all well names in month):")
    print(f"  rows={row[0]}, wells={row[1]}, sum gas={row[2]:,.2f}, cond={row[3]:,.2f}, water={row[4]:,.2f}")

    cur.execute(
        """
        SELECT COUNT(DISTINCT RTRIM(p.[Well Name])) AS prod_wells_matched_wm
        FROM dbo.PCE_Production p
        WHERE CAST(p.[Date] AS DATE) BETWEEN ? AND ?
          AND EXISTS (
              SELECT 1 FROM dbo.PCE_WM wm
              WHERE (wm.[Exception] IS NULL OR wm.[Exception] IN (N'', N'N'))
                AND (
                    RTRIM(CAST(wm.[Well Name] AS NVARCHAR(4000))) = RTRIM(CAST(p.[Well Name] AS NVARCHAR(4000)))
                    OR (
                        NULLIF(RTRIM(CAST(wm.[Composite Name] AS NVARCHAR(4000))), N'') IS NOT NULL
                        AND RTRIM(CAST(wm.[Composite Name] AS NVARCHAR(4000)))
                            = RTRIM(CAST(p.[Well Name] AS NVARCHAR(4000)))
                    )
                )
          )
        """,
        start,
        end,
    )
    matched = cur.fetchone()[0]
    print(f"\nProduction well names that match an active PCE_WM row: {matched}")

    cur.execute(
        """
        SELECT TOP 15
            RTRIM(p.[Well Name]) AS ProdWellName,
            SUM(ISNULL(CAST(p.[Gathered Gas (e³m³/d)] AS FLOAT), 0)) AS gas
        FROM dbo.PCE_Production p
        WHERE CAST(p.[Date] AS DATE) BETWEEN ? AND ?
          AND NOT EXISTS (
              SELECT 1 FROM dbo.PCE_WM wm
              WHERE (wm.[Exception] IS NULL OR wm.[Exception] IN (N'', N'N'))
                AND (
                    RTRIM(CAST(wm.[Well Name] AS NVARCHAR(4000))) = RTRIM(CAST(p.[Well Name] AS NVARCHAR(4000)))
                    OR (
                        NULLIF(RTRIM(CAST(wm.[Composite Name] AS NVARCHAR(4000))), N'') IS NOT NULL
                        AND RTRIM(CAST(wm.[Composite Name] AS NVARCHAR(4000)))
                            = RTRIM(CAST(p.[Well Name] AS NVARCHAR(4000)))
                    )
                )
          )
        GROUP BY RTRIM(p.[Well Name])
        HAVING SUM(ISNULL(CAST(p.[Gathered Gas (e³m³/d)] AS FLOAT), 0)) > 0
        ORDER BY gas DESC
        """,
        start,
        end,
    )
    orphans = cur.fetchall()
    print(f"\nTop production names with gas > 0 but NO active WM match ({len(orphans)} shown):")
    for name, gas in orphans:
        print(f"  {name!r}: gas sum={gas:,.2f}")

    conn.close()


if __name__ == "__main__":
    main()
