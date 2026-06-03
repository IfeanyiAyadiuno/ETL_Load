"""
Run on the Windows PC (same folder as ProductionUpdate.exe / .env) to check whether
Snowflake returns gathered water for pvunitallocmonthday.

Usage (from ETL_Load folder):
    python scripts/diagnose_gathered_water_snowflake.py

Optional:
    python scripts/diagnose_gathered_water_snowflake.py --days 60
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta

from snowflake_connector import SnowflakeConnector


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Look back this many days from today−5 (default 30)",
    )
    args = parser.parse_args()

    end = date.today() - timedelta(days=5)
    start = end - timedelta(days=max(args.days, 1))
    params = (str(start), str(end))
    print(f"Date range: {start} through {end}\n")

    sf = SnowflakeConnector()
    try:
        cols_sql = """
        SELECT column_name
        FROM PACIFICCANBRIAM_PV30.INFORMATION_SCHEMA.COLUMNS
        WHERE table_schema = 'UNITSMETRIC'
          AND table_name = 'PVUNITALLOCMONTHDAY'
          AND (column_name ILIKE '%GATH%' OR column_name ILIKE '%WATER%')
        ORDER BY 1
        """
        print("=== pvunitallocmonthday columns (GATH / WATER) ===")
        print(sf.query(cols_sql).to_string(index=False))
        print()

        agg_sql = """
        SELECT
            COUNT(*) AS n_rows,
            COUNT(VOLPRODGATHGAS) AS n_gas,
            COUNT(VOLPRODGATHHCLIQ) AS n_cond,
            COUNT(VOLPRODGATHWATER) AS n_water,
            SUM(CASE WHEN VOLPRODGATHGAS <> 0 THEN 1 ELSE 0 END) AS gas_nonzero,
            SUM(CASE WHEN VOLPRODGATHWATER <> 0 THEN 1 ELSE 0 END) AS water_nonzero
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvunitallocmonthday
        WHERE DTTM >= %s AND DTTM <= %s
        """
        print("=== row counts (alloc table) ===")
        print(sf.query(agg_sql, params).to_string(index=False))
        print()

        sample_sql = """
        SELECT
            IDRECCOMP,
            CAST(DTTM AS DATE) AS ProdDate,
            VOLPRODGATHGAS,
            VOLPRODGATHHCLIQ,
            VOLPRODGATHWATER
        FROM PACIFICCANBRIAM_PV30.UNITSMETRIC.pvunitallocmonthday
        WHERE DTTM >= %s AND DTTM <= %s
          AND (VOLPRODGATHGAS <> 0 OR VOLPRODGATHWATER <> 0)
        ORDER BY DTTM DESC
        LIMIT 10
        """
        print("=== sample rows (gas or water nonzero) ===")
        print(sf.query(sample_sql, params).to_string(index=False))
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 1
    finally:
        sf.close()

    print(
        "\nIf water_nonzero is 0 but gas_nonzero > 0, Prodview may not store gathered "
        "water in VOLPRODGATHWATER for this asset — confirm the field name in Prodview "
        "Snowflake / support."
    )
    print(
        "If water_nonzero > 0 but PCE_CDA is still empty, run Prodview "
        "Full rebuild (full CDA history) or Quick Update (~18 mo rolling window)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
