import os
from dotenv import load_dotenv
load_dotenv()


# preview.py
from snowflake_connector import SnowflakeConnector
from queries import QUERIES

sf = SnowflakeConnector()

for name, sql in QUERIES.items():
    print(f"\n=== {name} ===")
    try:
        # get count
        count_sql = f"SELECT COUNT(*) AS total_rows FROM ({sql.strip().rstrip(';')}) t"
        count_df = sf.query(count_sql)
        total_rows = int(count_df.iloc[0, 0])

        # get latest 10 by DTTM
        preview_sql = sql.strip().rstrip(";") + " ORDER BY DTTM DESC LIMIT 10"
        df = sf.query(preview_sql)

        print(f"Total rows: {total_rows}")
        print(df)
    except Exception as e:
        print(f"[ERROR] {name}: {e}")

sf.close()
