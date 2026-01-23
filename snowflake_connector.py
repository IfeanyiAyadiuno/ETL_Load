# snowflake_connector.py
import os
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector
import pandas as pd

class SnowflakeConnector:
    def __init__(self):
        # Always load .env from THIS folder (works no matter where you run from)
        env_path = Path(__file__).resolve().parent / ".env"
        load_dotenv(dotenv_path=env_path)

        self.conn = None

    def connect(self):
        if self.conn is not None:
            return self.conn

        account = os.getenv("SNOWFLAKE_ACCOUNT")
        user = os.getenv("SNOWFLAKE_USER")
        password = os.getenv("SNOWFLAKE_PASSWORD")
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
        database = os.getenv("SNOWFLAKE_DATABASE")
        schema = os.getenv("SNOWFLAKE_SCHEMA")
        role = os.getenv("SNOWFLAKE_ROLE")

        # Debug (safe): don't print password
        if not account:
            raise RuntimeError("SNOWFLAKE_ACCOUNT is missing inside SnowflakeConnector.connect()")

        self.conn = snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role,
        )
        return self.conn

    def query(self, sql: str) -> pd.DataFrame:
        conn = self.connect()
        cur = conn.cursor()
        try:
            cur.execute(sql)
            cols = [c[0] for c in cur.description]
            rows = cur.fetchall()
            return pd.DataFrame(rows, columns=cols)
        finally:
            cur.close()

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None
