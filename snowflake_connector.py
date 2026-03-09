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

        # Validate required environment variables
        missing_vars = []
        if not account:
            missing_vars.append("SNOWFLAKE_ACCOUNT")
        if not user:
            missing_vars.append("SNOWFLAKE_USER")
        if not password:
            missing_vars.append("SNOWFLAKE_PASSWORD")
        
        if missing_vars:
            error_msg = (
                f"Missing required Snowflake configuration in .env file:\n"
                f"  {', '.join(missing_vars)}\n\n"
                f"Please ensure .env file exists in the application directory\n"
                f"and contains all required Snowflake connection settings."
            )
            raise RuntimeError(error_msg)

        try:
            self.conn = snowflake.connector.connect(
                account=account,
                user=user,
                password=password,
                warehouse=warehouse,
                database=database,
                schema=schema,
                role=role,
                timeout=30,
            )
            return self.conn
        except Exception as e:
            error_msg = (
                f"Failed to connect to Snowflake:\n"
                f"  Account: {account}\n"
                f"  User: {user}\n"
                f"  Error: {str(e)}\n\n"
                f"Please check:\n"
                f"  1. Snowflake credentials are correct\n"
                f"  2. Network connectivity\n"
                f"  3. Snowflake account is accessible"
            )
            raise ConnectionError(error_msg) from e

    def query(self, sql: str, params: tuple = None) -> pd.DataFrame:
        """
        Execute SQL query with optional parameters for safety
        
        Args:
            sql: SQL query string
            params: Optional tuple of parameters for parameterized queries
        """
        conn = self.connect()
        cur = conn.cursor()
        try:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            cols = [c[0] for c in cur.description]
            rows = cur.fetchall()
            return pd.DataFrame(rows, columns=cols)
        except Exception as e:
            error_msg = f"Snowflake query failed: {str(e)}\nQuery: {sql[:200]}..."
            raise RuntimeError(error_msg) from e
        finally:
            cur.close()

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None
