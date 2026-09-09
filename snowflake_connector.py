# snowflake_connector.py
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector
import pandas as pd

def _app_dir():
    """Return the application directory, whether running from source or frozen exe."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


<<<<<<< HEAD
=======
def _resolve_private_key_path(raw_path: str) -> Path:
    """Resolve private key path relative to the application directory when not absolute."""
    path = Path(raw_path)
    if not path.is_absolute():
        path = _app_dir() / path
    return path.resolve()


>>>>>>> 7836e4dd58780a7e81d7c476cde1ec01ffb1baca
class SnowflakeConnector:
    def __init__(self):
        env_path = _app_dir() / ".env"
        load_dotenv(dotenv_path=env_path)

        self.conn = None

    def connect(self):
        if self.conn is not None:
            return self.conn

        account = os.getenv("SNOWFLAKE_ACCOUNT")
        user = os.getenv("SNOWFLAKE_USER")
<<<<<<< HEAD
        password = os.getenv("SNOWFLAKE_PASSWORD")
=======
        private_key_path_raw = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
        private_key_passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
>>>>>>> 7836e4dd58780a7e81d7c476cde1ec01ffb1baca
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
        database = os.getenv("SNOWFLAKE_DATABASE")
        schema = os.getenv("SNOWFLAKE_SCHEMA")
        role = os.getenv("SNOWFLAKE_ROLE")

<<<<<<< HEAD
        # Validate required environment variables
=======
>>>>>>> 7836e4dd58780a7e81d7c476cde1ec01ffb1baca
        missing_vars = []
        if not account:
            missing_vars.append("SNOWFLAKE_ACCOUNT")
        if not user:
            missing_vars.append("SNOWFLAKE_USER")
<<<<<<< HEAD
        if not password:
            missing_vars.append("SNOWFLAKE_PASSWORD")
        
=======
        if not private_key_path_raw:
            missing_vars.append("SNOWFLAKE_PRIVATE_KEY_PATH")

>>>>>>> 7836e4dd58780a7e81d7c476cde1ec01ffb1baca
        if missing_vars:
            error_msg = (
                f"Missing required Snowflake configuration in .env file:\n"
                f"  {', '.join(missing_vars)}\n\n"
                f"Please ensure .env file exists in the application directory\n"
                f"and contains all required Snowflake connection settings."
            )
            raise RuntimeError(error_msg)

<<<<<<< HEAD
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
=======
        private_key_file = _resolve_private_key_path(private_key_path_raw)
        if not private_key_file.is_file():
            raise RuntimeError(
                f"Snowflake private key file not found:\n"
                f"  {private_key_file}\n\n"
                f"Set SNOWFLAKE_PRIVATE_KEY_PATH in .env to your .p8 file\n"
                f"(relative paths are resolved from the application directory)."
            )

        conn_params = {
            "account": account,
            "user": user,
            "authenticator": "SNOWFLAKE_JWT",
            "private_key_file": str(private_key_file),
            "warehouse": warehouse,
            "database": database,
            "schema": schema,
            "role": role,
            "timeout": 30,
        }
        if private_key_passphrase:
            conn_params["private_key_file_pwd"] = private_key_passphrase

        try:
            self.conn = snowflake.connector.connect(**conn_params)
>>>>>>> 7836e4dd58780a7e81d7c476cde1ec01ffb1baca
            return self.conn
        except Exception as e:
            error_msg = (
                f"Failed to connect to Snowflake:\n"
                f"  Account: {account}\n"
                f"  User: {user}\n"
<<<<<<< HEAD
                f"  Error: {str(e)}\n\n"
                f"Please check:\n"
                f"  1. Snowflake credentials are correct\n"
                f"  2. Network connectivity\n"
                f"  3. Snowflake account is accessible"
=======
                f"  Private key: {private_key_file}\n"
                f"  Error: {str(e)}\n\n"
                f"Please check:\n"
                f"  1. Snowflake key-pair credentials are correct\n"
                f"  2. The public key is registered for this user in Snowflake\n"
                f"  3. Network connectivity\n"
                f"  4. Snowflake account is accessible"
>>>>>>> 7836e4dd58780a7e81d7c476cde1ec01ffb1baca
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
