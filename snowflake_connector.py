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


def _resolve_private_key_path(raw_path: str) -> Path:
    """Resolve private key path relative to the application directory when not absolute."""
    path = Path(raw_path)
    if not path.is_absolute():
        path = _app_dir() / path
    return path.resolve()


def _load_snowflake_env() -> None:
    env_path = _app_dir() / ".env"
    load_dotenv(dotenv_path=env_path)


def snowflake_target_label() -> str:
    """Human-readable Snowflake account/user/database from ``.env``."""
    _load_snowflake_env()
    account = os.getenv("SNOWFLAKE_ACCOUNT") or "?"
    user = os.getenv("SNOWFLAKE_USER") or "?"
    database = os.getenv("SNOWFLAKE_DATABASE") or "?"
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE") or "?"
    return f"{user}@{account} / {database} (warehouse: {warehouse})"


def probe_snowflake_connection() -> tuple[bool, str]:
    """
    Open a Snowflake connection and verify the session context.

    Returns (success, message) for GUI status labels.
    """
    try:
        sf = SnowflakeConnector()
        conn = sf.connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()"
            )
            row = cur.fetchone()
            user, role, wh, db = row if row else ("?", "?", "?", "?")
            return (
                True,
                f"Connected to Snowflake as {user} (role: {role}, warehouse: {wh}, database: {db})",
            )
        finally:
            sf.close()
    except Exception as exc:
        return False, f"Cannot connect to Snowflake ({snowflake_target_label()}): {exc}"


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
        private_key_path_raw = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
        private_key_passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
        database = os.getenv("SNOWFLAKE_DATABASE")
        schema = os.getenv("SNOWFLAKE_SCHEMA")
        role = os.getenv("SNOWFLAKE_ROLE")

        missing_vars = []
        if not account:
            missing_vars.append("SNOWFLAKE_ACCOUNT")
        if not user:
            missing_vars.append("SNOWFLAKE_USER")
        if not private_key_path_raw:
            missing_vars.append("SNOWFLAKE_PRIVATE_KEY_PATH")

        if missing_vars:
            error_msg = (
                f"Missing required Snowflake configuration in .env file:\n"
                f"  {', '.join(missing_vars)}\n\n"
                f"Please ensure .env file exists in the application directory\n"
                f"and contains all required Snowflake connection settings."
            )
            raise RuntimeError(error_msg)

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
            return self.conn
        except Exception as e:
            error_msg = (
                f"Failed to connect to Snowflake:\n"
                f"  Account: {account}\n"
                f"  User: {user}\n"
                f"  Private key: {private_key_file}\n"
                f"  Error: {str(e)}\n\n"
                f"Please check:\n"
                f"  1. Snowflake key-pair credentials are correct\n"
                f"  2. The public key is registered for this user in Snowflake\n"
                f"  3. Network connectivity\n"
                f"  4. Snowflake account is accessible"
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
