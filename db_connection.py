# db_connection.py
import pyodbc
import os
from dotenv import load_dotenv
from pathlib import Path

# Load .env file
env_path = Path(__file__).resolve().parent / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    load_dotenv()

SQL_SERVER = os.getenv("SQL_SERVER", "CALVMSQL02")
SQL_DATABASE = os.getenv("SQL_DATABASE", "Re_Main_Production")
SQL_DRIVER = os.getenv("SQL_DRIVER", "{ODBC Driver 17 for SQL Server}")

def get_sql_conn():
    """Create connection to SQL Server with error handling"""
    conn_str = (
        f'DRIVER={SQL_DRIVER};'
        f'SERVER={SQL_SERVER};'
        f'DATABASE={SQL_DATABASE};'
        f'Trusted_Connection=yes;'
        f'Connection Timeout=30;'
    )
    try:
        return pyodbc.connect(conn_str, timeout=30)
    except pyodbc.Error as e:
        error_msg = (
            f"Failed to connect to SQL Server:\n"
            f"  Server: {SQL_SERVER}\n"
            f"  Database: {SQL_DATABASE}\n"
            f"  Error: {str(e)}\n\n"
            f"Please check:\n"
            f"  1. SQL Server is running and accessible\n"
            f"  2. Network connectivity\n"
            f"  3. Windows authentication is working\n"
            f"  4. ODBC Driver 17 is installed"
        )
        raise ConnectionError(error_msg) from e