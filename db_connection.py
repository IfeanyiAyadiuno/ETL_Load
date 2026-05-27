# db_connection.py
import sys
import os
import configparser

import pyodbc
from dotenv import load_dotenv
from pathlib import Path


def _app_dir():
    """Return the application directory, whether running from source or frozen exe."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


# Load .env file (baseline for SQL_* and driver)
env_path = _app_dir() / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    load_dotenv()

SQL_SERVER = os.getenv("SQL_SERVER", "CALVMSQL02")
SQL_DATABASE = os.getenv("SQL_DATABASE", "Re_Main_Production")
SQL_DRIVER = os.getenv("SQL_DRIVER", "{ODBC Driver 17 for SQL Server}")


def _read_sql_credentials_from_ini() -> tuple[str | None, str | None]:
    """
    Return (server, database) from settings.ini [SQL] if the file exists and values are present.
    """
    try:
        from app_paths import get_settings_path

        path = get_settings_path()
        if not path or not os.path.isfile(path):
            return None, None
        cfg = configparser.ConfigParser(interpolation=None)
        cfg.read(path, encoding="utf-8")
        if not cfg.has_section("SQL"):
            return None, None
        srv = cfg.get("SQL", "server", fallback="").strip()
        db = cfg.get("SQL", "database", fallback="").strip()
        return (srv or None), (db or None)
    except Exception:
        return None, None


def _apply_sql_credentials(server: str | None, database: str | None) -> None:
    """Promote resolved server/database to module globals + os.environ (keeps getenv in sync)."""
    global SQL_SERVER, SQL_DATABASE
    if server:
        SQL_SERVER = server.strip()
        os.environ["SQL_SERVER"] = SQL_SERVER
    if database:
        SQL_DATABASE = database.strip()
        os.environ["SQL_DATABASE"] = SQL_DATABASE


def configure_sql_targets(server: str, database: str) -> None:
    """
    Set SQL Server/database at runtime after **Settings → Save**.
    Mirrors into ``os.environ`` so getenv stays aligned with ``settings.ini``.
    If a ``.env`` file exists next to the application, adds/updates ``SQL_SERVER`` and
    ``SQL_DATABASE`` there as well.

    Empty strings skip updating that field so we never accidentally blank a credential.
    """
    s = server.strip() if server else ""
    d = database.strip() if database else ""
    if not s and not d:
        return
    _apply_sql_credentials(s if s else None, d if d else None)
    _sync_dotenv_sql_credentials_file_if_present()


def _sync_dotenv_sql_credentials_file_if_present() -> None:
    """If ``.env`` exists beside the app, upsert SQL_SERVER / SQL_DATABASE only."""
    if not env_path.is_file():
        return
    try:
        raw = env_path.read_text(encoding="utf-8")
    except OSError:
        return

    normalized = raw.replace("\r\n", "\n").replace("\r", "\n")
    lines = normalized.split("\n")
    srv_line = f"SQL_SERVER={SQL_SERVER}"
    db_line = f"SQL_DATABASE={SQL_DATABASE}"
    srv_hit = db_hit = False
    out_lines: list[str] = []

    for segment in lines:
        st = segment.lstrip("\ufeff")
        if not st.strip() or st.lstrip().startswith("#"):
            out_lines.append(segment)
            continue
        if "=" not in st:
            out_lines.append(segment)
            continue
        key, _, _val = st.partition("=")
        kl = key.strip().upper()
        if kl == "SQL_SERVER":
            out_lines.append(srv_line)
            srv_hit = True
        elif kl == "SQL_DATABASE":
            out_lines.append(db_line)
            db_hit = True
        else:
            out_lines.append(segment)

    if not srv_hit:
        out_lines.append(srv_line)
    if not db_hit:
        out_lines.append(db_line)

    try:
        env_path.write_text("\n".join(out_lines) + "\n", encoding="utf-8")
    except OSError:
        return


def merge_sql_from_settings_ini_into_runtime() -> None:
    """
    Reload server/database from ``settings.ini`` if present.

    Intended at import-time so the GUI-managed database wins over stale ``.env`` once
    a settings file exists. Call ``configure_sql_targets`` after Saves to refresh
    without restarting.
    """
    ini_srv, ini_db = _read_sql_credentials_from_ini()
    _apply_sql_credentials(ini_srv, ini_db)


# After .env baseline, layer ``settings.ini`` [SQL] (if any) onto globals + environ
merge_sql_from_settings_ini_into_runtime()


def sql_target_label() -> str:
    """Human-readable server.database from Settings / runtime config."""
    return f"{SQL_SERVER}.{SQL_DATABASE}"


def probe_sql_connection() -> tuple[bool, str]:
    """
    Open a connection and verify the active database.

    Returns (success, message) including configured target and live DB_NAME().
    """
    try:
        conn = get_sql_conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT DB_NAME()")
            row = cur.fetchone()
            live_db = row[0] if row else "?"
            return (
                True,
                f"Connected to {sql_target_label()} (active database: {live_db})",
            )
        finally:
            conn.close()
    except Exception as exc:
        return False, f"Cannot connect to {sql_target_label()}: {exc}"


def get_sql_conn():
    """Create connection to SQL Server with error handling"""
    conn_str = (
        f"DRIVER={SQL_DRIVER};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        "Trusted_Connection=yes;"
        "Connection Timeout=30;"
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
