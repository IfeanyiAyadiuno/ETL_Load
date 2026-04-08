# What coworkers need to run the Production Update app

Internal checklist. You place the built app on the share; they run the `.exe` from that folder (or a full copy of the folder on their PC if the network blocks it).

---

## On the computer

1. **Windows 10 or 11 (64-bit)** — same as typical office PCs.

2. **Microsoft ODBC Driver for SQL Server** — the app uses **ODBC** to SQL Server. Install **ODBC Driver 17 for SQL Server** (default in the app) unless your `.env` sets `SQL_DRIVER` to Driver 18, in which case install that driver instead.  
   - [Microsoft ODBC driver download](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

3. **Network path to SQL Server** — they must be able to reach the SQL Server host (e.g. corporate LAN or **VPN** when off-site). No extra “SQL client” install is required beyond the ODBC driver.

4. **Windows account permission** — the app connects with **`Trusted_Connection=yes`** (Windows authentication). Their **domain / Windows login** must be **granted access** on that SQL Server and database (same as anyone who uses SSMS with Windows auth to that server). If they cannot connect in SSMS with Windows auth, the app will not connect either.

5. **Snowflake (only if they use Prodview, well import from Snowflake, etc.)** — outbound **HTTPS** to Snowflake must be allowed, and the **`.env`** next to the `.exe` must contain valid Snowflake settings. Required for connection: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`. Optional in `.env` if you use them: `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_ROLE`.

6. **SQL Server / database names** — usually set in **`.env`** as `SQL_SERVER` and `SQL_DATABASE`. If `.env` is missing, the app falls back to built-in defaults in code (confirm what you ship).

---

## In the shared folder (you maintain this)

Next to the **`.exe`**, in the **same folder**:

- The full **PyInstaller output**: the `.exe`, the **`_internal`** folder, and any bundled files.
- **`.env`** — you provide this (SQL + Snowflake variables as above). The app only auto-loads **`.env` from the folder that contains the executable** (not their user profile).
- **`settings.ini`** — if you rely on saved paths and SQL display fields, keep it beside the `.exe` or let them set **Settings** once.

They need **read (and execute) permission** on that share. Restrict the share if `.env` holds secrets.

---

## First run

1. Connect **VPN** if your SQL Server is not reachable from their current network.  
2. Open the shared folder in File Explorer and double-click the **`.exe`**.  
3. If Windows **SmartScreen** warns about an unknown publisher, use **More info → Run anyway** if policy allows.  
4. If the app **does not start** or is **very slow** when run from `\\server\...`, copy the **entire** application folder to a local path (e.g. Desktop), keep **`.env` next to the `.exe`**, and run from there.

---

## Quick “it doesn’t connect” checks

- VPN / on-site network.  
- ODBC driver installed and matches `SQL_DRIVER` in `.env`.  
- `.env` is in the **same folder as the `.exe`**.  
- Their Windows account has SQL access.  
- For Snowflake: credentials in `.env` and firewall allows Snowflake.
