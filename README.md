# Production Update System

Desktop application (**PyQt5**) for Pacific Canbriam Energy LTD: refresh **Snowflake / Prodview** daily data into SQL Server (**`PCE_CDA`**, **`PCE_Production`**), maintain **Well Master** (**`PCE_WM`**), run **Production Accounting (PA)** and **Public Sales** allocation passes, and import **surveys** and **type curves**.

## Documentation

- **[USER_GUIDE.md](USER_GUIDE.md)** — Operators: full procedures, tables, runbook.

## Requirements

- **Windows** (64-bit), **ODBC Driver 17** (or 18) for SQL Server, network access to SQL Server (**Windows authentication**).
- **Snowflake** access (VPN/HTTPS as required) for Prodview and related imports — credentials in **`.env`** (see `.env.example`).
- **Python 3.10+** when running from source.

## Run from source

```bash
cd /path/to/ETL_Load
python -m pip install -r requirements.txt
python production_update_gui.py
```

Optional: create a venv (`python3 -m venv .venv`, activate, then `pip install -r requirements-dev.txt`) and run `python -m pytest -q` — **12** tests, no GUI.

## Configuration

1. Copy **`.env.example`** to **`.env`** and set Snowflake (and optional SQL) variables.
2. Copy **`settings.ini.example`** to **`settings.ini`** (or use **Settings** in the app) and set **ValNav**, **Accumap**, **Survey**, **Type Curves**, and **Whitson+** file paths. Paths are normally **standard for the office** (often a network share), not different per PC.

## CLI utilities (no main window)

- `python production_update_gui.py --accumap-unmatched -m "Aug 2025"` — Accumap UWI audit ([accumap_unmatched_cli.py](accumap_unmatched_cli.py)).
- `python survey_import.py "<path-to-survey.xlsx>" [append|overwrite]` — Survey import without GUI.
- `python cda.py` / `python af.py` / `python purge_exception_wells.py` — legacy or batch tools; see **USER_GUIDE** runbook and [docs/HANDOFF_INVENTORY.md](docs/HANDOFF_INVENTORY.md).

## Project layout (short)

- **`production_update_gui.py`** — main window.
- **`*_dialog.py`** — PyQt dialogs; **`*_gui.py`** (where present) — worker threads and ETL logic.
- **`db_connection.py`**, **`snowflake_connector.py`** — data connections.
- **`tests/`** — pytest.
- **`sql/`** — optional view / DDL scripts referenced in the user guide.

## License / support

Internal use. Support and change control per company IT and reservoir engineering practice.
