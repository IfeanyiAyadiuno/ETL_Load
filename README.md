# Production Update System

Desktop application (**PyQt5**) for Pacific Canbriam Energy LTD: refresh **Snowflake / Prodview** daily data into SQL Server (**`PCE_CDA`**, **`PCE_Production`**), maintain **Well Master** (**`PCE_WM`**), run **Production Accounting (PA)** and **Public Sales** allocation passes, and import **surveys** and **type curves**.

## Documentation

All operational and developer documentation lives under [`docs/`](docs/):

- **[docs/USER_GUIDE.md](docs/USER_GUIDE.md)** — Operators: full procedures, tables, runbook.
- **[docs/DEV_GUIDE.md](docs/DEV_GUIDE.md)** — Stakeholder / onboarding overview.
- **[docs/DEV_GUIDE_LAYOUT.md](docs/DEV_GUIDE_LAYOUT.md)** — Deep technical map (module / SQL responsibilities).
- **[docs/APPLICATION_ARCHITECTURE.md](docs/APPLICATION_ARCHITECTURE.md)** — Logical schema + Mermaid diagram.
- **[docs/PACKAGING_WINDOWS.md](docs/PACKAGING_WINDOWS.md)** — Building the Windows executable.
- **[docs/HANDOFF_INVENTORY.md](docs/HANDOFF_INVENTORY.md)** — Module/script inventory.
- **[docs/CODE_OPTIMIZATIONS.md](docs/CODE_OPTIMIZATIONS.md)**, **[docs/CODE_REVIEW_REDUNDANCY.md](docs/CODE_REVIEW_REDUNDANCY.md)** — Internal review notes.
- **[docs/presentation.md](docs/presentation.md)** / **[docs/presentation.pptx](docs/presentation.pptx)** — Project overview deck.

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

Optional: create a venv (`python3 -m venv .venv`, activate, then `pip install -r requirements-dev.txt`) and run `python -m pytest -q` — automated tests, no GUI.

## Configuration

1. Copy **`.env.example`** to **`.env`** and set Snowflake (and optional SQL) variables.
2. Copy **`settings.ini.example`** to **`settings.ini`** (or use **Settings** in the app) and set **ValNav**, **Accumap**, **Survey**, **Type Curves**, and **Whitson+** file paths. Paths are normally **standard for the office** (often a network share), not different per PC.

## CLI utilities (no main window)

- `python production_update_gui.py --accumap-unmatched -m "Aug 2025"` — Accumap UWI audit (entry point in [accumap_unmatched_cli.py](accumap_unmatched_cli.py)).
- `python survey_import.py "<path-to-survey.xlsx>" [append|overwrite]` — Survey import without GUI.
- `python purge_exception_wells.py` — Delete CDA / Production / Allocation / Survey rows for wells with `PCE_WM.Exception = 'Y'`. See the runbook in [docs/USER_GUIDE.md](docs/USER_GUIDE.md).

## Project layout (short)

- **`production_update_gui.py`** — main window.
- **`*_dialog.py`** — PyQt dialogs; **`*_gui.py`** (where present) — worker threads and ETL logic.
- **`db_connection.py`**, **`snowflake_connector.py`** — data connections.
- **`tests/`** — pytest.
- **`docs/`** — all documentation (see above).

## License / support

Internal use. Support and change control per company IT and reservoir engineering practice. See [LICENSE](LICENSE).
