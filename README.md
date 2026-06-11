# Production Update System

Desktop application (**PyQt5**) for **Pacific Canbriam Energy LTD**: refresh **Snowflake / Prodview** daily data into SQL Server, maintain **Well Master**, run **ValNav (PA)** and **Public Sales** allocation passes, import **surveys**, **type curves**, and **monthly forecasts**, push production to **Whitson+**, and export gathered production reports.

## Documentation

**[docs/PRODUCTION_UPDATE_GUIDE.md](docs/PRODUCTION_UPDATE_GUIDE.md)** — single user manual (operations, runbook, ER diagrams, troubleshooting). Suitable for conversion to Word.

## Requirements

- **Windows** (64-bit), **Python 3.10+** when running from source
- **ODBC Driver 17 or 18** for SQL Server (Windows authentication)
- Network access to SQL Server and **Snowflake** (VPN as required by IT)
- Application password on startup (contact your team lead)

## Quick start

```bash
cd /path/to/ETL_Load
python -m venv .venv
.venv\Scripts\activate          # Windows
pip install -r requirements.txt
python production_update_gui.py
```

On macOS/Linux (development only): `source .venv/bin/activate`

## Configuration

| File | Purpose |
|------|---------|
| [`.env.example`](.env.example) | Copy to `.env` — Snowflake credentials and optional SQL overrides |
| [`settings.ini.example`](settings.ini.example) | Copy to `settings.ini` — SQL server/database and Excel template paths |

**Settings paths** (`[PATHS]` in `settings.ini`):

| Key | Used by |
|-----|---------|
| `valnav_template` | ValNav Monthly Update (Sales + NGL) |
| `accumap_template` | Public Sales Data and Ratios |
| `survey_file` | Survey Data Import |
| `type_curves_file` | Type Curves Import |
| `whitson_file` | Whitson+ Mass Upload |
| `monthly_forecasts_template` | Monthly Forecasts Import |

Paths are usually on a **shared drive** and the same for most users.

## Main window operations

1. Well Master List  
2. Prodview / Snowflake — Daily Production Retrieve  
3. ValNav Monthly Update (Sales + NGL)  
4. Public Sales Data and Ratios  
5. Survey Data Import  
6. Type Curves Import  
7. Monthly Forecasts Import  
8. Whitson+ Mass Upload  
9. Exports / Reports  

**Recommended update order:** Well Master (if needed) → Prodview → ValNav → Public Sales → optional imports. See the user guide for month-close detail.

## CLI utilities (no main window)

| Command | Purpose |
|---------|---------|
| `python production_update_gui.py --accumap-unmatched -m "Aug 2025"` | Accumap UWI audit vs Well Master |
| `python production_update.py` | Full Prodview rebuild (headless) |
| `python survey_import.py "<file>" append` | Survey import without GUI |
| `python purge_exception_wells.py` | Remove data for wells with `Exception = 'Y'` |
| `python scripts/whitson_upload.py --well-name "..."` | Push one well to Whitson+ |

## Tests

```bash
pip install -r requirements-dev.txt
python -m pytest -q
```

## Packaging (Windows exe)

```bash
pip install -r requirements-dev.txt
pyinstaller --clean ProductionUpdate.spec
```

Output: `dist/ProductionUpdate/ProductionUpdate.exe` plus `_internal/`. Ship the full folder with `settings.ini`, `.env`, and `images/`.

## Project layout

| Path | Role |
|------|------|
| `production_update_gui.py` | Main entry point |
| `*_dialog.py` | PyQt dialogs |
| `*_gui.py` / workers | Background ETL threads |
| `db_connection.py`, `snowflake_connector.py` | Database connections |
| `scripts/` | SQL migrations and CLI helpers |
| `tests/` | pytest unit tests |
| `docs/` | User documentation |

## License / support

Internal use. Support and change control per company IT and reservoir engineering practice.
