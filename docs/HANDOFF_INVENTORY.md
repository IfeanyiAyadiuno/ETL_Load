# Handoff inventory — Python modules and artifacts

Inventory of every Python file and other artifact in the repository, classified as part of the **GUI / ETL chain**, **tests**, **CLI helper**, **dev utility**, **build output**, or **configuration**.

## GUI and ETL (active application chain)

- **`production_update_gui.py`** — Main window, navigation, CLI dispatch for accumap audit.
- **`settings_dialog.py`** — Settings UI / `settings.ini`.
- **`exports_dialog.py`** — Exports placeholder UI.
- **`prodview_update_dialog.py`** — Prodview UI.
- **`prodview_update_gui.py`** — Snowflake/SQL CDA and production update logic.
- **`prodview_date_bounds.py`** — Rolling Snowflake window and "today − lag" end date (shared with `production_update`).
- **`monthly_loader_dialog.py`** — PA dialog.
- **`monthly_loader_gui.py`** — PA allocation logic.
- **`sales_ratios_dialog.py`** — Public Sales UI.
- **`sales_ratios_gui.py`** — Public Sales / sales ratios logic.
- **`sales_allocation_updates.py`** — Shared allocation SQL helpers.
- **`well_master_gui.py`** — Well Master UI.
- **`well_master_db.py`** — Well Master data access.
- **`well_master_delegates.py`** — Table delegates.
- **`survey_import_dialog.py`** — Survey UI.
- **`survey_mapping_dialog.py`** — Directional survey mapping UI.
- **`survey_import.py`** — Survey ETL + CLI entry.
- **`type_curves_import_dialog.py`** — Type curves UI.
- **`type_curves_import.py`** — Type curves ETL.
- **`sync_typecurves_to_production.py`** — Materialize `PCE_TC` into `PCE_Production` at `ImportDate`.
- **`whitson_mass_upload_dialog.py`** — Whitson+ UI (stub worker).
- **`accumap_unmatched_cli.py`** — Accumap UWI audit (GUI and `--accumap-unmatched` CLI entry).
- **`db_connection.py`** — SQL Server (pyodbc, env).
- **`snowflake_connector.py`** — Snowflake (env).
- **`app_paths.py`** — `settings.ini` path resolution.
- **`log_format.py`** — Log formatting.
- **`styles.py`** — Shared Qt styles.
- **`purge_exception_wells.py`** — Deletes CDA / Production / Allocation / Survey rows for wells flagged with `PCE_WM.Exception = 'Y'`. Imported by `well_master_db`; also runnable as `python purge_exception_wells.py`.
- **`production_update.py`** — Full rebuild / legacy CDA pipeline (imported by `prodview_update_dialog` / `prodview_update_gui`).

## Tests (`tests/`, run with `pytest`)

- `tests/test_prodview_helpers.py`
- `tests/test_prodview_date_bounds.py`
- `tests/test_run_quick_update.py`
- `tests/test_type_curves_match_key.py`
- `tests/test_survey_well_match.py`
- `tests/test_survey_wm_composite_name.py`

## SQL helpers (`scripts/`)

- `scripts/add_pce_tc_wh_cumulative_columns.sql` — One-time migration: adds the `Gas WH Cumulative Production (10³m³)` and `Condensate WH Cumulative Production (m³)` columns to `PCE_TC` and backfills them from existing cumulative columns. Run once per database; safe to re-run (guarded by `IF NOT EXISTS`).

## Configuration

- **`settings.ini`** — Office / team configuration (SQL labels, ValNav / Accumap / survey paths — usually shared network paths so one file matches every workstation). Use `settings.ini.example` when documenting a fresh setup.
- **`.env`** — Secrets (Snowflake credentials, optional SQL overrides). Stays on the workstation only; not tracked by git. Use `.env.example` for the variable names.

## Documentation files (this folder)

- **`USER_GUIDE.md`** — Operators — comprehensive procedures and runbook.
- **`DEV_GUIDE.md`** — Stakeholder / onboarding overview (short).
- **`DEV_GUIDE_LAYOUT.md`** — Deep technical map for developers / leads.
- **`PACKAGING_WINDOWS.md`** — Building the Windows executable.
- **`APPLICATION_ARCHITECTURE.md`** — Schema diagram and notes.
- **`CODE_OPTIMIZATIONS.md`**, **`CODE_REVIEW_REDUNDANCY.md`** — Internal code review notes.
- **`presentation.md`** / **`presentation.pptx`** — Project overview deck.

## Build / generated (not source of truth)

- **`build/`** — PyInstaller intermediate output; rebuild per `PACKAGING_WINDOWS.md`. Gitignored.
- **`dist/`** — PyInstaller final output (the `.exe` and `_internal/`). Gitignored.
- **`__pycache__/`** — Generated at runtime; gitignored.

## Verification commands

```bash
python -m compileall -q .
python -m pytest -q
```
