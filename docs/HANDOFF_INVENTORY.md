# Handoff inventory — Python modules and artifacts

Generated for internal continuity / transfer. **Classification:** GUI/ETL chain, test, CLI-only/legacy, dev utility, or build artifact.

## GUI and ETL (keep — active application chain)

- **`production_update_gui.py`** — Main window, navigation, CLI dispatch for accumap audit.
- **`settings_dialog.py`** — Settings UI / `settings.ini`.
- **`exports_dialog.py`** — Exports placeholder UI.
- **`prodview_update_dialog.py`** — Prodview UI.
- **`prodview_update_gui.py`** — Snowflake/SQL CDA and production update logic.
- **`prodview_date_bounds.py`** — Rolling Snowflake window and “today − lag” end date (shared with `production_update`).
- **`monthly_loader_dialog.py`** — PA dialog.
- **`monthly_loader_gui.py`** — PA allocation logic.
- **`sales_ratios_dialog.py`** — Public Sales UI.
- **`sales_ratios_gui.py`** — Public Sales / sales ratios logic.
- **`sales_allocation_updates.py`** — Shared allocation SQL helpers.
- **`well_master_gui.py`** — Well Master UI.
- **`well_master_db.py`** — Well Master data access.
- **`well_master_delegates.py`** — Table delegates.
- **`well_master_cda_worker.py`** — Background CDA populate after WM import.
- **`survey_import_dialog.py`** — Survey UI.
- **`survey_mapping_dialog.py`** — Directional survey mapping UI.
- **`survey_import.py`** — Survey ETL + CLI entry.
- **`type_curves_import_dialog.py`** — Type curves UI.
- **`type_curves_import.py`** — Type curves ETL.
- **`sync_typecurves_to_production.py`** — Materialize `PCE_TC` into `PCE_Production` at `ImportDate`.
- **`whitson_mass_upload_dialog.py`** — Whitson+ UI (stub worker).
- **`accumap_unmatched_cli.py`** — Accumap UWI audit (GUI and script entry).
- **`db_connection.py`** — SQL Server (pyodbc, env).
- **`snowflake_connector.py`** — Snowflake (env).
- **`app_paths.py`** — `settings.ini` path resolution.
- **`log_format.py`** — Log formatting.
- **`styles.py`** — Shared Qt styles.
- **`purge_exception_wells.py`** — WM delete dependencies (imported by `well_master_db`).
- **`production_update.py`** — Full rebuild / legacy CDA pipeline (imported by `prodview_update_dialog` / `prodview_update_gui`).

## Tests (keep)

- `tests/test_prodview_helpers.py`
- `tests/test_run_quick_update.py`
- `tests/test_type_curves_match_key.py`
- `tests/test_prodview_date_bounds.py`

## CLI / legacy scripts (keep — not imported by GUI; run directly)

- **`cda.py`** — Legacy Snowflake → `PCE_CDA` pipeline; document in README; do not delete without sign-off.
- **`af.py`** — Allocation Factors Excel loader (`input()`); script body still has **hardcoded `I:\...` paths** for historical runs — replace when maintaining; document in README.
- **`survey_import.py`** — Also runnable as `python survey_import.py "<path>"` with second argument `append` or `overwrite`.
- **`gas_idrec_production_peek.py`** — Debug / support CLI.
- **`test_well_lookup.py`** — Ad hoc well/CDA checks.
- **`purge_exception_wells.py`** — Also runnable as `__main__` for batch purge.
- **`scripts/accumap_unmatched_uwis.py`** — Thin wrapper → `accumap_unmatched_cli`.

## Orphaned or low-value wrappers (resolved in handoff pass)

- **`type.py`** — **Removed** — use `type_curves_import.append_typecurves_from_excel` / `delete_typecurves_from_tc` directly.
- **`pyo.py`** — **Removed** — replaced by **`scripts/list_odbc_drivers.py`** for ODBC driver listing.

## Build / generated (not source of truth)

- **`dist/`** — PyInstaller output; build per `PACKAGING_WINDOWS.md`; do not treat as editable source when archiving.
- **`__pycache__/`** — Generated at runtime; omit from source handoffs; never treat as editable source.

## Configuration

- **`settings.ini`** — **Office / team configuration** (SQL labels, ValNav–Accumap–survey paths — usually **shared network paths**, so one file can match every workstation); use `settings.ini.example` when documenting a fresh layout.
- **`.env`** — Secrets; keep on the machine only — use `.env.example` for variable names when documenting setup.

## Documentation files

- **`USER_GUIDE.md`** — Operators — comprehensive.
- **`DEV_GUIDE.md`** — Stakeholders / onboarding — short overview.
- **`DEV_GUIDE_LAYOUT.md`** — Developers — deep technical map.
- **`COWORKER_SETUP.md`** — Deployed exe consumers.
- **`PACKAGING_WINDOWS.md`** — Build engineers.
- **`APPLICATION_ARCHITECTURE.md`** — Architecture diagram + notes.

## Verification command (imports)

```bash
python -m compileall -q .
python -m pytest -q
```
