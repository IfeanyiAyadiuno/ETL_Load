# ETL_Load refactor changelog

## Phase 0 — Safety, secrets, and reliability

### Changes
- Added `settings.ini` and `access_token.txt` to `.gitignore`; redacted tracked `settings.ini` to placeholders.
- Removed hardcoded Whitson credentials from `scripts/whitson_upload.py`; credentials now come from `settings.ini` or `WHITSON_*` env vars (`whitson_credentials.py`).
- Whitelisted `--mapping-table` in `scripts/match_enersight_well_mapping.py`.
- Added `sql_connection()` context manager in `db_connection.py`.
- Fixed connection leaks in `monthly_loader_gui.py`, `well_master_gui.py`, and `well_master_db.py` read helpers.
- Deleted dead `run_prodview_update()` (~280 lines) from `prodview_update_gui.py`.
- Fixed `tests/test_fetch_cda_data.py` sort-order expectation to match `_sort_cda_dataframe` tier logic.

### Impact
- **Reliability:** Connections close on exception paths; fewer hung SQL sessions.
- **Security:** Secrets no longer committed in upload script; local `settings.ini` should stay untracked after first commit of `.gitignore` update.
- **Maintainability:** Removed unused Prodview pipeline that could drift from live paths.

### Verification
- `pytest`: 196 tests passing.

### Ops notes
- Rotate Whitson `client_secret` and invalidate any JWT that was in `access_token.txt`.
- Copy `settings.ini.example` to local `settings.ini` with real values (file is gitignored).

---

## Phase 1 — Schema dedup and pipeline foundation

### Changes
- Added `pce_production_schema.py` — canonical `PCE_Production` INSERT columns, SQL builder, `df_to_insert_rows`, `batch_executemany`.
- Migrated `production_update.insert_pce_production`, `prodview_update_gui.run_quick_update`, and `sync_typecurves_to_production` to shared schema.
- Added `pce_rebuild_pipeline.run_post_production_rebuild_steps()` — unified TC sync → UWI → NGL → WM metadata → FRCST tail.
- Added `fetch_well_master_lookups(conn)` — single `PCE_WM` query for quick update.
- Accumap: `load_accumap_sales_workbook()` + single-read in `sales_ratios_gui.py`; batched `executemany` UPDATE in `merge_accumap_into_allocation_factors`.
- Tests: `test_pce_production_schema_parity.py`, `test_post_rebuild_pipeline_order.py`.

### Impact
- **Maintainability:** One source of truth for production INSERT schema; post-rebuild steps in one module.
- **Performance:** One WM query per quick update (was 3–4); one Accumap Excel read per sales-ratios run (was per month).

### Verification
- `pytest`: 196 tests passing.

---

## Phase 2 — UI responsiveness and UX

### Changes
- **Well Master:** `WellMasterLoadWorker`, `WellMasterSaveWorker`, `WellMasterSnowflakeImportWorker`, `WellMasterInsertWorker`; deferred initial load via `QTimer.singleShot`.
- **Exports:** `MonthBoundsWorker`; deferred month-bound load on open.
- **Prodview:** Deferred SQL probe on open; Close morphs to Cancel during run; full rebuild uses `log_callback` (no stdout hijack).
- **Monthly Forecasts:** `closeEvent` guard while workers run; Close disabled during operations.

### Impact
- **UX:** Well Master, Exports, and Prodview dialogs open without blocking on DB/Snowflake.
- **Reliability:** Forecasts dialog cannot close mid-import/delete.

### Verification
- `pytest` green; manual smoke: open Well Master / Exports / Prodview (<1s to interactive).

---

## Phase 3 — Architecture and documentation

### Changes
- Added `whitson_api.py` facade (credentials + `WhitsonConnection` factory).
- Added `pipelines/` package exporting `run_post_production_rebuild_steps`.
- Added `docs/DATABASE_INDEXES.md` and `scripts/recommended_indexes.sql`.
- `production_update.main()` accepts optional `log_callback` for GUI streaming.

### Impact
- **Maintainability:** Clear entry points for post-rebuild and Whitson; logging no longer depends on stdout redirection in Prodview full rebuild.

### Verification
- Full `pytest` suite.
