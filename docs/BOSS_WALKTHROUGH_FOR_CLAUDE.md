# Boss walkthrough — reference notes (Claude)

Personal cheat sheet while walking the Production Update app: where things live and which SQL Server objects they touch. Confirm anything sensitive (hosts, credentials) with IT.

## Quick map

- **Start the app:** `production_update_gui.py` — main PyQt window.
- **Buttons** open `*_dialog.py` files; **heavy work** runs in workers, `*_gui.py`, or standalone modules (not on the UI thread).
- **Snowflake / Prodview:** `snowflake_connector.py`.
- **SQL Server:** `db_connection.py` (pyodbc, usually Windows auth).
- **Secrets / optional SQL overrides:** `.env`, read when connecting via `db_connection.py`.
- **Paths and templates:** `settings.ini`, resolved with `app_paths.py`, edited in-app via `settings_dialog.py`.

## Repo shape (why it looks “flat”)

- **`production_update_gui.py`** at repo root is the **hub**: main window, log, opens dialogs; almost no ETL SQL here.
- **`*_dialog.py`** — UI, progress, `QThread` workers that call engine code.
- **`*_gui.py`** (where present) — **heavy ETL** for that feature: Snowflake, pandas, batch SQL. Most **inline SQL** for that flow.
- **`db_connection.py` / `snowflake_connector.py`** — shared connections.
- **`app_paths.py`** — finds `settings.ini` next to the frozen exe or project root.
- **`tests/`** — pytest, mocks where needed, no GUI.

## At a glance (process → files)

- **Prodview / Snowflake:** `prodview_update_dialog.py`, `prodview_update_gui.py` — also `production_update.py` for full rebuild path.
- **PA (ValNav):** `monthly_loader_dialog.py`, `monthly_loader_gui.py` — plus `sales_allocation_updates.py` where shared with sales.
- **Public Sales:** `sales_ratios_dialog.py`, `sales_ratios_gui.py`, `sales_allocation_updates.py`.
- **Well Master:** `well_master_gui.py`, `well_master_db.py` — `well_master_cda_worker.py` for optional CDA fill for new wells.
- **Surveys:** `survey_import_dialog.py`, `survey_import.py`.
- **Type curves:** `type_curves_import_dialog.py`, `type_curves_import.py`.
- **Accumap audit (CLI):** `accumap_unmatched_cli.py` or `production_update_gui.py --accumap-unmatched` — rules in `sales_allocation_updates.py`.
- **Settings:** `settings_dialog.py` — writes `settings.ini`; no SQL for persistence.

**Where queries live (boss line):** Most SQL is **strings inside the Python file named after the feature** (`*_gui.py`, `well_master_db.py`, `survey_import.py`, etc.). Search for `INSERT INTO` / `DELETE FROM` there. Not a separate queries repo for Prodview / PA / Public Sales.

---

## Prodview / Snowflake

- **What:** Pull daily / historical production from Snowflake into SQL Server so internal data matches Prodview.
- **Main files:** `prodview_update_dialog.py`, `prodview_update_gui.py`; alternate pipeline `production_update.py` (full rebuild / legacy CDA path).
- **Where SQL lives:** Large inline strings in `prodview_update_gui.py` (`_CDA_INSERT_SQL`, `_PROD_INSERT_SQL`, deletes, inserts, sequence updates); rebuild in `production_update.py`.
- **Main tables:** `PCE_CDA`, `PCE_Production`. Column detail: `output.txt` snapshot or live DB.

## PA (ValNav) — Production Accounting

- **What:** Load ValNav Excel for a month into allocations and update ValNav-driven columns on CDA/production (S2 / condensate sales side — not Accumap gas sales).
- **Main files:** `monthly_loader_dialog.py`, `monthly_loader_gui.py`.
- **Where SQL lives:** Inline in `monthly_loader_gui.py`; shared updates via `sales_allocation_updates` (e.g. ValNav application to CDA/production).
- **Main tables:** `Allocation_Factors`; updates to `PCE_CDA`, `PCE_Production` per implementation — verify in code or DB.

## Public Sales Data and Ratios

- **What:** Load Accumap into `Allocation_Factors`, then update gas sales and sales CGR columns on CDA/production for a month range — typically **after** PA for the same months when ValNav side must be current.
- **Main files:** `sales_ratios_dialog.py`, `sales_ratios_gui.py`, `sales_allocation_updates.py`.
- **Where SQL lives:** `sales_ratios_gui.py` orchestrates; shared merge/update patterns in `sales_allocation_updates.py`.
- **Main tables:** `Allocation_Factors`, `PCE_WM`; reads/updates on `PCE_CDA`, `PCE_Production` — confirm on live data.

## Well Master

- **What:** View/edit well list, IDs, locations, flags; import new wells from Snowflake; optional worker to fill `PCE_CDA` for new keys.
- **Main files:** `well_master_gui.py` (UI), `well_master_db.py` (SQL), `well_master_cda_worker.py` (background CDA path).
- **Where SQL lives:** `well_master_db.py`. Deletes for flagged wells may involve `purge_exception_wells.py` and dependent tables — see user guide.
- **Main tables:** `PCE_WM`.

## Survey import

- **What:** Bulk or directional Excel → `PCE_Surveys`; match file well text to `PCE_WM.[Well Name]` via normalized keys in `survey_import.py`.
- **Main files:** `survey_import_dialog.py`, `survey_import.py`.
- **Where SQL lives:** `survey_import.py` (inserts, deletes, matching selects).
- **Main tables:** `PCE_Surveys`; matching uses `PCE_WM`.

## Type curves

- **What:** Excel → `PCE_TC` (WM-backed keys: longer of Excel vs WM + ` - TC`; file-only: Excel + ` - TC`); YE2 bulk script; delete-from-TC; `sync_typecurves_to_production.py` materializes into `PCE_Production` at `ImportDate`.
- **Main files:** `type_curves_import_dialog.py`, `type_curves_import.py`, `sync_typecurves_to_production.py`.
- **Where SQL lives:** Inline insert/delete in `type_curves_import.py`; sync insert/delete in `sync_typecurves_to_production.py`.
- **Main tables:** `PCE_TC`, `PCE_Production` (materialized TC rows).

## Accumap audit (CLI)

- **What:** For a calendar month, compare Accumap UWIs to `PCE_WM` without opening the main GUI.
- **Main files:** `accumap_unmatched_cli.py`; entry `production_update_gui.py --accumap-unmatched`; shared logic `sales_allocation_updates.py`.
- **Where SQL lives:** Mostly reads on SQL Server for UWI mapping; Accumap path from `settings.ini` unless overridden on CLI.
- **Main tables:** `PCE_WM` for matching.

## Whitson+ / Exports

- **What:** Placeholder UIs — minimal or no production ETL yet.

## Other CLI (same folder)

- `survey_import.py`, `cda.py`, `af.py`, `purge_exception_wells.py`, etc. — see [`README.md`](../README.md) and [`HANDOFF_INVENTORY.md`](HANDOFF_INVENTORY.md).

---

## Configuration recap

- **`.env`** — Snowflake credentials; optional `SQL_SERVER` / `SQL_DATABASE` / `SQL_DRIVER`. Not for Excel paths.
- **`settings.ini`** — Excel template paths and Settings display fields.
- **`.env.example` / `settings.ini.example`** — templates only, no secrets.

## Other docs

- [`USER_GUIDE.md`](../USER_GUIDE.md) — procedures, runbook, troubleshooting.
- [`DEV_GUIDE_LAYOUT.md`](../DEV_GUIDE_LAYOUT.md) — module map, button → file.
- [`README.md`](../README.md) — run from source, doc index.
- [`DEV_GUIDE.md`](../DEV_GUIDE.md) — short stakeholder overview.
- [`PACKAGING_WINDOWS.md`](../PACKAGING_WINDOWS.md) — PyInstaller build.

Schema snippets for principal tables appear in [`output.txt`](../output.txt); treat as non-authoritative vs production unless refreshed.

## If something is not in this file

Say it is not covered here and point to [`USER_GUIDE.md`](../USER_GUIDE.md), [`DEV_GUIDE_LAYOUT.md`](../DEV_GUIDE_LAYOUT.md), or the specific `*_gui.py` / `*_dialog.py` named above.
