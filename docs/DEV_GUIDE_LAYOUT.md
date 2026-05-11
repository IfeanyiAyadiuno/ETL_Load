# Pacific Canbriam Energy - Production Update System
## Developer Guide Layout & Technical Overview

**Short overview for stakeholders and onboarding:** see [`DEV_GUIDE.md`](DEV_GUIDE.md) (non-technical, under two pages). This file is the **deep technical map** for developers and leads.

It explains **where the main update logic lives**, what each major module does, and what is important to know as a developer or maintainer.

---

## 1. INTRODUCTION

### 1.1 Purpose of this Document
- Provide a **high-level but technically accurate** overview of how the Production Update System is structured.
- Show **where all the update logic lives** (Prodview/Snowflake updates, PA monthly loader, surveys, type curves, sales ratios, well master).
- Highlight the **most important things to know as a developer**: key modules, patterns, and risks.

### 1.2 Audience
- A manager or lead who:
  - Understands basic coding concepts (functions, modules, databases).
  - Wants to know "**where is the logic?**" and "**what are the key moving pieces?**".
- Developers onboarding to the project.

### 1.3 What this System Does (Business View)
- Pulls **daily production data** from Snowflake/Prodview and loads it into SQL Server.
- Maintains a **Well Master** table with metadata for each well.
- Processes **Production Accounting (PA) allocations** from **ValNav**; **Accumap** public sales gas is applied in **Public Sales Data and Ratios** (see §4.2 and §4.5).
- Manages **Survey data** and **Type Curves** imports.
- Calculates and updates **sales ratios** and related public data.

---

## 2. ARCHITECTURE OVERVIEW (FOR MANAGERS)

### 2.1 Three Main Layers

- **1) GUI Layer (screens and buttons)**
  - All PyQt5 dialogs and windows.
  - Handles **user input, progress bars, and logs**.
  - **Never** does heavy database or Snowflake work directly.

- **2) ETL / Logic Layer (Python engines)**
  - Standalone Python modules that:
    - Talk to **Snowflake**.
    - Read **Excel** files from network drives.
    - Insert, update, and delete rows in **SQL Server**.
  - These are the "**brains**" behind each button.

- **3) Infrastructure & Config Layer**
  - Shared helpers for **database connections** and **Snowflake connections**.
  - Runtime config via `settings.ini` and `.env`.

### 2.2 Where the Code Lives (Summary)

- **Main window & navigation:** `production_update_gui.py`
- **Dialogs (UI only, no heavy logic):**
  - `prodview_update_dialog.py` – Prodview/Snowflake daily update UI.
  - `monthly_loader_dialog.py` – PA monthly loader UI.
  - `survey_import_dialog.py` – Survey import UI.
  - `type_curves_import_dialog.py` – Type curves import UI.
  - `sales_ratios_dialog.py` – Sales ratios UI.
  - `well_master_gui.py` – Well Master UI (also contains its logic).

- **Core ETL / logic modules:**
  - `prodview_update_gui.py` – **Prodview/Snowflake update logic** (**Snowflake → CDA + production rebuild** + full rebuild).
  - `monthly_loader_gui.py` – **Production Accounting monthly loader** core logic.
  - `survey_import.py` – **Survey ETL** logic.
  - `type_curves_import.py` – **Type Curves** ETL logic (`PCE_TC`). Call `append_typecurves_from_excel` / `delete_typecurves_from_tc` from this module (legacy `type.py` wrapper removed).
  - `sales_ratios_gui.py` – **Sales ratios** ETL logic.
  - `production_update.py` – Older / alternate CDA pipeline (legacy/backup).
  

- **Infrastructure & config:**
  - `db_connection.py` – SQL Server connection helper.
  - `snowflake_connector.py` – Snowflake connection + query helper.
  - `settings.ini` – GUI-configurable settings (SQL server, file paths).
  - `.env` – Snowflake credentials (account, user, password, etc.).

---

## 3. MAIN WINDOW & NAVIGATION

### 3.1 Main Entry Point
- **File:** `production_update_gui.py`
- **Function:** `main()`
- Creates the Qt application and shows `ProductionUpdateGUI`.

### 3.2 What the Main Window Does
- Shows company header and the **eight main operation buttons** (Settings is separate in the header):
  1. **📋 Well Master List** – manage well metadata.
  2. **❄️ Prodview/Snowflake Daily Production Retrieve** – daily & historical production update.
  3. **📊 Production Accounting Allocations (PA)** – monthly PA allocations.
  4. **📈 Public Sales Data and Ratios** – sales ratios update.
  5. **📐 Survey Data Import** – survey data loader.
  6. **📊 Type Curves Import** – type curve loader into **`PCE_TC`**.
  7. **Whitson+ Mass Upload** – file read / log stub.
  8. **📁 Exports / Reports** – placeholder for future reporting (last in the list).


### 3.3 How Buttons Map to Logic (Very Important)

- Each button **only opens a dialog**; it does not do the heavy work itself.
- Mapping:
  - **Well Master List** → `WellMasterDialog` in `well_master_gui.py`.
  - **Prodview/Snowflake Daily Production Retrieve** → `ProdviewUpdateDialog` in `prodview_update_dialog.py`.
  - **PA Allocations** → `MonthlyLoaderDialog` in `monthly_loader_dialog.py`.
  - **Sales Ratios** → `SalesRatiosDialog` in `sales_ratios_dialog.py`.
  - **Survey Import** → `SurveyImportDialog` in `survey_import_dialog.py`.
  - **Type Curves Import** → `TypeCurvesImportDialog` in `type_curves_import_dialog.py`.
  - **Whitson+ Mass Upload** → `WhitsonMassUploadDialog` in `whitson_mass_upload_dialog.py`.
  - **Exports / Reports** → `ExportsDialog` (UI placeholder).

> **Key takeaway for a manager:** The main window is just a **menu and log**. Each actual process lives in its own dialog file + engine file.

---

## 4. WHERE THE UPDATE LOGIC LIVES (PER FEATURE)

This section is the **most important** if you want to know "where is the code that actually changes data".

### 4.1 Prodview / Daily Production Update (Snowflake → SQL Server)

- **User path:**
  - Button: **❄️ Prodview/Snowflake Daily Production Retrieve**.
  - Dialog: `ProdviewUpdateDialog` (`prodview_update_dialog.py`).

- **Where the real logic is:**
  - **File:** `prodview_update_gui.py`
  - **Main function:** `run_quick_update(progress_callback=None, log_callback=None)` — rolling 18 months through `prodview_effective_end_date()` (today − 2).
  - **Full rebuild** is started from the same dialog but runs `production_update.main` (clears/rebuilds `PCE_Production` from all SQL Server `PCE_CDA` rows; no Snowflake call in that step).

- **What `run_quick_update` does (high-level):**
  1. Parse the month range (`"MMM YYYY"`) into actual dates.
  2. Connect to SQL Server via `get_sql_conn()` from `db_connection.py`.
  3. Pull **well mapping** from `PCE_WM` (well name, GasIDREC, PressuresIDREC, formation, layer, etc.).
  4. For each month in the range:
     - Pull **7 datasets** from Snowflake via `SnowflakeConnector` (`snowflake_connector.py`):
       - ECF, GasWH, CGR, WGR, Pressures, Allocations, Water.
     - Build a **daily spine**: every well × every day in that month.
     - Merge all Snowflake data onto the spine (pandas joins).
     - Compute derived fields like `Condensate_WH_Production`.
     - Delete existing rows for that month from `PCE_CDA` and `PCE_Production`.
     - Insert new rows into `PCE_CDA` in **batches** with `fast_executemany` turned on.
  5. Return a **summary dict** (months processed, rows inserted, duration, etc.).


> **Manager summary:** All the **Prodview/Snowflake update logic** (that your users see) is in `prodview_update_gui.py`, orchestrated by `ProdviewUpdateDialog`.

---

### 4.2 Production Accounting Monthly Loader (ValNav; Accumap → §4.5)

- **User path:**
  - Button: **📊 Production Accounting Allocations (PA)**.
  - Dialog: `MonthlyLoaderDialog` (`monthly_loader_dialog.py`).

- **Logic file:**
  - **File:** `monthly_loader_gui.py`
  - **Main entry:** `run_monthly_loader(month, valnav_path, progress_callback, log_callback)` (optional deprecated `accumap_path` ignored)

- **What it does (target split — ValNav vs Accumap):**
  1. Reads the selected **month** and builds a period.
  2. Loads the **ValNav** Excel file (path from `settings.ini`). **Accumap** is **not** part of this job; see **§4.5**.
  3. Uses `PCE_WM` for well mapping and **PCE_CDA** monthly aggregates (same pattern as today for factor denominators).
  4. Writes **ValNav-derived** columns into **`Allocation_Factors`** (e.g. `S2_Gas`, `Sales_Condensate`, `WH_to_S2_AllocFactor`, `WH_to_Sales_Cond_AllocFactor`, and related CDA rollups used to compute them). Does **not** populate **`Sales_Gas`** or **`WH_to_Sales_AllocFactor`** (those come from Accumap in Public Sales).
  5. Calls **`apply_valnav_allocation_to_cda_and_production`** in **`sales_allocation_updates.py`** (ValNav-only CDA/Production columns):
     - **`PCE_CDA`:** `[Gas - S2 Production]`, `[Condensate - Sales Production]`
     - **`PCE_Production`:** `[Gas S2 Production (10³m³)]`, `[Condensate Sales (m³/d)]` (via the existing `PCE_WM` composite-name join used in sales ratios)
  6. Does **not** in this step update **`[Gas - Sales Production]`**, **`[Gas Sales Production (10³m³)]`**, or **`[Sales CGR Ratio]`** / **`[Sales CGR (m³/e³m³)]`** — those stay for **Public Sales Data and Ratios** after Accumap-backed factors exist.

> **Manager summary:** PA monthly logic is in `monthly_loader_gui.py`; the dialog file only deals with UI and threading. **Ownership:** ValNav → PA + partial CDA/Production; Accumap → Public Sales + remaining CDA/Production sales-gas and CGR fields. Shared SQL helpers live in `sales_allocation_updates.py`.

---

### 4.3 Survey Data Import

- **User path:**
  - Button: **📐 Survey Data Import**.
  - Dialog: `SurveyImportDialog` (`survey_import_dialog.py`).

- **Logic file:**
  - **File:** `survey_import.py`
  - **Main function:** `import_surveys(excel_path, import_mode, progress_callback, log_callback)`

- **What it does:**
  - Reads an Excel survey file.
  - Normalizes/validates the data.
  - If **Overwrite**, clears existing survey data from the target table.
  - Inserts survey rows in batches with logging and progress.

> **Key risk:** Overwrite mode deletes existing survey data first – this is guarded by radio buttons and warnings.

---

### 4.4 Type Curves Import (`PCE_TC`)

- **User path:**
  - Button: **📊 Type Curves Import**.
  - Dialog: `TypeCurvesImportDialog` (`type_curves_import_dialog.py`).

- **Logic files:**
  - **`type_curves_import.py`** — **`append_typecurves_from_excel`**, **`delete_typecurves_from_tc`**, **`scan_typecurve_wells`**, **`ye2_append_rows_to_pce_tc`** (bulk YE2 path), Excel base-name rule + match to **`PCE_WM.[Well Name]`** via **`_tc_well_match_key`**, **`_tc_storage_base_name`** (longer of Excel vs WM + **` - TC`**), Vincent unit conversions, `executemany` into **`dbo.PCE_TC`**.
  - **`sync_typecurves_to_production.py`** — **`sync_tc_to_production`**: materializes **`PCE_TC`** into **`PCE_Production`** at **`ImportDate`** (Python column map only; no repo-shipped view DDL).
  - **`type_curves_import.py`** — primary API for type curve load/delete (legacy `type.py` removed).

- **What it does:**
  - Reads the type-curve workbook (**first sheet**, **row 1 = headers**), maps columns by normalized header text (vendor “Gas S1” column is stored as **Gas S2** in SQL).
  - Writes **`dbo.PCE_TC`**, then refreshes **`PCE_Production`** for TC-backed rows via **`sync_tc_to_production`** (also after Prodview **Snowflake → CDA + production rebuild** / full rebuild where applicable). Stored **`[Well Name]`** for WM-backed rows uses the **longer** of cleaned Excel vs WM **`[Well Name]`**, then **` - TC`**. File-only: names starting with **`YE2`** (covers **`YE23`**) verbatim; other file-only rows append **` - TC`**.
  - **Append:** per stored key, `DELETE` then `INSERT` for rows in scope (all rows in the file, or a user-selected subset from the scan list).
  - **Delete:** `DELETE FROM PCE_TC` for selected stored well keys (no Excel); matching **`PCE_Production`** rows for those keys are removed.

- **SQL:** DDL for **`PCE_TC`** is maintained on the server by the DBA; the app embeds insert/update SQL in Python only.

> **Manager summary:** Operators confirm scope in the dialog; unmatched file wells can produce **`unmatched_type_curve_wells_*.csv`** next to the Excel file after a run.

---

### 4.5 Sales Ratios & Public Data

- **User path:**
  - Button: **📈 Public Sales Data and Ratios**.
  - Dialog: `SalesRatiosDialog` (`sales_ratios_dialog.py`).

- **Logic files:**
  - **`sales_ratios_gui.py`** — **`run_sales_ratios_update`**: orchestrates each month (requires **`accumap_path`**).
  - **`sales_allocation_updates.py`** — **`merge_accumap_into_allocation_factors`**, **`apply_full_sales_ratios_for_month`**: Accumap merge into **`Allocation_Factors`**, then gas sales + **`Sales CGR Ratio`** on **`PCE_CDA`**, and a four-column sync from CDA to **`PCE_Production`**.

- **Accumap / public sales gas:**
  - The **Accumap** Excel (sheet **Sales Gas - to PRW**, **`PRD Monthly Mktbl GAS e3m3`**) is read **here** per calendar month; **`Sales_Gas`**, **`WH_to_Sales_AllocFactor`**, and **`Gathered_to_Sales`** are written onto existing **`Allocation_Factors`** rows for that month (PA must have created the rows first).
  - Then **`apply_full_sales_ratios_for_month`** updates only **`[Gas - Sales Production]`** (Accumap-backed **`CASE`** on **`Sales_Gas` / **`days_in_month`**) and **`[Sales CGR Ratio]`** on **`PCE_CDA`**; S2 and condensate sales on CDA remain from **PA**. It still writes all four aligned sales columns on **`PCE_Production`** from **`PCE_CDA`**.

> **Key point:** This module **does not pull from Snowflake**; it works on SQL Server **`PCE_CDA`**, **`Allocation_Factors`**, and **`PCE_Production`**. **Does not** replace PA: PA should have already written ValNav-side factors and S2/condensate sales fields on CDA/Production for the same months where applicable.

---

### 4.6 Well Master List

- **User path:**
  - Button: **📋 Well Master List**.
  - Dialog & logic: `WellMasterDialog` in **`well_master_gui.py`**.

- **What it does:**
  - Loads `PCE_WM` from SQL Server into a grid.
  - Allows editing of most fields, except key identifiers.
  - Provides search, Excel import/export, and (optionally) syncing from Snowflake.

> **Design note:** Unlike other features, **UI and logic live together** in `well_master_gui.py`.

---

## 5. INFRASTRUCTURE & CONFIG (IMPORTANT FOR RELIABILITY)

### 5.1 SQL Server Connections – `db_connection.py`

- **Function:** `get_sql_conn()`
- Central place for creating SQL Server connections.
- Uses:
  - Driver: e.g., `{ODBC Driver 17 for SQL Server}`.
  - Server & Database: from environment or defaults.
  - `Trusted_Connection=yes`.
  - A **timeout** to avoid hanging the UI.

> **Why it matters:** If you ever change servers or want to tune timeouts, you do it here.

### 5.2 Snowflake Connections – `snowflake_connector.py`

- **Class:** `SnowflakeConnector`
- Responsibilities:
  - Load `.env` from the app directory.
  - Validate that key env vars are present (`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, etc.).
  - Manage a single **reusable Snowflake connection** per process.
  - Provide `query(sql, params=None) -> pandas.DataFrame`.

> **Why it matters:** All Snowflake access goes through this; missing or wrong `.env` will show clear error messages.

### 5.3 Settings – `settings.ini`

- Edited via the **⚙️ Settings** dialog.
- Controls:
  - SQL server & database.
  - Paths for ValNav, Accumap, Survey, and Type Curves Excel files.

> **Why it matters:** Business users can change file locations **without editing code**.

---

## 6. THREADING, PROGRESS, AND LOGGING (PATTERN USED EVERYWHERE)

### 6.1 Pattern Used for Long-Running Jobs

For each big operation (Prodview update, PA loader, surveys, type curves, sales ratios):

1. **Dialog** creates a `QThread`-based worker.
2. **Worker** calls a backend function such as `run_quick_update(...)` (Snowflake → CDA + production rebuild) or `production_update.main` (full rebuild).
3. Backend functions accept **`progress_callback`** and **`log_callback`**.
4. Worker translates those callbacks into **Qt signals**:
   - `progress_signal(int)` → updates the progress bar.
   - `log_signal(str)` → appends to the results log.
   - `finished_signal(summary)` → final summary.
   - `error_signal(str)` → error popup + summary.

> **Key advantage:** The UI stays responsive and users see exactly what’s happening.

### 6.2 Logging Strategy

- **GUI logs:**
  - Main window log: high-level actions (e.g., "Opening Prodview dialog").
  - Per-dialog logs: detailed ETL steps and stats.
- **Console logs:**
  - If backend functions are run standalone (without GUI), `log_callback` is absent, and they fall back to `print()`.

---

## 7. THINGS A MANAGER/LEAD SHOULD KNOW

### 7.1 Safety & Risk Points

- **Prodview Full Rebuild:**
  - Heavy operation; can take 30–40 minutes.
  - Clears and rebuilds `PCE_Production` from all `PCE_CDA` (no Snowflake query in that step; CDA refresh is the **Snowflake → CDA + production rebuild** mode’s job).
  - Orchestrated by `ProdviewUpdateDialog` calling `production_update.main`.

- **Type Curves Import:**
  - **Append** deletes and re-inserts rows in **`PCE_TC`** per well in scope; **Delete** removes selected wells from **`PCE_TC`** only.
  - Guarded by confirmation dialogs.

- **Survey Overwrite Mode:**
  - Deletes existing survey data before import.
  - Guarded by radio button choice + warning.

### 7.2 Performance Features

- **Batch inserts** with `fast_executemany` enabled in:
  - `prodview_update_gui.py` (Prodview updates).
  - `type_curves_import.py` (type curves).
- **Rolling-window Snowflake pull** in `prodview_update_gui.py` to bound data volume.

### 7.3 How to Add New Functionality

- Recommended pattern:
  1. Create a new **backend module** with a clean function interface `run_new_thing(args, progress_callback, log_callback)`.
  2. Create a new **dialog** that collects inputs and displays logs.
  3. Add a **worker `QThread`** that calls the backend and emits signals.
  4. Wire the new dialog into `ProductionUpdateGUI` as another button.

> This keeps UI, ETL logic, and infrastructure nicely separated and testable.

---

## 8. QUICK REFERENCE TABLE (FOR MANAGERS)

Provide a **simple table** like this in the final document:

- **Prodview/Snowflake Daily Update**  
  - UI: `prodview_update_dialog.py` (`ProdviewUpdateDialog`)  
  - Logic: `prodview_update_gui.py` (`run_quick_update` = Snowflake → CDA + production rebuild; full rebuild via `production_update.main`)  
  - Data: Snowflake → `PCE_CDA`, `PCE_Production`

- **PA Monthly Loader**  
  - UI: `monthly_loader_dialog.py` (`MonthlyLoaderDialog`)  
  - Logic: `monthly_loader_gui.py` (`run_monthly_loader`)  
  - Data: ValNav → `Allocation_Factors` (ValNav columns/factors) + targeted `PCE_CDA` / `PCE_Production` updates (S2 gas, condensate sales only). Accumap → Public Sales (see Sales Ratios).

- **Survey Import**  
  - UI: `survey_import_dialog.py` (`SurveyImportDialog`)  
  - Logic: `survey_import.py` (`import_surveys`)  
  - Data: Excel → `PCE_Surveys`

- **Type Curves Import**  
  - UI: `type_curves_import_dialog.py` (`TypeCurvesImportDialog`)  
  - Logic: `type_curves_import.py` (`append_typecurves_from_excel`, `delete_typecurves_from_tc`)  
  - Data: Excel → **`PCE_TC`** (not `PCE_Production`)

- **Sales Ratios**  
  - UI: `sales_ratios_dialog.py` (`SalesRatiosDialog`)  
  - Logic: `sales_ratios_gui.py`  
  - Data: Accumap → `Allocation_Factors` (sales gas); `PCE_CDA` and `PCE_Production` recalculations (gas sales, CGR, and aligned columns)

- **Well Master**  
  - UI + Logic: `well_master_gui.py` (`WellMasterDialog` + helpers)  
  - Data: `PCE_WM`

- **Shared Infrastructure**  
  - SQL Server: `db_connection.py` (`get_sql_conn`)  
  - Snowflake: `snowflake_connector.py` (`SnowflakeConnector`)  
  - Settings: `settings.ini`  
  - Env: `.env` (Snowflake)
