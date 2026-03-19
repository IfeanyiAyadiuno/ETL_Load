# Pacific Canbriam Energy - Production Update System
## Developer Guide Layout & Technical Overview

This document is a **layout/template** for a developer-focused guide, similar in spirit to `USER_GUIDE_LAYOUT.md` but aimed at a semi-technical manager or lead. It explains **where the main update logic lives**, what each major module does, and what is important to know as a developer.

You can paste this into ChatGPT with a prompt such as:
> "Turn this layout into a polished developer guide, keeping the structure, but smoothing the language for a semi-technical manager who wants to understand where the logic lives and what’s important for maintenance."

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
- Processes **Production Accounting (PA) allocations** from ValNav and Accumap spreadsheets.
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
  - `prodview_update_gui.py` – **Prodview/Snowflake update logic** (quick update + full rebuild).
  - `monthly_loader_gui.py` – **Production Accounting monthly loader** core logic.
  - `survey_import.py` – **Survey ETL** logic.
  - `type.py` – **Type Curves** ETL logic.
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
- Shows company header and the **seven main buttons**:
  1. **📋 Well Master List** – manage well metadata.
  2. **❄️ Prodview/Snowflake Daily Production Retrieve** – daily & historical production update.
  3. **📊 Production Accounting Allocations (PA)** – monthly PA allocations.
  4. **📈 Public Sales Data and Ratios** – sales ratios update.
  5. **📐 Survey Data Import** – survey data loader.
  6. **📊 Type Curves Import** – type curve loader (YE2 wells).
  7. **📁 Exports / Reports** – placeholder for future reporting.


### 3.3 How Buttons Map to Logic (Very Important)

- Each button **only opens a dialog**; it does not do the heavy work itself.
- Mapping:
  - **Well Master List** → `WellMasterDialog` in `well_master_gui.py`.
  - **Prodview/Snowflake Daily Production Retrieve** → `ProdviewUpdateDialog` in `prodview_update_dialog.py`.
  - **PA Allocations** → `MonthlyLoaderDialog` in `monthly_loader_dialog.py`.
  - **Sales Ratios** → `SalesRatiosDialog` in `sales_ratios_dialog.py`.
  - **Survey Import` → `SurveyImportDialog` in `survey_import_dialog.py`.
  - **Type Curves Import** → `TypeCurvesImportDialog` in `type_curves_import_dialog.py`.
  - **Exports / Reports` → `ExportsDialog` (UI placeholder).

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
  - **Main function:** `run_quick_update(start_month, end_month, progress_callback=None, log_callback=None)`
  - There is also a **full rebuild** path in the same file (long-running, clears/rebuilds more history).

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

### 4.2 Production Accounting Monthly Loader (ValNav & Accumap)

- **User path:**
  - Button: **📊 Production Accounting Allocations (PA)**.
  - Dialog: `MonthlyLoaderDialog` (`monthly_loader_dialog.py`).

- **Logic file:**
  - **File:** `monthly_loader_gui.py`
  - **Main entry:** `run_monthly_loader(month, valnav_path, accumap_path, progress_callback, log_callback)`

- **What it does:**
  1. Reads the selected **month** and builds a period.
  2. Loads **ValNav** and **Accumap** Excel files (paths from `settings.ini`).
  3. Uses `PCE_WM` for well mapping.
  4. Computes allocation factors, matches wells, fills gaps.
  5. Writes allocation data back to SQL Server tables in batches.

> **Manager summary:** PA monthly logic is in `monthly_loader_gui.py`; the dialog file only deals with UI and threading.

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

### 4.4 Type Curves Import (YE2 wells)

- **User path:**
  - Button: **📊 Type Curves Import**.
  - Dialog: `TypeCurvesImportDialog` (`type_curves_import_dialog.py`).

- **Logic file:**
  - **File:** `type.py`
  - **Main function:** `import_typecurves(excel_path, progress_callback=None, log_callback=None)`

- **What it does:**
  - Reads type curves Excel file.
  - Connects to SQL Server.
  - **Deletes** all rows in `PCE_Production` where Well Name starts with `"YE2"`.
  - Inserts new YE2 rows from Excel in batches.

> **Manager summary:** This is a **destructive but controlled** operation; the dialog shows a clear warning before running. The logic is isolated in `type.py`.

---

### 4.5 Sales Ratios & Public Data

- **User path:**
  - Button: **📈 Public Sales Data and Ratios**.
  - Dialog: `SalesRatiosDialog` (`sales_ratios_dialog.py`).

- **Logic file:**
  - **File:** `sales_ratios_gui.py`
  - Contains functions that:
  - For a selected month range, read relevant records from `PCE_CDA`.
  - Recalculate sales-related fields and ratios.
  - Update **both** `PCE_CDA` (daily-level fields) **and** `PCE_Production` (monthly summary fields) in batches.
    
> **Key point:** This module **does not pull from Snowflake**; it works on existing SQL Server data in `PCE_CDA` and pushes matching fields into `PCE_Production`.

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
2. **Worker** calls a backend function like `run_quick_update(...)`.
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
  - Clears and rebuilds a large amount of data.
  - Fully controlled through `prodview_update_gui.py` and `ProdviewUpdateDialog`.

- **Type Curves Import:**
  - Explicitly deletes **all `YE2%` wells** from `PCE_Production` before inserting.
  - Guarded by a clear warning dialog.

- **Survey Overwrite Mode:**
  - Deletes existing survey data before import.
  - Guarded by radio button choice + warning.

### 7.2 Performance Features

- **Batch inserts** with `fast_executemany` enabled in:
  - `prodview_update_gui.py` (Prodview updates).
  - `type.py` (type curves).
- **Per-month processing** for quick updates to keep data volumes smaller.

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
  - Logic: `prodview_update_gui.py` (`run_quick_update`, full rebuild path)  
  - Data: Snowflake → `PCE_CDA`, `PCE_Production`

- **PA Monthly Loader**  
  - UI: `monthly_loader_dialog.py` (`MonthlyLoaderDialog`)  
  - Logic: `monthly_loader_gui.py` (`run_monthly_loader`)  
  - Data: ValNav + Accumap → PA tables

- **Survey Import**  
  - UI: `survey_import_dialog.py` (`SurveyImportDialog`)  
  - Logic: `survey_import.py` (`import_surveys`)  
  - Data: Excel → `PCE_Surveys`

- **Type Curves Import**  
  - UI: `type_curves_import_dialog.py` (`TypeCurvesImportDialog`)  
  - Logic: `type.py` (`import_typecurves`)  
  - Data: Excel → `PCE_Production` (YE2 wells)

- **Sales Ratios**  
  - UI: `sales_ratios_dialog.py` (`SalesRatiosDialog`)  
  - Logic: `sales_ratios_gui.py`  
  - Data: `PCE_CDA` and `PCE_Production`recalculations

- **Well Master**  
  - UI + Logic: `well_master_gui.py` (`WellMasterDialog` + helpers)  
  - Data: `PCE_WM`

- **Shared Infrastructure**  
  - SQL Server: `db_connection.py` (`get_sql_conn`)  
  - Snowflake: `snowflake_connector.py` (`SnowflakeConnector`)  
  - Settings: `settings.ini`  
  - Env: `.env` (Snowflake)

---

## 9. NOTES FOR CHATGPT FORMATTING

- Turn this into a **clean, manager-readable developer overview**:
  - Keep section headings and ordering.
  - Use plain language but keep the module/function names accurate.
  - No screenshots or images needed.
- Emphasize:
  - Which module/file owns each piece of logic.
  - Where destructive operations happen.
  - The separation between UI and ETL logic.

