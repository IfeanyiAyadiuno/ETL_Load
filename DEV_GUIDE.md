# Production Update System — Developer overview

**Organization:** Pacific Canbriam Energy LTD  
**Audience:** Stakeholders and new team members who need the big picture without reading all implementation code.  
**For technical depth:** see [DEV_GUIDE_LAYOUT.md](DEV_GUIDE_LAYOUT.md). **For day-to-day operations:** see [USER_GUIDE.md](USER_GUIDE.md).

---

## What this system does

The **Production Update System** is a Windows desktop app that keeps SQL Server production tables in step with **Snowflake (Prodview)** daily data, **Well Master** metadata, and monthly **ValNav** and **Accumap** allocation files. Staff use a single main window to open focused dialogs (each with its own progress and log). The goal is repeatable, documented updates instead of one-off spreadsheets or ad hoc SQL.

---

## Main components

- **Main window** — Launch operations, show the operation log.
- **Settings** — SQL server display names; paths to ValNav, Accumap, Survey, Type Curves, and Whitson+ templates (saved in `settings.ini`).
- **Well Master** — Edit well metadata; import new wells from Snowflake; stage completions. **Tables:** `PCE_WM`.
- **Prodview / Snowflake** — Pull daily meter data (**Snowflake → CDA + production rebuild**, default) or **full rebuild** from CDA. **Tables:** `PCE_CDA`, `PCE_Production`.
- **PA (Production Accounting)** — Load ValNav month; update allocation factors and ValNav-driven columns on CDA/production. **Tables:** `Allocation_Factors`, `PCE_CDA`, `PCE_Production`.
- **Public Sales** — Load Accumap; update public gas sales and sales CGR columns. **Tables:** `Allocation_Factors`, `PCE_CDA`, `PCE_Production`.
- **Survey import** — Load survey rows from Excel/CSV (bulk or mapped layout). **Tables:** `PCE_Surveys`.
- **Type curves** — Load or delete type-curve rows from Excel. **Tables:** `PCE_TC`.
- **Whitson+** — Placeholder workflow for future mass upload.
- **Exports / Reports** — Placeholder for future extracts.

Typical sequence for a month: **Well Master** (keys correct) → **Prodview** (daily spine current) → **PA** when ValNav is ready → **Public Sales** when Accumap is ready. Details and risks (destructive steps, VPN) are in the **User Guide**.

---

## How to run it

**From source (developers):** Install Python 3.10+, open the application folder on your PC or internal share, `pip install -r requirements.txt`, copy `.env.example` to `.env` and fill Snowflake settings, then `python production_update_gui.py`. See [README.md](README.md).

**Deployed build (analysts):** Use the packaged folder from IT or engineering: `.exe`, `_internal`, `.env`, and `settings.ini` beside the executable. See [COWORKER_SETUP.md](COWORKER_SETUP.md) for ODBC, VPN, and Windows auth expectations.

**Building an installer:** See [PACKAGING_WINDOWS.md](PACKAGING_WINDOWS.md).

---

## Where to find help

- **Step-by-step operations, runbook, glossary** — [USER_GUIDE.md](USER_GUIDE.md)
- **SQL module map, file-level responsibilities** — [DEV_GUIDE_LAYOUT.md](DEV_GUIDE_LAYOUT.md)
- **Logical schema / Mermaid overview** — [APPLICATION_ARCHITECTURE.md](APPLICATION_ARCHITECTURE.md)
- **Packaged app prerequisites** — [COWORKER_SETUP.md](COWORKER_SETUP.md)
- **Module list, CLI scripts, what is legacy** — [docs/HANDOFF_INVENTORY.md](docs/HANDOFF_INVENTORY.md)
- **IT / access / escalation** — Use your internal service desk or data-owner contacts (fill in per org).

---

## File structure (high level)

- **`production_update_gui.py`** — application entry and main window.
- **`*_dialog.py`** — PyQt UI for each operation; workers often live in matching **`*_gui.py`** files.
- **`db_connection.py`**, **`snowflake_connector.py`**, **`app_paths.py`** — connections and paths.
- **`well_master_*.py`**, **`survey_import.py`**, **`type_curves_import.py`**, **`sales_allocation_updates.py`** — domain logic.
- **`tests/`** — automated tests (`pytest`).

---

## Turning this document into Word

Use the same workflow as the User Guide: attach this Markdown to your Word-generation process (e.g. the prompt block under **“Copy-paste: ChatGPT instructions for Word”** in [USER_GUIDE.md](USER_GUIDE.md)), substituting “Developer overview” where it says “User Guide”.
