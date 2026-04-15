# Production Update System — User Guide

**Organization:** Pacific Canbriam Energy LTD  
**Document version:** 1.1  
**Last updated:** April 8, 2026  

## Document purpose and scope

This User Guide describes the **Production Update System** (desktop application) used within Pacific Canbriam Energy LTD. It documents prerequisites, main-window operations, module-specific procedures, the principal SQL Server tables and views, a maintenance runbook, and logical schema diagrams. The intended audience is staff authorized to run production updates, allocations, and related imports.

---

## Table of contents

1. [Before you start](#before-you-start)  
2. [How the pieces fit together](#how-the-pieces-fit-together)  
3. [Application architecture and data flow](#application-architecture-and-data-flow)  
4. [Main window](#main-window)  
5. [Settings](#settings)  
6. [Well Master List](#well-master-list)  
7. [Prodview / Snowflake — Daily Production Retrieve](#prodview--snowflake--daily-production-retrieve)  
8. [Production Accounting Allocations (PA)](#production-accounting-allocations-pa)  
9. [Public Sales Data and Ratios](#public-sales-data-and-ratios)  
10. [Survey Data Import](#survey-data-import)  
11. [Type Curves Import](#type-curves-import)  
12. [Whitson+ Mass Upload](#whitson-mass-upload)  
13. [Exports / Reports](#exports--reports)  
14. [Operational considerations](#operational-considerations)  
15. [Runbook — script order and maintenance](#runbook--script-order-and-maintenance)  
16. [Troubleshooting](#troubleshooting)  
17. [Appendix A — Figures checklist](#appendix-a--figures-checklist-for-screenshots)  
18. [Appendix B — Glossary](#appendix-b--glossary)  
19. [Appendix C — Logical database schema (SQL Server)](#appendix-c--logical-database-schema-sql-server)  
20. [Copy-paste: ChatGPT instructions for Word (.docx)](#copy-paste-chatgpt-instructions-for-word-docx)  

---

## Before you start

### Prerequisites

- Network connectivity to the company **SQL Server** instance and, for Prodview and Snowflake well import, to **Snowflake**.  
- **Windows** host; SQL Server access uses **Windows authentication** unless otherwise configured.  
- **`.env`** in the application directory (or project root when running from source), containing Snowflake and any server/database overrides. Connection defaults may reference environment variables such as `SQL_SERVER` and `SQL_DATABASE` (confirm values with IT).  
- **`settings.ini`** adjacent to the executable or project folder. File paths and SQL connection fields are written when **Settings** is saved.

### Execution

- **Deployed build:** Run the packaged executable (for example `ProductionUpdate.exe`) from a directory that includes `.env` and `settings.ini` where applicable.  
- **From source (repo root):** Install runtime dependencies, then start the main window:

```text
cd /d I:\ETL_Load
python -m pip install -r requirements.txt
python production_update_gui.py
```

In **PowerShell**, use `cd I:\ETL_Load` (no `/d`). Use your actual project folder in place of `I:\ETL_Load` if different.

- **Accumap UWI audit (terminal only, no GUI window):** Uses the same Accumap path rules as Public Sales (`settings.ini` **Accumap Template** unless you pass `-a`). Example:

```text
python production_update_gui.py --accumap-unmatched --month "Aug 2025"
python production_update_gui.py --accumap-unmatched -m "Aug 2025" -a "I:\path\Accumap.xlsx"
```

Optional CSV output: add `-o "I:\path\accumap_audit.csv"` (see `accumap_unmatched_cli.py`).

Credential, VPN, and ODBC driver requirements are managed by IT; obtain confirmation before production use.

### Automated tests (developers)

From the repo root, install dev dependencies and run **pytest** (no GUI; uses mocks where applicable):

```text
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

---

## How the pieces fit together

### Data objects (summary)

| Object | Role |
|--------|------|
| `PCE_WM` | Well master: well list and linkage to Snowflake identifiers (**GasIDREC**, **PressuresIDREC**). Wells absent or excluded here may be omitted from daily loads. |
| `PCE_CDA` | Daily-style production rows sourced from Snowflake (and related processing). |
| `PCE_Production` | Production history for reporting; sequences, cumulatives, and averages derived from CDA and allocation passes. |
| `PCE_TC` | Type-curve metrics from the **Type Curves** Excel import only. Stored **`[Well Name]`** is **`PCE_WM.[Well Name]`** + literal **` - TC`** (physical well key, not composite), so rows stay distinct from **`PCE_Production`**. |
| `PCE_Surveys` | Survey stations and geometry loaded from Excel or CSV; keyed by **`SurveyID`** with **`UWI`**, **`[Well Name]`**, and optional **`Latitude`** / **`Longitude`** (decimal), plus legacy-style offset columns as applicable. |
| `Allocation_Factors` | Monthly allocation inputs: **ValNav**-sourced fields are written by **PA**; **Accumap**-sourced sales gas fields are written by **Public Sales Data and Ratios** (see below). |

**Reporting views (read-only in normal operations):** The database may expose views such as **`dbo.vw_PCE_Production_with_TypeCurves`**, **`dbo.vw_PCE_TC_with_Production_Well`**, and **`vw_PCE_WM_Ordered`** (schema may differ from `dbo`). The desktop app does not treat these as write targets during routine operations; they exist for reporting and joins. Deploy or refresh their definitions from the `sql/` scripts in this repository when IT approves.

**Production Accounting Allocations (PA)** loads **ValNav** data into **`Allocation_Factors`**, then updates **`PCE_CDA`** and **`PCE_Production`** only for **S2 gas** and **condensate sales** (the same columns that depend on ValNav-based factors), via **`monthly_loader_gui.py`** calling **`sales_allocation_updates.apply_valnav_allocation_to_cda_and_production`**.

**Public Sales Data and Ratios** loads **Accumap** (public sales gas) into **`Allocation_Factors`**, then updates **`PCE_CDA`** and **`PCE_Production`** for **gas sales production** and **sales CGR**, using **`sales_ratios_gui.py`** and **`sales_allocation_updates`**.

### Reference sequence

1. Maintain **Well Master** (including new wells from Snowflake where required).  
2. Execute **Prodview / Snowflake** per the agreed refresh schedule.  
3. Run **PA** when the monthly **ValNav** file is ready (PA applies ValNav to **`Allocation_Factors`** and refreshes **S2** and **condensate sales** on **`PCE_CDA`** / **`PCE_Production`**).  
4. Run **Public Sales Data and Ratios** when the **Accumap** file is ready and you need **gas sales** and **sales CGR** updated for a month range (it applies Accumap to **`Allocation_Factors`** and updates the remaining sales fields on **`PCE_CDA`** / **`PCE_Production`**).

**Why this order:** Daily wellhead-style rows should already exist in **`PCE_CDA`** from Prodview. **PA** does not replace **Public Sales**—each module owns a different subset of **`Allocation_Factors`** and of the calculated sales columns. The Public Sales dialog may warn you if factors or CDA rows are missing for the selected range.

**Sales Ratios cancel:** If you cancel during a run, the app stops **between months**; completed months stay committed.

---

## Application architecture and data flow

The diagram below is a **logical** map of the Python entry point, dialogs, backend modules, external systems, and SQL Server tables the application reads and writes during normal use. For **script order**, **destructive steps**, and **verification**, see [Runbook — script order and maintenance](#runbook--script-order-and-maintenance).

For a **single-image** architecture overview (one Mermaid figure plus short notes), see **[`APPLICATION_ARCHITECTURE.md`](APPLICATION_ARCHITECTURE.md)**.

```mermaid
flowchart TB
  subgraph entryLayer["Entry"]
    exec_py["production_update_gui.py"]
  end
  exec_py --> fork{"argv[1]?"}
  fork -->|"default"| qt["PyQt5 QApplication MainWindow"]
  fork -->|"--accumap-unmatched"| acc_cli["accumap_unmatched_cli.py"]
  acc_cli --> xl_acc["Accumap Excel read"]
  acc_cli --> sql_acc["SQL Server read PCE_WM"]

  subgraph gui_dlgs["Dialogs"]
    d_set["settings_dialog.py"]
    d_wm["well_master_gui.py"]
    d_pv["prodview_update_dialog.py"]
    d_pa["monthly_loader_dialog.py"]
    d_pub["sales_ratios_dialog.py"]
    d_sur["survey_import_dialog.py"]
    d_tc["type_curves_import_dialog.py"]
    d_whi["whitson_mass_upload_dialog.py"]
    d_exp["exports_dialog.py"]
  end

  subgraph backend["Backend modules"]
    m_db["db_connection.py"]
    m_pv["prodview_update_gui.py"]
    m_pu["production_update.py"]
    m_wmjob["well_master_cda_worker.py"]
    m_pa["monthly_loader_gui.py"]
    m_pubg["sales_ratios_gui.py"]
    m_salloc["sales_allocation_updates.py"]
    m_sur["survey_import.py"]
    m_tc["type_curves_import.py"]
  end

  subgraph ext["External systems"]
    sf["Snowflake"]
    xl["Excel CSV paths from settings.ini"]
  end

  subgraph dbo["SQL Server dbo"]
    TWM["PCE_WM"]
    TCDA["PCE_CDA"]
    TPR["PCE_Production"]
    TAF["Allocation_Factors"]
    TSV["PCE_Surveys"]
    TTC["PCE_TC"]
  end

  qt --> gui_dlgs

  d_set --> m_db
  d_wm --> m_wmjob
  d_pv --> m_pv
  d_pv -->|"Full rebuild"| m_pu
  d_pa --> m_pa
  d_pub --> m_pubg
  d_sur --> m_sur
  d_tc --> m_tc
  d_whi --> wh_stub["File read log only STUB"]
  d_exp --> exp_stub["No DB writes Coming soon"]

  m_db --> TWM
  m_wmjob --> sf
  m_wmjob --> TWM
  m_wmjob --> TCDA
  m_pv --> sf
  m_pv -->|"Quick Update"| TCDA
  m_pv --> TPR
  m_pu --> TAF
  m_pu --> TCDA
  m_pu --> TPR
  m_pa --> xl
  m_pa --> TAF
  m_pa --> m_salloc
  m_salloc --> TCDA
  m_salloc --> TPR
  m_pubg --> xl
  m_pubg --> TAF
  m_pubg --> m_salloc
  m_sur --> xl
  m_sur --> TWM
  m_sur --> TSV
  m_tc --> xl
  m_tc --> TTC
```

**Legend (behavior the diagram summarizes):**

- **Prodview Quick Update** (`prodview_update_gui.run_quick_update`): pulls **Snowflake** for the selected **From**–**To** months; **deletes** **`PCE_CDA`** and **`PCE_Production`** rows whose dates fall in that range; inserts replacement **`PCE_CDA`** rows; then **reads the entire **`PCE_CDA`** table**, recomputes sequences, cumulatives, and monthly averages in Python, **deletes all **`PCE_Production`** rows for every well name** present in that rebuilt dataset, and bulk re-inserts production for those wells. Effects on production therefore extend to **full well history** for those wells, not only the selected calendar months.  
- **Prodview Full Rebuild** (`production_update.main`): **does not** call Snowflake. It first repaints selected sales-related columns on **`PCE_CDA`** from **`Allocation_Factors`** (when allocation rows exist), then **deletes all rows in **`PCE_Production`**** and rebuilds production from **all** of **`PCE_CDA`**.  
- **`run_prodview_update`** in `prodview_update_gui.py` (range-based Snowflake + SQL insert path) exists in code but is **not** invoked from the current Prodview dialog; the dialog uses **Quick Update** and **Full Rebuild** only.

---

## Main window

Application title: **Pacific Canbriam Energy - Production Update System**. Header: **Pacific Canbriam Energy LTD** / **Production Update System**. **Settings** is located in the top-right of the header.

**Operations** exposes eight modules (each opens a modal dialog):

| # | Control label |
|---|----------------|
| 1 | Well Master List |
| 2 | Prodview / Snowflake — Daily Production Retrieve |
| 3 | Production Accounting Allocations (PA) |
| 4 | Public Sales Data and Ratios |
| 5 | Survey Data Import |
| 6 | Type Curves Import |
| 7 | Whitson+ Mass Upload |
| 8 | Exports / Reports |

Operation buttons are mutually exclusive (single selection). Closing a dialog clears the selection.

The **Operation log** records timestamped lines (`[HH:MM:SS] …`). Status text (for example **Ready**) appears below the log.

`[IMAGE: Main window — header, Settings, all eight operation buttons, operation log]`

> **Figure 1 — Main window**  
> *[Insert screenshot: full main window including header, Operations list with all eight buttons, Operation log. Red arrow: Settings. Red arrow: Operations card. Red arrow: Operation log panel.]*

<!-- Image: assets/user-guide/figure-01-main-window.png -->

![Figure 1 — Main window](images/figure-01-main-window.png)

---

## Settings

**Settings** configures SQL Server display fields and default file paths.

- **SQL Server Connection:** Server and Database (persisted in `settings.ini`; `db_connection` may also read `SQL_SERVER` / `SQL_DATABASE` from `.env`).  
- **Paths** (each **Browse**): **ValNav Template**, **Accumap Template**, **Survey File**, **Type Curves File**, **Whitson+ File**.

**Save Settings** writes `settings.ini`. **Cancel** discards changes.

PA uses **ValNav Template**; **Public Sales Data and Ratios** uses **Accumap Template** for public sales gas. Survey, Type Curves, and Whitson read their paths as defaults; incorrect or missing paths produce file-not-found messages in the respective dialogs.

`[IMAGE: Settings dialog — SQL Server fields, path rows, Save Settings]`

> **Figure 2 — Settings**  
> *[Insert screenshot: Settings dialog showing SQL Server fields and all file path rows with Browse. Red arrow: Save Settings. Red arrow: ValNav / Accumap paths.]*

<!-- Image: assets/user-guide/figure-02-settings.png -->

![Figure 2 — Settings](images/figure-02-settings.png)

---

## Well Master List

**Objective:** Maintain `PCE_WM`: view and edit permitted attributes, stage additions, Excel import/export, **Import New Wells** from Snowflake, and removal of selected wells.

**View/Edit:** Search, column edits where enabled, **Refresh**, **Save Changes**, Excel operations, **Update from Snowflake**, **Remove Selected**. Identifier fields (for example Well Name, GasIDREC, PressuresIDREC) are fixed after creation.

**Add New:** Stage rows; **Save to Database** commits staged data.

### Import New Wells (Snowflake)

**Preview New Wells** lists candidate wells. Uncheck rows to exclude them; only checked rows are inserted. Confirm selection, then **Add Wells**.

**Caution (Snowflake `*` and tester-only wells):** Leading **asterisks (`*`)** on well or unit names in the preview come from Snowflake’s naming convention; they are **not kept as part of the stored Well Name**—the application **strips leading asterisks** when you confirm **Add Wells** (including after you edit the name in the preview table). If Snowflake returns wells that exist **only as Tester records** (no Daily meter yet), the app first opens **New Wells – GasIDREC Required**: you must **enter the correct GasIDREC** from ProdView for each row you keep (**PressuresIDREC** is shown read-only from Snowflake on that screen); after you confirm or skip that step, **Preview New Wells** and **Add Wells** work as described above.

### Post-import behavior

The application queues a background job (`well_master_cda_worker`) to load daily data into **`PCE_CDA`** for inserted wells (historical window per application logic, typically from 2009 through the current date). A progress indicator is shown during this task.

**Caution:** That job updates **`PCE_CDA` only**. It does **not** rebuild **`PCE_Production`**. For production refresh (sequences, cumulatives, and so on), run **Prodview / Snowflake** in **Quick Update** for the required months, or coordinate a **Full Rebuild** with the responsible team.

`[IMAGE: Well Master View/Edit tab — table, Save Changes, Import New Wells]`

> **Figure 3 — Well Master View/Edit**  
> *[Insert screenshot: Well Master dialog, View/Edit tab, search and table. Red arrow: Save Changes. Red arrow: Import New Wells (Snowflake).]*

<!-- Image: assets/user-guide/figure-03-well-master-view.png -->

![Figure 3 — Well Master View/Edit](images/figure-03-well-master-view.png)

`[IMAGE: Preview New Wells — checkboxes and Add Wells]`

> **Figure 4 — Preview New Wells**  
> *[Insert screenshot: Preview New Wells with checkbox column, Well Name / IDs, Add Wells. Red arrow: checkbox column. Red arrow: Add Wells.]*

<!-- Image: assets/user-guide/figure-04-preview-new-wells.png -->

![Figure 4 — Preview New Wells](images/figure-04-preview-new-wells.png)

---

## Prodview / Snowflake — Daily Production Retrieve

**Objective:** Retrieve daily production-related data from Snowflake and update `PCE_CDA` and `PCE_Production`.

### Update range (Quick Update)

- **From:** Approximately the last **36** calendar months, listed **oldest first**. Default selection is the **oldest** month; adjust for the required start month.  
- **To:** **Current calendar month** only (single combo entry). **From**–**To** defines the Snowflake pull and the initial **date-range** replacement on **`PCE_CDA`** / **`PCE_Production`**.

### Update mode

**Quick Update Mode** (default selection in the dialog)

1. Pulls **Snowflake** for every calendar day from **From** through **To** (inclusive month boundaries).  
2. **Deletes** existing **`PCE_CDA`** rows with **`ProdDate`** in that span and **deletes** existing **`PCE_Production`** rows with **`Date`** in that span, then inserts the merged daily rows into **`PCE_CDA`**.  
3. Loads **all** rows from **`PCE_CDA`** into memory, applies well-name mapping and first-production filtering, then recomputes sequences, cumulatives, monthly averages, and on-production year in Python.  
4. **Deletes every row in **`PCE_Production`** for each well name** present in that rebuilt dataset (all history for those wells), then bulk re-inserts production for the same wells from the recalculated dataframe.

**Caution:** Step 4 means Quick Update can change **entire production histories** for wells in scope, not only the months shown in **From**–**To**. Avoid cancelling mid-run; the confirmation dialog warns that partial commits are possible.

**Full Rebuild Mode**

1. Repaints Gas S2, gas sales, condensate sales, and Sales CGR on **`PCE_CDA`** from **`Allocation_Factors`** for every distinct allocation month (when allocation rows exist).  
2. **Deletes all rows** in **`PCE_Production`** and rebuilds the table from **all** rows currently in **`PCE_CDA`** (sequences, cumulatives, monthly averages).  
3. **Does not** query Snowflake in this step; it assumes **`PCE_CDA`** already reflects the desired Snowflake history (typically after one or more Quick Updates).

Extended runtime (the dialog notes on the order of tens of minutes for a large database). **From/To** selectors do **not** apply; **Update Range** is disabled when Full rebuild is selected.

Select **Run Update**, acknowledge the confirmation, monitor **Results** and the progress bar (indeterminate during Full rebuild with a heartbeat status). **Close** exits the dialog; if a job is running, the app may warn that cancellation is best-effort and that Quick Update can leave partial commits.

`[IMAGE: Prodview dialog — Quick vs Full, From/To, Results, Run Update]`

> **Figure 5 — Prodview dialog**  
> *[Insert screenshot: Prodview dialog with Update Range, Update Mode radios, Results, Run Update. Red arrow: Full vs Quick. Red arrow: From/To. Red arrow: Run Update.]*

<!-- Image: assets/user-guide/figure-05-prodview.png -->

![Figure 5 — Prodview dialog](images/figure-05-prodview.png)

---

## Production Accounting Allocations (PA)

**Objective:** For the selected month, write **ValNav**-based rows and columns into **`Allocation_Factors`**, then update **`PCE_CDA`** and **`PCE_Production`** for **S2 gas** and **condensate sales** only, via **`sales_allocation_updates.apply_valnav_allocation_to_cda_and_production`**.

**Prerequisites:** **ValNav Template** set in **Settings**; ValNav source file must exist for the target month.

**Accumap** is **not** part of this step: public **sales gas** and **sales CGR** are handled when you run **Public Sales Data and Ratios** using the **Accumap Template**.

**Month control:** Dropdown contains roughly the **last 24** calendar months, **oldest first**. Select the month that matches the files being loaded (first list item is the oldest in the window).

**Procedure**

1. Open **Production Accounting Allocations (PA)**.  
2. Select **Month**.  
3. Verify status: database and **ValNav file**. (Accumap path is shown for reference; PA does not read it.)  
4. **Run Monthly Loader**; confirm the dialog.  
5. Review **Results** (counts, warnings, elapsed time).

`[IMAGE: PA dialog — month, paths, Run Monthly Loader, Results]`

> **Figure 6 — PA allocations**  
> *[Insert screenshot: PA dialog with month, ValNav path, Accumap path, status lines, Results. Red arrow: Month. Red arrow: Run Monthly Loader.]*

<!-- Image: assets/user-guide/figure-06-pa-allocations.png -->

![Figure 6 — PA allocations](images/figure-06-pa-allocations.png)

---

## Public Sales Data and Ratios

**Objective:** Apply **Accumap** (public sales gas) to **`Allocation_Factors`**, then update **`PCE_CDA`** and **`PCE_Production`** for **gas sales production** and **sales CGR** (and aligned columns computed in the same pass). **S2 gas** and **condensate sales** should already be current from **PA** for months where you ran ValNav first.

**Prerequisites:** **Accumap Template** in **Settings**; run **PA** first for months where ValNav-based allocation and S2/condensate sales fields need to be current.

**Month range:** **From** and **To** list every month from **January 2008** through the **current calendar month**, oldest first. Default **From** is **Jan 2008**; default **To** is the **current month** (full history through the current month unless changed).

**Run Update** → confirm → monitor the log.

`[IMAGE: Public Sales dialog — From, To, Run Update, log]`

> **Figure 7 — Public Sales Data and Ratios**  
> *[Insert screenshot: dialog with From/To combos, info panel, Run Update. Red arrow: From. Red arrow: To. Red arrow: Run Update.]*

<!-- Image: assets/user-guide/figure-07-public-sales.png -->

![Figure 7 — Public Sales Data and Ratios](images/figure-07-public-sales.png)

---

## Survey Data Import

**Objective:** Load survey rows from Excel into **`PCE_Surveys`** (including **`Latitude`** and **`Longitude`** when mapped or present in the file, per column mapping rules in `survey_import.py`).

There are **two import paths** in the Survey dialog:

1. **Bulk (Settings path)** — Uses **Survey File** from **Settings** (read-only path in the dialog). This is the **legacy** flat-table flow: the first row is headers; columns are matched by name (matching is **case-insensitive** after normalization). The file may be **Excel** (`.xlsx` / `.xls`) or **comma-separated** `.csv` (Excel-style export). Change **Settings** to repoint the file.

2. **Directional / mapped import** — Choose this mode to **Browse** to any `.xlsx` / `.xls` / `.csv` file whose layout varies by vendor. CSV is treated as a **single sheet** (no sheet picker). Click **Configure mapping…** to open a **second dialog**: pick the sheet, set the **header row** and **first data row**, choose the **well name** cell, and map file columns to survey fields (**Measured Depth** is required). **UWI** and **Pad** are **not** taken from the Excel file; after the well name is read, the app looks up **one** row in **`PCE_WM`** by matching the cell text to **`[Well Name]`** (same field used across Well Master and production) and uses **`[Value Navigator UWI]`** and **`[Pad Name]`** for every survey row. You can **Load/Save** mapping presets (JSON) for repeat layouts.

**Import Mode (GUI):** **Append Mode** (insert rows not already present) or **Overwrite Mode** (delete existing rows for matching UWIs, then insert). Applies to both paths.

**Command-line** `survey_import.py` also supports a third mode, **`merge`**, which is **not** exposed as a radio button in the Survey dialog.

Execute **Run Import**; review the **Import Log**.

`[IMAGE: Survey dialog — path, Append/Overwrite, Run Import, log]`

> **Figure 8 — Survey Data Import**  
> *[Insert screenshot: Survey dialog with Path, Append/Overwrite radios, Import Log. Red arrow: Path. Red arrow: Run Import.]*

<!-- Image: assets/user-guide/figure-08-survey-import.png -->

![Figure 8 — Survey Data Import](images/figure-08-survey-import.png)

---

## Type Curves Import

**Settings:** **Type Curves File** path; the dialog shows it read-only.

**Modes:** **Append from Excel** (**Load from file**, optional multi-select, **Run**) or **Delete from PCE_TC** (**Load from DB**, **Delete**). Only **`dbo.PCE_TC`** is written; **`PCE_Production`** is unchanged.

**Sheet:** First worksheet, **row 1** = headers. Ignored columns include **TC/Production**, **Date**, **Days Seq**, **Day Seq UPRT**. **`ImportDate`** is the import run date.

**Gas S1 → S2:** Vendor **Gas S1 Production (10³m³)** maps to **`[Gas S2 Production (10³m³)]`** (single gas column in the table).

**Units:** **Gas WH** mcf/d → **`[Gas WH Production (e³m³/d)]`**; **Condensate WH** bbl/d → **`[Condensate WH (m³/d)]`**; **Cum Gas** bcf → **`[Cum Gas (e³m³)]`**; **Cum Condy** Mbbl → **`[Cum Condy (m³)]`**.

**Well matching:** Normalized text (spaces, hyphens, case, slashes, digit runs). With **≥ six** hyphen parts, the last **two** are dropped for the base id (for example `…-26W6M - T3 - PnP` → `…-26W6M`); shorter ids stay intact. **Meridian `M`:** an optional trailing **`M`** after **`W` + digits** at the **end** of the match key (for example Excel **`26W6M`**) is ignored so it can align with WM **`26W6`**. Matched WM **`[Well Name]`** is stored plus **` - TC`**.

**Production joins:** **`PCE_TC`** uses physical WM names; production may use composites — see **`sql/vw_PCE_TC_with_Production_Well.sql`** and **`sql/vw_PCE_Production_with_TypeCurves.sql`**.

**Append:** No selection = all mapped wells in file. Per well in scope, existing TC rows for that key are replaced from the file. Unmatched names → **`unmatched_type_curve_wells_<timestamp>.csv`** next to the workbook when applicable.

**Log:** Counts, warnings, and errors appear in the dialog log.

`[IMAGE: Type Curves dialog — modes, path, Run/Delete, log]`

> **Figure 9 — Type Curves Import**  
> *[Insert screenshot: Append from Excel / Delete from PCE_TC radios, path, list, Run / Delete, Log.]*

<!-- Image: assets/user-guide/figure-09-type-curves.png -->

![Figure 9 — Type Curves Import](images/figure-09-type-curves.png)

---

## Whitson+ Mass Upload

**Objective:** Select a sheet from the **Whitson+ File** (**Settings**) for the upload workflow.

1. Verify **Path**.  
2. **Load Sheets** — populate sheet names from the workbook.  
3. Select **Sheet**.  
4. **Post Data** — runs the worker thread.

**Note:** The current implementation performs file read and logging only; the log includes **`[STUB]`** lines and does not invoke an external API. Behavior may change in a future release; refer to internal release notes.

`[IMAGE: Whitson dialog — Path, Load Sheets, Sheet, Post Data, log]`

> **Figure 10 — Whitson+ Mass Upload**  
> *[Insert screenshot: Whitson dialog with Path, Load Sheets, Sheet combo, Upload Log, Post Data. Red arrow: Load Sheets. Red arrow: Sheet. Red arrow: Post Data.]*

<!-- Image: assets/user-guide/figure-10-whitson.png -->

![Figure 10 — Whitson+ Mass Upload](images/figure-10-whitson.png)

---

## Exports / Reports

The dialog displays a **Coming soon** notice. No export jobs are initiated from this module in the current build.

---

## Operational considerations

- **Full Rebuild** clears and repopulates **`PCE_Production`** from all **`PCE_CDA`** rows after refreshing selected **`PCE_CDA`** sales columns from **`Allocation_Factors`**. It does **not** pull Snowflake. Reserve it for full production-table refresh scenarios, not for “fix one month in isolation” unless you understand the full recompute scope.  
- **Quick Update** is the usual mode for routine Snowflake month-range refreshes; remember it recomputes **entire well histories** in **`PCE_Production`** for wells touched by the post-merge **`PCE_CDA`** dataset.  
- Avoid forced termination of the application during active jobs; use dialog **Close** and cancel paths where provided. The Prodview dialog explicitly warns that cancellation may not stop work immediately and that Quick Update can leave partial commits after a successful step.  
- Production database changes should follow company backup and change-control practices.

---

## Runbook — script order and maintenance

Use this section for **routine refreshes**, **new wells**, and **optional CLI utilities**. Commands assume a Windows Command Prompt or PowerShell session with the working directory set to your clone (for example `I:\ETL_Load`).

### Routine GUI refresh (recommended order)

| Step | Action | Depends on | Destructive? | Verify |
|------|--------|--------------|----------------|--------|
| 1 | **Settings** — confirm SQL Server, **ValNav** and **Accumap** paths | — | No | **Save Settings**; reopen Settings to confirm persistence. |
| 2 | **Well Master** — add or fix wells, **Import New Wells** if needed | Valid **`PCE_WM`** for every well you expect in Snowflake | Removing wells deletes dependent rows (see purge note below) | Row exists in **`PCE_WM`** with **GasIDREC** populated for Snowflake pulls. |
| 3 | **Prodview** — **Quick Update** for required **From**–**To** | Step 2 for new wells; VPN/Snowflake | **Yes** — replaces CDA in range, then rebuilds production for all wells in the merged CDA rebuild set (see Prodview section) | Dialog **Results**: row counts, `QUICK UPDATE COMPLETE` summary; spot-check dates in **`PCE_CDA`**. |
| 4 | **PA** — **Run Monthly Loader** for each ValNav month | Step 3 for current CDA; ValNav file on disk | **Yes** — writes **`Allocation_Factors`** and updates S2/condensate columns in **`PCE_CDA`** / **`PCE_Production`** for that month | Results log; allocation and production values for sample wells. |
| 5 | **Public Sales** — **Run Update** for month range | **PA** for same months when ValNav side must be current; Accumap file | **Yes** — writes Accumap side of **`Allocation_Factors`** and updates gas sales / sales CGR on **`PCE_CDA`** / **`PCE_Production`** | Results log; warnings if factors missing. |
| 6 | Optional **Survey**, **Type Curves** | Survey: **`PCE_WM`** lookup for UWI/pad; TC: WM names | Survey overwrite deletes by UWI; TC **Delete** truncates keys in **`PCE_TC`** | Import logs; row counts. |
| 7 | Optional **Full Rebuild** (Prodview) | Current **`PCE_CDA`** is trusted | **Yes** — deletes **all** **`PCE_Production`** then rebuilds | Long run; final summary from **`production_update`**; row counts. |

**After new wells only:** Background job fills **`PCE_CDA`** for the new keys; run **Step 3** (Quick Update) or **Step 7** (Full Rebuild) when you need **`PCE_Production`** aligned.

### If something fails

- Capture the **Results** / Operation log text and the approximate step (Snowflake, SQL insert, allocation month *n*, and so on).  
- **Do not** kill the process during **Quick Update** or **Full Rebuild** unless IT approves recovery; partial transactions can leave mixed state.  
- Re-run from the last **known-good** step after fixing the root cause (connectivity, missing file, bad month selection). Use database restore procedures per IT if data is inconsistent.

### CLI and utility scripts

| Command / script | Purpose | Depends on | Destructive? | Verify |
|------------------|---------|------------|--------------|--------|
| `python production_update_gui.py` | Opens main GUI | `.env`, `settings.ini`, ODBC | No | Window opens; Settings saves. |
| `python production_update_gui.py --accumap-unmatched -m "Aug 2025"` | Prints matched/unmatched Accumap UWIs; optional `-o` CSV | **`PCE_WM`**, Accumap path | No (read-only) | Terminal output or CSV. |
| `python scripts/accumap_unmatched_uwis.py -m "Aug 2025"` | Same audit without the main GUI entry wrapper | Same | No | Same |
| `python production_update.py` | Same pipeline as **Prodview Full Rebuild**: refresh CDA sales from **`Allocation_Factors`**, delete all **`PCE_Production`**, rebuild from **`PCE_CDA`** | Populated **`PCE_CDA`** | **Yes** — full **`PCE_Production`** delete | Console summary; row counts. |
| `python cda.py` | Legacy **Snowflake → `PCE_CDA`** pipeline (VBA-style first-production filter); clears CDA in a fixed date range then reloads | Snowflake, **`PCE_WM`** | **Yes** — deletes **`PCE_CDA`** rows in range | Console steps; inspect **`PCE_CDA`**. Prefer **Prodview Quick Update** for normal operations unless you maintain this script intentionally. |
| `python af.py` | **Allocation_Factors** loader from Excel (interactive `input()` at end) | Excel layout expected by script, **`PCE_WM`** mapping | **Yes** — deletes/replaces allocation rows per script logic | Summary block printed at end. |
| `python survey_import.py "<path>"` then `append`, `overwrite`, or `merge` | Survey import without GUI | File on disk, **`PCE_WM`** for mapped path | **Overwrite** deletes by UWI; **merge** updates selectively | Console / exit code. |
| `python purge_exception_wells.py` | Deletes **`PCE_CDA`**, **`PCE_Production`**, **`Allocation_Factors`**, **`PCE_Surveys`** rows for wells with **`PCE_WM.Exception = 'Y'`** | Exception flags set deliberately | **Yes** | Printed delete counts. |
| `python gas_idrec_production_peek.py …` | Debug peek at **`PCE_Production`** by **GasIDREC** / **PressuresIDREC** | Well keys in **`PCE_WM`** | No | Printed rows. |
| `python test_well_lookup.py …` | Ad hoc well / CDA / production / Snowflake samples (if maintained) | Environment | Read-only if only querying | Console output. |

`[IMAGE: Runbook reference — example SSMS row-count query results after a refresh]`

---

## Troubleshooting

| Symptom | Checks |
|---------|--------|
| SQL Server connection failure | VPN, server name, Windows authentication, ODBC drivers; `.env` / environment variables versus actual deployment. |
| Snowflake errors | `SNOWFLAKE_*` variables in `.env`; network path to Snowflake. |
| Well absent from CDA | Well present in **`PCE_WM`** with **GasIDREC** populated; **Exception** flags; CDA load executed (Prodview Quick Update or post–new-well background job) for dates covering Snowflake data. |
| Production looks wrong after Quick Update | Remember **full-well** **`PCE_Production`** rebuild for wells in the merged CDA set; re-run **PA** / **Public Sales** for months that must be reapplied after CDA changed underneath. |

Optional: `test_well_lookup.py` can print sample rows from `PCE_WM`, CDA, Production, and Snowflake for a given well name when maintained for your environment.

Escalate with **Results** / log excerpts to database or application support per internal procedures.

---

## Appendix A — Figures checklist (for screenshots)

| Figure # | Suggested filename | What to capture | Red arrow notes |
|----------|-------------------|-----------------|-----------------|
| 1 | `figure-01-main-window.png` | Full main window | Settings; Operations list; Operation log |
| 2 | `figure-02-settings.png` | Settings dialog | Save Settings; ValNav + Accumap paths |
| 3 | `figure-03-well-master-view.png` | Well Master View/Edit | Save Changes; Import New Wells |
| 4 | `figure-04-preview-new-wells.png` | Preview New Wells | Checkbox column; Add Wells |
| 5 | `figure-05-prodview.png` | Prodview dialog | Full vs Quick; From/To; Run Update |
| 6 | `figure-06-pa-allocations.png` | PA dialog | Month; Run Monthly Loader |
| 7 | `figure-07-public-sales.png` | Public Sales dialog | From; To; Run Update |
| 8 | `figure-08-survey-import.png` | Survey import | Path; Run Import |
| 9 | `figure-09-type-curves.png` | Type Curves import | Mode radios; Run / Delete |
| 10 | `figure-10-whitson.png` | Whitson+ dialog | Load Sheets; Sheet; Post Data |

---

## Appendix B — Glossary

| Term | Definition |
|------|------------|
| `PCE_WM` | Well master table: identifiers and attributes per well. |
| `PCE_CDA` | Daily-style production data from Snowflake for wells in the well master (plus allocation-driven sales columns). |
| `PCE_Production` | Production history with sequences, cumulatives, and averages for downstream use. |
| `PCE_TC` | Type-curve table populated only by the Type Curves import; well key is WM **`[Well Name]`** + **` - TC`**. |
| `Allocation_Factors` | Monthly allocation data: ValNav side from **PA**; Accumap (sales gas) from **Public Sales Data and Ratios**. |
| GasIDREC / PressuresIDREC | Snowflake keys linking a well to meter/completion data. |
| Composite Name | Production row naming derived from well master mapping where applicable. |
| Quick Update | Snowflake pull for selected **From**–**To** months; replaces **`PCE_CDA`** in that date range; then reloads **all** **`PCE_CDA`**, recomputes metrics, and **replaces all **`PCE_Production`** history** for wells in the rebuilt dataset. |
| Full Rebuild | Repaint selected **`PCE_CDA`** sales columns from **`Allocation_Factors`**, then delete all rows in **`PCE_Production`** and rebuild from **all** **`PCE_CDA`**; **no** Snowflake call. |

---

## Appendix C — Logical database schema (SQL Server)

The following **entity-relationship diagram is logical**: the application and well keys imply the relationships below. SQL Server might not declare every relationship as a foreign key. Column inventory is maintained from **`INFORMATION_SCHEMA.COLUMNS`** exports (see `output.txt` in the repository for a recent tab-separated example).

**Tables included:** `PCE_WM`, `PCE_CDA`, `PCE_Production`, `Allocation_Factors`, `PCE_Surveys`, `PCE_TC`. **Views** commonly deployed with the app include **`dbo.vw_PCE_Production_with_TypeCurves`**, **`dbo.vw_PCE_TC_with_Production_Well`**, and **`vw_PCE_WM_Ordered`** (view schema may be owner-specific); they are read-only for routine ETL and join production to type curves or order wells for display.

```mermaid
erDiagram
  PCE_WM {
    string GasIDREC
    string PressuresIDREC
    string Well_Name
    string Exception
  }
  PCE_CDA {
    bigint CDA_ID
    string Well_Name
    date ProdDate
    string GasIDREC
    string PressuresIDREC
  }
  PCE_Production {
    bigint Production_ID
    string Well_Name
    date Date
  }
  Allocation_Factors {
    bigint Allocation_ID
    string Well_Name
    date MonthStartDate
  }
  PCE_Surveys {
    int SurveyID
    string UWI
    string Well_Name
    decimal Latitude
    decimal Longitude
  }
  PCE_TC {
    bigint PCE_TCId
    string Well_Name
    date ImportDate
  }

  PCE_WM ||--o{ PCE_CDA : "Well_Name plus IDs"
  PCE_WM ||--o{ PCE_Production : "Well_Name"
  PCE_WM ||--o{ Allocation_Factors : "Well_Name"
  PCE_WM ||--o{ PCE_Surveys : "Well_Name"
  PCE_WM ||--o{ PCE_TC : "Well_Name suffix - TC"
```

`[IMAGE: Rendered ER diagram — logical PCE tables from Appendix C for Word]`

---

## Copy-paste: ChatGPT instructions for Word (.docx)

Provide the following instructions **together with** this `USER_GUIDE.md` when generating a Word document.

---

**Instructions for the assistant:**

Convert the attached Markdown **User Guide** to a **Microsoft Word** document (.docx).

1. **Title page:** Title **Production Update System — User Guide**, subtitle **User Guide**, organization **Pacific Canbriam Energy LTD** (or as in the document), plus **Version** and **Date** placeholders if shown.

2. **Styles:** Apply **Heading 1** to the document title and major parts; **Heading 2** and **Heading 3** for sections and subsections consistent with the Markdown hierarchy.

3. **Table of contents:** Insert an automatic **Table of Contents** after the title page (Word: References → Table of Contents).

4. **Lists and code:** Preserve bullet and numbered lists. Apply **monospace** to inline code, table names (for example `PCE_CDA`), file names, and environment variable names.

5. **Figures:** For each block beginning **Figure N —**, insert a picture placeholder or frame. Use the italic line under the figure title as the art brief. Caption each figure `Figure N — …`. Where the italic line specifies **Red arrow:**, add **red arrow shapes** in Word (Insert → Shapes → arrow, red outline) to the indicated controls. If image files are missing, use a labeled placeholder with the caption. Where a line begins with **`[IMAGE:`**, treat it as an additional art brief for the same or an adjacent figure block.

6. **Mermaid:** If the Markdown contains fenced **`mermaid`** code blocks, either render them to images and embed, or paste the source into a monospace appendix labeled **Diagram source**.

7. **Callouts:** Apply distinct formatting (for example paragraph border or labeled text box) for **Note**, **Caution**, and similar emphasized blocks.

8. **Accuracy:** Do not add features not described in the source. Retain **Coming soon** where stated.

9. **Appendices:** Include Appendix A (figures table), Appendix B (glossary), and Appendix C (logical database schema and diagram).

---

*End of User Guide.*
