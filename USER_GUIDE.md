# Production Update System — User Guide

**Organization:** Pacific Canbriam Energy LTD  
**Document version:** 1.0  
**Last updated:** *(insert date)*  

## Document purpose and scope

This User Guide describes the **Production Update System** (desktop application) used within Pacific Canbriam Energy LTD. It documents prerequisites, main-window operations, module-specific procedures, and the principal SQL Server tables affected. The intended audience is staff authorized to run production updates, allocations, and related imports.

---

## Table of contents

1. [Before you start](#before-you-start)  
2. [How the pieces fit together](#how-the-pieces-fit-together)  
3. [Main window](#main-window)  
4. [Settings](#settings)  
5. [Well Master List](#well-master-list)  
6. [Prodview / Snowflake — Daily Production Retrieve](#prodview--snowflake--daily-production-retrieve)  
7. [Production Accounting Allocations (PA)](#production-accounting-allocations-pa)  
8. [Public Sales Data and Ratios](#public-sales-data-and-ratios)  
9. [Survey Data Import](#survey-data-import)  
10. [Type Curves Import](#type-curves-import)  
11. [Exports / Reports](#exports--reports)  
12. [Whitson+ Mass Upload](#whitson-mass-upload)  
13. [Operational considerations](#operational-considerations)  
14. [Troubleshooting](#troubleshooting)  
15. [Appendix A — Figures checklist](#appendix-a--figures-checklist-for-screenshots)  
16. [Appendix B — Glossary](#appendix-b--glossary)  
17. [Copy-paste: ChatGPT instructions for Word (.docx)](#copy-paste-chatgpt-instructions-for-word-docx)  

---

## Before you start

### Prerequisites

- Network connectivity to the company **SQL Server** instance and, for Prodview and Snowflake well import, to **Snowflake**.  
- **Windows** host; SQL Server access uses **Windows authentication** unless otherwise configured.  
- **`.env`** in the application directory (or project root when running from source), containing Snowflake and any server/database overrides. Connection defaults may reference environment variables such as `SQL_SERVER` and `SQL_DATABASE` (confirm values with IT).  
- **`settings.ini`** adjacent to the executable or project folder. File paths and SQL connection fields are written when **Settings** is saved.

### Execution

- **Deployed build:** Run the packaged executable (e.g. `ProductionUpdate.exe`) from a directory that includes `.env` and `settings.ini` where applicable.  
- **Source:** Python 3.x, dependencies per `requirements.txt`, launch the entry point that opens the Production Update main window.

Credential, VPN, and ODBC driver requirements are managed by IT; obtain confirmation before production use.

---

## How the pieces fit together

### Data objects (summary)

| Object | Role |
|--------|------|
| `PCE_WM` | Well master: well list and linkage to Snowflake identifiers (**GasIDREC**, **PressuresIDREC**). Wells absent or excluded here may be omitted from daily loads. |
| `PCE_CDA` | Daily-style production rows sourced from Snowflake (and related processing). |
| `PCE_Production` | Production history for reporting; sequences, cumulatives, and averages derived from CDA according to the job executed. |
| `Allocation_Factors` | Monthly allocation inputs from ValNav and Accumap (PA module). |

**Public Sales Data and Ratios** applies allocation factors to update calculated sales fields in `PCE_CDA` and aligned columns in `PCE_Production`.

### Reference sequence

1. Maintain **Well Master** (including new wells from Snowflake where required).  
2. Execute **Prodview / Snowflake** per the agreed refresh schedule.  
3. Run **PA** after monthly ValNav and Accumap files are available.  
4. Run **Public Sales Data and Ratios** when sales-ratio fields require updating for a defined month range.

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
| 7 | Exports / Reports |
| 8 | Whitson+ Mass Upload |

Operation buttons are mutually exclusive (single selection). Closing a dialog clears the selection.

The **Operation log** records timestamped lines (`[HH:MM:SS] …`). Status text (e.g. **Ready**) appears below the log.

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

PA, Survey, Type Curves, and Whitson modules read these paths as defaults; incorrect or missing paths produce file-not-found messages in the respective dialogs.

> **Figure 2 — Settings**  
> *[Insert screenshot: Settings dialog showing SQL Server fields and all file path rows with Browse. Red arrow: Save Settings. Red arrow: ValNav / Accumap paths.]*

<!-- Image: assets/user-guide/figure-02-settings.png -->

![Figure 2 — Settings](images/figure-02-settings.png)

---

## Well Master List

**Objective:** Maintain `PCE_WM`: view and edit permitted attributes, stage additions, Excel import/export, **Import New Wells** from Snowflake, and removal of selected wells.

**View/Edit:** Search, column edits where enabled, **Refresh**, **Save Changes**, Excel operations, **Update from Snowflake**, **Remove Selected**. Identifier fields (e.g. Well Name, GasIDREC, PressuresIDREC) are fixed after creation.

**Add New:** Stage rows; **Save to Database** commits staged data.

### Import New Wells (Snowflake)

**Preview New Wells** lists candidate wells. Uncheck rows to exclude them; only checked rows are inserted. Confirm selection, then **Add Wells**.

### Post-import behavior

The application queues a background job to load daily data into **`PCE_CDA`** for inserted wells (historical window per application logic, typically from 2009 through the current date). A progress indicator is shown during this task.

**Caution:** That job updates **`PCE_CDA` only**. It does **not** rebuild **`PCE_Production`**. For Production refresh (sequences, cumulatives, etc.), run **Prodview / Snowflake** in **Quick Update** for the required months, or coordinate a **Full Rebuild** with the responsible team.

> **Figure 3 — Well Master View/Edit**  
> *[Insert screenshot: Well Master dialog, View/Edit tab, search and table. Red arrow: Save Changes. Red arrow: Import New Wells (Snowflake).]*

<!-- Image: assets/user-guide/figure-03-well-master-view.png -->

![Figure 3 — Well Master View/Edit](images/figure-03-well-master-view.png)

> **Figure 4 — Preview New Wells**  
> *[Insert screenshot: Preview New Wells with checkbox column, Well Name / IDs, Add Wells. Red arrow: checkbox column. Red arrow: Add Wells.]*

<!-- Image: assets/user-guide/figure-04-preview-new-wells.png -->

![Figure 4 — Preview New Wells](images/figure-04-preview-new-wells.png)

---

## Prodview / Snowflake — Daily Production Retrieve

**Objective:** Retrieve daily production-related data from Snowflake and update `PCE_CDA` and `PCE_Production`.

### Update range (Quick Update)

- **From:** Approximately the last **36** calendar months, listed **oldest first**. Default selection is the **oldest** month; adjust for the required start month.  
- **To:** **Current calendar month** only (single entry). **From**–**To** defines the Quick Update window.

### Update mode

**Full Rebuild Mode** (default)

- Invokes the full `production_update` pipeline: **`PCE_Production`** is cleared and rebuilt from **`PCE_CDA`** (sequences, cumulatives, monthly averages).  
- Extended runtime (dialog indicates on the order of 30–40 minutes for a full database).  
- **From/To** selectors do **not** bound this mode.

**Quick Update Mode**

- Processes the selected **From**–**To** range.  
- Updates **`PCE_CDA`** for that span, then updates **`PCE_Production`** for affected wells (merge, sequences, cumulatives, averages) per the quick-update implementation.

Select **Run Update**, acknowledge the confirmation, monitor **Results** and the progress bar. **Close** exits the dialog; cancellation may be offered while a job is active.

> **Figure 5 — Prodview dialog**  
> *[Insert screenshot: Prodview dialog with Update Range, Update Mode radios, Results, Run Update. Red arrow: Full vs Quick. Red arrow: From/To. Red arrow: Run Update.]*

<!-- Image: assets/user-guide/figure-05-prodview.png -->

![Figure 5 — Prodview dialog](images/figure-05-prodview.png)

---

## Production Accounting Allocations (PA)

**Objective:** Load one month of ValNav and Accumap data into **`Allocation_Factors`** (monthly loader logic).

**Prerequisites:** **ValNav Template** and **Accumap Template** set in **Settings**; source files must exist for the target month.

**Month control:** Dropdown contains roughly the **last 24** calendar months, **oldest first**. Select the month that matches the files being loaded (first list item is the oldest in the window).

**Procedure**

1. Open **Production Accounting Allocations (PA)**.  
2. Select **Month**.  
3. Verify status: database, **ValNav file**, **Public Data Accumap file**.  
4. **Run Monthly Loader**; confirm the dialog.  
5. Review **Results** (counts, warnings, elapsed time).

> **Figure 6 — PA allocations**  
> *[Insert screenshot: PA dialog with month, ValNav path, Accumap path, status lines, Results. Red arrow: Month. Red arrow: Run Monthly Loader.]*

<!-- Image: assets/user-guide/figure-06-pa-allocations.png -->

![Figure 6 — PA allocations](images/figure-06-pa-allocations.png)

---

## Public Sales Data and Ratios

**Objective:** Update calculated sales fields in **`PCE_CDA`** and corresponding columns in **`PCE_Production`** using **`Allocation_Factors`** (set-based SQL updates).

**Month range:** **From** and **To** list every month from **January 2008** through the **current calendar month**, oldest first. Default **From** is **Jan 2008**; default **To** is the **current month** (full history through the current month unless changed).

**Run Update** → confirm → monitor the log.

> **Figure 7 — Public Sales Data and Ratios**  
> *[Insert screenshot: dialog with From/To combos, info panel, Run Update. Red arrow: From. Red arrow: To. Red arrow: Run Update.]*

<!-- Image: assets/user-guide/figure-07-public-sales.png -->

![Figure 7 — Public Sales Data and Ratios](images/figure-07-public-sales.png)

---

## Survey Data Import

**Objective:** Load survey rows from Excel into **`PCE_Surveys`**.

There are **two import paths** in the Survey dialog:

1. **Bulk (Settings path)** — Uses **Survey File** from **Settings** (read-only path in the dialog). This is the **legacy** flat-table flow: the first row is headers; columns are matched by name (matching is **case-insensitive** after normalization). The file may be **Excel** (`.xlsx` / `.xls`) or **comma-separated** `.csv` (Excel-style export). Change **Settings** to repoint the file.

2. **Directional / mapped import** — Choose this mode to **Browse** to any `.xlsx` / `.xls` / `.csv` file whose layout varies by vendor. CSV is treated as a **single sheet** (no sheet picker). Click **Configure mapping…** to open a **second dialog**: pick the sheet, set the **header row** and **first data row**, choose the **well name** cell, and map file columns to survey fields (**Measured Depth** is required). **UWI** and **Pad** are **not** taken from the Excel file; after the well name is read, the app looks up **one** row in **`PCE_WM`** by matching the cell text to **`[Well Name]`** (same field used across Well Master and production) and uses **`[Value Navigator UWI]`** and **`[Pad Name]`** for every survey row. You can **Load/Save** mapping presets (JSON) for repeat layouts.

**Import Mode:** **Append Mode** (insert rows not already present) or **Overwrite Mode** (delete existing rows for matching UWIs, then insert). Applies to both paths.

Execute **Run Import**; review the **Import Log**.

> **Figure 8 — Survey Data Import**  
> *[Insert screenshot: Survey dialog with Path, Append/Overwrite radios, Import Log. Red arrow: Path. Red arrow: Run Import.]*

<!-- Image: assets/user-guide/figure-08-survey-import.png -->

![Figure 8 — Survey Data Import](images/figure-08-survey-import.png)

---

## Type Curves Import

**Objective:** Import type curve data from the workbook defined as **Type Curves File** in **Settings**.

**Path** is read-only in the dialog; update **Settings** if the file location changes.

**Run Import** displays a warning: existing type-curve records for wells whose names begin with **`YE2`** are deleted before load. Confirm only after review.

Monitor **Import Log** for completion and errors.

> **Figure 10 — Type Curves Import**  
> *[Insert screenshot: Type Curves dialog with Path, Import Log, Run Import. Red arrow: Path. Red arrow: Run Import.]*

<!-- Image: assets/user-guide/figure-10-type-curves.png -->

![Figure 10 — Type Curves Import](images/figure-10-type-curves.png)

---

## Exports / Reports

The dialog displays a **Coming soon** notice. No export jobs are initiated from this module in the current build.

---

## Whitson+ Mass Upload

**Objective:** Select a sheet from the **Whitson+ File** ( **Settings** ) for the upload workflow.

1. Verify **Path**.  
2. **Load Sheets** — populate sheet names from the workbook.  
3. Select **Sheet**.  
4. **Post Data** — runs the worker thread.

**Note:** The current implementation performs file read and logging only; the log includes **`[STUB]`** lines and does not invoke an external API. Behavior may change in a future release; refer to internal release notes.

> **Figure 9 — Whitson+ Mass Upload**  
> *[Insert screenshot: Whitson dialog with Path, Load Sheets, Sheet combo, Upload Log, Post Data. Red arrow: Load Sheets. Red arrow: Sheet. Red arrow: Post Data.]*

<!-- Image: assets/user-guide/figure-09-whitson.png -->

![Figure 9 — Whitson+ Mass Upload](images/figure-09-whitson.png)

---

## Operational considerations

- **Full Rebuild** replaces the entire **`PCE_Production`** table and requires substantial elapsed time. Reserve it for full refresh scenarios, not incremental date corrections.  
- **Quick Update** is the appropriate mode for routine month-range refreshes.  
- Avoid forced termination of the application during active jobs; use dialog **Close**/cancel paths where provided.  
- Production database changes should follow company backup and change-control practices.

---

## Troubleshooting

| Symptom | Checks |
|---------|--------|
| SQL Server connection failure | VPN, server name, Windows authentication, ODBC drivers; `.env` / environment variables vs. actual deployment. |
| Snowflake errors | `SNOWFLAKE_*` variables in `.env`; network path to Snowflake. |
| Well absent from CDA | Well present in **`PCE_WM`** with **GasIDREC** populated; **Exception** flags; CDA load executed (Prodview Quick Update or post–new-well background job) for dates covering Snowflake data. |

Optional: `test_well_lookup.py` (if maintained) can print sample rows from `PCE_WM`, CDA, Production, and Snowflake for a given well name.

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
| 9 | `figure-09-whitson.png` | Whitson+ dialog | Load Sheets; Sheet; Post Data |
| 10 | `figure-10-type-curves.png` | Type Curves import | Path; Run Import |

---

## Appendix B — Glossary

| Term | Definition |
|------|------------|
| `PCE_WM` | Well master table: identifiers and attributes per well. |
| `PCE_CDA` | Daily-style production data from Snowflake for wells in the well master. |
| `PCE_Production` | Production history with sequences, cumulatives, and averages for downstream use. |
| `Allocation_Factors` | Monthly allocation data from ValNav/Accumap (PA). |
| GasIDREC / PressuresIDREC | Snowflake keys linking a well to meter/completion data. |
| Composite Name | Production row naming derived from well master mapping where applicable. |
| Quick Update vs Full Rebuild | Quick: month-bounded CDA and Production update. Full: complete `PCE_Production` rebuild from `PCE_CDA`. |

---

## Copy-paste: ChatGPT instructions for Word (.docx)

Provide the following instructions **together with** this `USER_GUIDE.md` when generating a Word document.

---

**Instructions for the assistant:**

Convert the attached Markdown **User Guide** to a **Microsoft Word** document (.docx).

1. **Title page:** Title **Production Update System — User Guide**, subtitle **User Guide**, organization **Pacific Canbriam Energy LTD** (or as in the document), plus **Version** and **Date** placeholders if shown.

2. **Styles:** Apply **Heading 1** to the document title and major parts; **Heading 2** and **Heading 3** for sections and subsections consistent with the Markdown hierarchy.

3. **Table of contents:** Insert an automatic **Table of Contents** after the title page (Word: References → Table of Contents).

4. **Lists and code:** Preserve bullet and numbered lists. Apply **monospace** to inline code, table names (e.g. `PCE_CDA`), file names, and environment variable names.

5. **Figures:** For each block beginning **Figure N —**, insert a picture placeholder or frame. Use the italic line under the figure title as the art brief. Caption each figure `Figure N — …`. Where the italic line specifies **Red arrow:**, add **red arrow shapes** in Word (Insert → Shapes → arrow, red outline) to the indicated controls. If image files are missing, use a labeled placeholder with the caption.

6. **Callouts:** Apply distinct formatting (e.g. paragraph border or labeled text box) for **Note**, **Caution**, and similar emphasized blocks.

7. **Accuracy:** Do not add features not described in the source. Retain **Coming soon** where stated.

8. **Appendices:** Include Appendix A (figures table) and Appendix B (glossary).

---

*End of User Guide.*
