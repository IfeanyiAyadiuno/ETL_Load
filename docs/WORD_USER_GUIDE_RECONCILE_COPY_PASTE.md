# Word User Guide — copy-paste reconcile blocks

Use this file when updating **`PCE_Production_Update_User_Guide (1).docx`** (or any export derived from an older guide) so it matches the **current application** and **`USER_GUIDE.md`** in this repository.

**How to use:** In Word, use **Find** (or Find & Replace) for each **FIND (delete)** block, then paste the **REPLACE (paste)** block. Word may merge paragraphs when pasting; re-apply heading styles if needed.

**Canonical source in repo:** `USER_GUIDE.md` (Document version **1.2**).

---

## 1. Cover / title page — document version

**WHERE:** Title page, line under organization (centered).

**FIND (delete):**

```text
Document Version: 1.1
```

**REPLACE (paste):**

```text
Document Version: 1.2
```

**WHERE:** Same area — last updated date (adjust to your publication date if not April 8, 2026).

**FIND (delete):**

```text
Last Updated: April 8, 2026
```

**REPLACE (paste):** *(keep the same date if still accurate, or set your handoff date)*

```text
Last updated: April 8, 2026
```

---

## 2. Section 1 — Before you start — “From source” wording (optional)

**WHERE:** **Execution** subsection, paragraph that starts with “From source”.

**FIND (delete):**

```text
From source (repo root):
```

**REPLACE (paste):** *(matches `USER_GUIDE.md`)*

```text
From source (project folder):
```

---

## 3. Section 2 — Data objects table — **`PCE_TC` row** (required)

**WHERE:** **How the Pieces Fit Together** → **Data Objects (Summary)** table, row for **`PCE_TC`**.

**FIND (delete):**

```text
PCE_TCType-curve metrics from the Type Curves Excel import only. Stored [Well Name] is PCE_WM.[Well Name] + literal  - TC (physical well key, not composite), so rows stay distinct from PCE_Production.
```

**Note:** In Word the table may use line breaks instead of run-on text. If Find does not match, search for this shorter unique fragment:

**FIND (delete) — short fragment:**

```text
Type Curves Excel import only
```

Then replace the **entire cell** or paragraph for `PCE_TC` with:

**REPLACE (paste):** *(single table cell text; add your own line breaks inside the cell for readability)*

```text
PCE_TC — Type-curve metrics from the Type Curves Excel import (GUI) or YE2/YE23 bulk script. WM-backed stored [Well Name] is the longer of cleaned Excel vs PCE_WM.[Well Name], plus literal " - TC". File-only rows that do not match WM use Excel text; if the name starts with YE2 (including YE23), it is stored verbatim with no " - TC" suffix; other file-only rows get " - TC". PCE_Production receives a materialized copy at ImportDate via sync_tc_to_production. Pad names: the PCE-TC- prefix on [Pad Name] is applied only for non–YE2-family type curves; YE2/YE23-style wells keep a normalized pad without that prefix.
```

---

## 4. Section 2 — Reporting views paragraph (required)

**WHERE:** Immediately after the **Data Objects** table (or merged into that narrative), the paragraph about **reporting views** / **vw_PCE_** / **`sql/`** scripts.

**FIND (delete):**

```text
Reporting views (read-only in normal operations): The database may expose views such as dbo.vw_PCE_Production_with_TypeCurves, dbo.vw_PCE_TC_with_Production_Well, and vw_PCE_WM_Ordered (schema may differ from dbo). The desktop app does not treat these as write targets during routine operations; they exist for reporting and joins. Deploy or refresh their definitions from the sql/ scripts in this repository when IT approves.
```

If your Word file uses different line breaks, search for:

**FIND (delete) — short fragment:**

```text
sql/ scripts in this repository
```

**REPLACE (paste):**

```text
Optional reporting views: Your DBA may deploy read-only views (for example joins between production and type curves). The desktop app does not ship view DDL in this repository; routine ETL uses PCE_TC, PCE_Production, and Python-side sync_tc_to_production instead of view-based writes.
```

---

## 5. Section 10 — Survey Data Import — **Objective + WM naming** (add if missing)

**WHERE:** **Survey Data Import** section, after the objective line (or replace a short objective if you only have one sentence).

If the Word guide has **no** paragraph on **`PCE_Surveys.[Well Name]`** vs Composite Name, **insert** the following **after** the objective paragraph (new paragraph).

**REPLACE (paste) — insert new paragraph(s):**

```text
For each imported survey row, PCE_Surveys.[Well Name] is taken from Well Master: [Composite Name] when non-empty on the matched WM row, otherwise [Well Name]. Bulk import resolves this via [Value Navigator UWI] when possible, and otherwise via the same well-name matching keys used to find the WM row.

Bulk (Settings path): first row = headers; Excel or CSV; case-insensitive column normalization.

Directional / mapped import: UWI and Pad are not taken from the survey file; after the well name cell is matched to one PCE_WM row (by [Well Name] match keys), the app uses that row’s [Value Navigator UWI] and [Pad Name], and stores the composite-preferred well label (Composite Name when set, else Well Name) on each inserted survey row.
```

---

## 6. Section 11 — Type Curves Import — **Pad `PCE-TC-` rule** (add if missing)

**WHERE:** **Type Curves Import** section (after modes / well matching, or in the dialog description).

**REPLACE (paste) — insert new paragraph:**

```text
Pad names from the workbook are normalized. The PCE-TC- prefix on [Pad Name] is applied only for non–YE2-family type curves (stored [Well Name] not matching the application’s YE2-style rule, i.e. names that do not start with YE2). YE2/YE23-style rows keep a normalized pad slug without the PCE-TC- prefix.
```

**Optional — full intro aligned with the GUI** (replace a short “loads into PCE_TC…” blurb if you want one block):

**REPLACE (paste):**

```text
Loads into dbo.PCE_TC, then materializes into PCE_Production at ImportDate. WM-backed keys use the longer of Excel vs WM well id plus " - TC". File-only rows: names starting with YE2 (including YE23) stay verbatim; other file-only wells get " - TC". Pad names get the "PCE-TC-" prefix only for non-YE2-family type curves; YE2/YE23 rows keep pad text without that prefix.
```

---

## 7. Section 15 — Runbook — CLI table — **`survey_import.py`** row (required)

**WHERE:** **Runbook** → **CLI and Utility Scripts** table (or list), row for **`python survey_import.py`**.

**FIND (delete):**

```text
python survey_import.py "<path>"Survey import without GUI. Then enter: append, overwrite, or mergeFile on disk, PCE_WM for mapped pathOverwrite deletes by UWI; merge updates selectivelyConsole / exit code.
```

Word may have broken this into a table; search for:

**FIND (delete) — short fragment:**

```text
append, overwrite, or merge
```

**REPLACE (paste):** *(table row narrative — adjust line breaks for your table layout)*

```text
python survey_import.py "<path>" then append or overwrite — Survey import without GUI. File on disk, PCE_WM for mapping. Overwrite deletes existing rows for matching UWIs, then inserts; append skips (UWI, depth) pairs already in PCE_Surveys before insert. Console / exit code.
```

---

## 8. Section 15 — Runbook — Step 6 optional row (optional clarity)

**WHERE:** Routine GUI refresh table, **Step 6** (Optional Survey, Type Curves).

**FIND (delete):**

```text
Survey: PCE_WM lookup for UWI/pad; TC: WM names
```

**REPLACE (paste):**

```text
Survey: PCE_WM via UWI and/or well-name keys; stored [Well Name] prefers WM Composite Name when set. Type curves: WM-backed stored names per Type Curves section; pad PCE-TC- prefix only for non-YE2-family rows.
```

---

## 9. Appendix B — Glossary — **`PCE_TC`** row (required)

**WHERE:** **Appendix B — Glossary** table, **`PCE_TC`** definition.

**FIND (delete):**

```text
PCE_TCType-curve table populated only by the Type Curves import; well key is WM [Well Name] +  - TC.
```

Or search:

**FIND (delete) — short fragment:**

```text
well key is WM [Well Name]
```

**REPLACE (paste):**

```text
PCE_TC — Type-curve metrics from the Type Curves GUI import and/or YE2/YE23 bulk script; stored [Well Name] rules and Pad Name prefix rules are as in the Data objects (summary) table for PCE_TC.
```

---

## 10. Appendix C — Schema narrative — views / `sql/` (required)

**WHERE:** **Appendix C — Logical Database Schema**, opening paragraphs (before or around the ER diagram).

**FIND (delete):** *(match the sentence about views and repo scripts; your Word may say “sql/” or list view names as guaranteed)*

```text
Views commonly deployed with the app include dbo.vw_PCE_Production_with_TypeCurves, dbo.vw_PCE_TC_with_Production_Well, and vw_PCE_WM_Ordered; they are read-only for routine ETL and join production to type curves or order wells for display.
```

**REPLACE (paste):**

```text
Optional database views (if any) are DBA-maintained; this repo does not ship view DDL. Type-curve data is joined in reporting via PCE_TC and materialized PCE_Production rows, not via application-bundled SQL view scripts.
```

---

## 11. Application architecture — Mermaid diagram (optional)

**WHERE:** **Application Architecture and Data Flow** — if the Word doc truncates the diagram with “see source repository for full diagram”.

**REPLACE (paste):** Open `USER_GUIDE.md` in the repo, copy the **entire** fenced `mermaid` block under **## Application architecture and data flow**, and paste into Word (per your internal process: render to image via mermaid.live, or keep as monospace “Diagram source” per `USER_GUIDE.md` § “Copy-paste: ChatGPT instructions for Word”).

---

## 12. Repo-only fix (optional) — `USER_GUIDE.md` glossary still simplified

The Markdown **`USER_GUIDE.md`** Appendix B row for **`PCE_TC`** is still the old one-liner. To keep **Markdown and Word** aligned after you edit Word, apply this in **`USER_GUIDE.md`**:

**FILE:** `USER_GUIDE.md` — Appendix B table.

**FIND (delete):**

```markdown
| `PCE_TC` | Type-curve table populated only by the Type Curves import; well key is WM **`[Well Name]`** + **` - TC`**. |
```

**REPLACE (paste):**

```markdown
| `PCE_TC` | Same rules as the **Data objects (summary)** row for **`PCE_TC`** (GUI and YE2/YE23 bulk; longer-of-Excel-vs-WM + **` - TC`** for WM-backed rows; file-only / YE2 rules; **`sync_tc_to_production`**; pad **`PCE-TC-`** prefix only for non–YE2-family rows). |
```

---

## Reference — line numbers in `USER_GUIDE.md`

| Topic | Approx. lines |
|--------|----------------|
| Document version / date | 3–5 |
| `PCE_TC` data object row | 97 |
| Reporting views | 101 |
| Survey Import (Composite / UWI / directional) | 406–417 |
| Type Curves Import (full section) | 433–455 |
| Runbook Step 6 | 520 |
| `survey_import.py` CLI | 541 |
| Appendix C views sentence | 602 |

---

*Generated for handoff: reconcile Word guide `PCE_Production_Update_User_Guide (1).docx` against repository `USER_GUIDE.md` and implemented behavior.*
