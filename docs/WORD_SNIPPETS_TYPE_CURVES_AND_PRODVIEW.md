# Word-ready snippets — Type Curves & Prodview

Use these blocks in **`PCE_Production_Update_User_Guide (1).docx`** (or regenerate from **`USER_GUIDE.md`**). Source of truth after edit: **`USER_GUIDE.md`** §Type Curves Import and §Prodview / Snowflake.

---

## Replace entire “Type Curves Import” bullet section

**Settings:** Type Curves File path; the dialog shows it read-only.

**Modes:** **Append from Excel** (**Load from file**, optional multi-select, **Run**) or **Delete from PCE_TC** (**Load from DB**, **Delete**). Writes **`dbo.PCE_TC`**, then refreshes matching **`PCE_Production`** rows (same **`[Well Name]`** as **`PCE_TC`**, **`[Date]`** = **`ImportDate`**) via **`sync_tc_to_production`** — so **`PCE_Production`** is updated for those type-curve keys (not left unchanged).

**Sheet:** First worksheet, **row 1** = headers. Ignored columns include **TC/Production**, **Date**, **Days Seq**, **Day Seq UPRT**. **`ImportDate`** is the import run date.

**Gas S1 → S2:** Vendor **Gas S1 Production (10³m³)** maps to **`[Gas S2 Production (10³m³)]`** (single gas column in the table).

**Units:** **Gas WH** mcf/d → **`[Gas WH Production (e³m³/d)]`**; **Condensate WH** bbl/d → **`[Condensate WH (m³/d)]`**; **Cum Gas** bcf → **`[Cum Gas (e³m³)]`**; **Cum Condy** Mbbl → **`[Cum Condy (m³)]`**. **Gas S2** and **condensate sales** rates and cumulatives from the workbook are converted to metric and stored in the **`(10³m³)`**, **`(m³/d)`**, and cumulative **`(m³)`** / **`(e³m³)`** columns on **`PCE_TC`**.

**Well matching:** Normalized text (spaces, hyphens, case, slashes, digit runs). With **six or more** hyphen-separated parts, the last **two** are dropped **only for the WM lookup key** (shorter ids stay intact). **Meridian M:** an optional trailing **M** after **W + digits** at the **end** of the match key is ignored so Excel can align with WM. **Stored `[Well Name]` for WM-backed rows** is the **longer** of cleaned Excel text vs **`PCE_WM.[Well Name]`** (tie → WM), then literal **` - TC`**. **File-only** rows: if the name starts with **YE2** (including **YE23**), the Excel string is stored **verbatim** (no **` - TC`**); other file-only wells get **` - TC`**.

**Pad names:** Pad cells are normalized to a hyphenated slug. The **`PCE-TC-`** prefix on **`[Pad Name]`** is applied only for **non–YE2-family** type curves (stored well name does **not** start with **YE2**). **YE2** / **YE23**-style rows keep the slug **without** the **`PCE-TC-`** prefix.

**Append:** No row selection = import **every** row in the file. For each stored well key in scope, existing **`PCE_TC`** rows for that key are replaced from the file. Unmatched names → **`unmatched_type_curve_wells_<timestamp>.csv`** next to the workbook when applicable.

**YE2/YE23 bulk (CLI):** **`python scripts/ye23_typecurves_to_pce_tc.py "<path-to-xlsx>"`** — inserts into **`PCE_TC`** with Excel well names verbatim (no **` - TC`**), then runs the same **`sync_tc_to_production`** pass.

**YE WH mirroring:** For stored **`[Well Name]`** values matching **`LIKE 'YE2%'`**, import sets **`[Gas WH Production (e³m³/d)]`** from the Gas S2 rate and **`[Condensate WH (m³/d)]`** from the condensate sales rate.

**Log:** Counts, warnings, and errors appear in the dialog log.

---

## Replace entire “Prodview / Snowflake” narrative (matches current dialog)

**Objective:** Keep **`PCE_CDA`** and **`PCE_Production`** aligned with Snowflake (**Snowflake → CDA + production rebuild**, default) or rebuild **`PCE_Production`** from all **`PCE_CDA`** using **`Allocation_Factors`** (**Full rebuild**). Both paths finish by materializing **`PCE_TC`** into **`PCE_Production`** via **`sync_tc_to_production`**.

**Dialog layout**

- **Overview** — Short scope line for the selected mode (about **5–10 minutes** for Full rebuild, **10–20 minutes** for Snowflake + production, including type-curve sync on the Snowflake path).

- **Update mode** — Two options (**Snowflake → CDA + production rebuild** is selected by default):
  - **Full rebuild — PCE_Production from all PCE_CDA** — Refreshes selected **`PCE_CDA`** sales columns from **`Allocation_Factors`**, deletes **all** **`PCE_Production`** rows, rebuilds from **all** **`PCE_CDA`**. **Does not** call Snowflake. Progress bar is **indeterminate** with an **elapsed time** status.
  - **Snowflake → CDA + production rebuild** — One Snowflake pull for a **rolling ~18 calendar months** ending on the app’s **effective end date** (about **today minus 2 days** by default). There are **no From/To pickers**; dates are **automatic**. Then replaces **`PCE_CDA`** for that window, rebuilds **`PCE_Production`** for wells in the merged dataset, and runs **`sync_tc_to_production`**.

- **This will:** — Bullet list that mirrors the selected mode (same text as the dialog’s **“This will:”** panel).

**Snowflake → CDA + production rebuild — what happens (summary)**

1. Trim future-dated **`PCE_CDA`** / **`PCE_Production`** past the effective end date where applicable.  
2. Pull **Snowflake** for each day in the rolling window.  
3. Replace **`PCE_CDA`** in that date range; delete **`PCE_Production`** in the same range **except** rows whose **`[Well Name]`** ends with **` - TC`** (type-curve keys) or starts with **`YE2`**.  
4. Reload **all** **`PCE_CDA`** for the production pass, apply mapping and first-production rules, recompute sequences, cumulatives, averages, on-production year.  
5. For each well in the rebuilt set, **delete all** **`PCE_Production`** rows for that well and **re-insert** — so **full per-well history** is refreshed, not only the 18-month window.  
6. Run **`sync_tc_to_production`**.

**Caution:** Step 5 can rewrite **entire** production history for ordinary wells in the rebuild set. Cancellation may be **best-effort**; partial commits are possible.

**Full rebuild — what happens (summary)**

1. Repaint Gas S2, gas sales, condensate sales, and Sales CGR on **`PCE_CDA`** from **`Allocation_Factors`** (when rows exist).  
2. Delete **all** **`PCE_Production`** and rebuild from **all** **`PCE_CDA`**.  
3. **No** Snowflake.  
4. Run **`sync_tc_to_production`**.

**Procedure:** Open the dialog → confirm **Overview** and mode → **Run Update** → confirm → watch **Results** and the progress bar → **Close** (with cancel warning if a job is still running).
