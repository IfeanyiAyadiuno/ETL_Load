# Gamma — Production Update interface deck (4 cards)

**How to use:** In [Gamma](https://gamma.app), create a new deck and use **generate from text** (or paste into the card editor). A line containing **only** `---` starts a **new card** (Gamma’s card-by-card control).

**After generation:** Pick a clean, minimal **corporate** theme (strong typography, restrained color). Add screenshots from your machine; align filenames with **Appendix A** in [`USER_GUIDE.md`](../USER_GUIDE.md) (`figure-01-main-window.png` through `figure-10-whitson.png`).

**Guide cross-reference (while presenting):**

- **Title** — Opening, purpose, version.
- **2 — Operators** — §4 Main window, §5 Settings · Figures 1–2.
- **3 — Data flow** — § How the pieces fit together, §6–9, Application architecture · Figures 3–7.
- **4 — Imports / next** — §10–15, Runbook, Troubleshooting, Appendix A–C.

**For Gamma:** copy **only** the block below — from the line `# Production Update System` through the last line of the file (do not copy the instructions or this table).

# Production Update System
## Interface walkthrough (companion to the User Guide)

**Pacific Canbriam Energy LTD** · Desktop application · April 2026

Presenter notes: Keep **USER_GUIDE.md** open in the same folder. Use the Table of contents to jump to any module in seconds while you speak.

---

## How operators use the application

**Single hub:** The **main window** lists operations (Settings, Well Master, Prodview, PA, Public Sales, Survey, Type Curves, Whitson+, Exports). Selecting an operation opens a **modal dialog**; progress and messages appear in the **operation log** on the right.

**First stop — Settings:** SQL connection context and **file paths** (ValNav, Accumap, Survey, Type Curves, Whitson+) are saved to **`settings.ini`**. Most dialogs **read** those paths; they do not silently repoint disks.

**While presenting:** User Guide → **§4 Main window**, **§5 Settings** · Figures **1–2** (checklist in Appendix A).

---

## Data flow in three moves (what touches the database)

**1 — Well Master (`PCE_WM`):** Authoritative well list and Snowflake keys (**GasIDREC**, **PressuresIDREC**). New wells and corrections land here before reliable Snowflake pulls.

**2 — Prodview / Snowflake:** Refreshes **`PCE_CDA`** (and **`PCE_Production`** per mode) over a date range — the daily spine the rest of the month depends on.

**3 — Allocations:** **PA (ValNav)** then **Public Sales (Accumap)** update **`Allocation_Factors`** and the **sales / S2** columns on **`PCE_CDA`** and **`PCE_Production`** in a deliberate order (see guide for destructive vs. non-destructive steps).

**While presenting:** User Guide → **§ How the pieces fit together**, **§6–9**, **§ Application architecture** · Figures **3–7** for dialog captures.

---

## Imports, extensions, and where to read next

**Imports:** **Survey** and **Type Curves** load Excel/CSV into **`PCE_Surveys`** and **`PCE_TC`** respectively (append/overwrite or append/delete patterns — see guide). **Whitson+** is staged for future API wiring; **Exports / Reports** is a placeholder today.

**Operations discipline:** The **Runbook** gives recommended order, CLI utilities, and verification. **Troubleshooting** and **Appendices** (figures checklist, glossary, schema) support deeper Q&A without bloating this deck.

**While presenting:** User Guide → **§10–15**, **Appendix A** (screenshot checklist), **Appendix B–C** as needed.

**Close:** Questions → cite guide section + figure number for follow-up.
