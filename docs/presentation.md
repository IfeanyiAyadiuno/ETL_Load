# Production Update System — Engineering Overview

> 10–15 min talk for engineers and other staff. Goal: show what the app
> does, why it exists, and how much time and risk it removes from the
> monthly/daily workflow.

---

## Slide 1 — Title

- **Pacific Canbriam Energy — Production Update System**
- A single application for daily and monthly production data
- Presented by: <Your Name>, <Date>

**Screenshot:** _figure-01-main-window.png_ (the main window with the 8 operations).

**Speaker notes:**  
Quick context: this is the application replacing the spreadsheet-driven
process the team used for daily Snowflake pulls, monthly allocations,
public sales, surveys, and type curves. Goal of this session: 10
minutes on what it is and what it saves us; a few minutes for
questions.

---

## Slide 2 — The problem we’re solving

- Daily/monthly numbers were assembled in **multiple Excel workbooks**
- **Manual copy/paste** from Snowflake, Accumap, ValNav
- Allocation factors and sales ratios computed by hand → easy to break
- Hard to audit; hard to redo a month consistently
- Onboarding a new engineer was “learn the spreadsheet”

**Screenshot placeholder:** an example of the **old Excel workflow**
(if you have a sanitized snippet) or a folder full of spreadsheets.

**Speaker notes:**  
The old process worked, but it was fragile. A broken formula or a
mis-pasted column meant rerunning everything by hand. There was no
single source of truth and no easy way to repeat a month exactly.

---

## Slide 3 — What the app is, in one sentence

- One desktop app, one team, one source of truth
- Pulls **Snowflake** + **Accumap** + **ValNav** + **Type Curves**
- Writes to **SQL Server** tables we already use
- Each operation is a **dialog** with a clear progress + log

**Screenshot placeholder:** main window with the 8 operation buttons.

**Speaker notes:**  
Everything we need to keep CDA, Production, Allocation Factors,
Surveys, and Type Curves up to date is now behind one button per task.
Same database tables, same column names — we didn’t change the
downstream consumers.

---

## Slide 4 — How data flows (high level)

- **Snowflake** → daily wellhead, gathered, pressures
- **ValNav** → monthly S2 / condensate factors
- **Accumap** → monthly public sales gas
- **Type Curves Excel** → forecast rows
- Out: `PCE_WM`, `PCE_CDA`, `PCE_Production`, `Allocation_Factors`, `PCE_Surveys`, `PCE_TC`

**Screenshot placeholder:** simplified diagram (we can render the
mermaid one from the user guide as a PNG, or draw a 5-box sketch).

**Speaker notes:**  
You don’t need to memorize this. The takeaway: each data source has
**one** ingestion path in the app, and each ingestion path has a clear
target. No “which spreadsheet wins” questions.

---

## Slide 5 — The eight operations

- **Well Master** — maintain `PCE_WM`, import new wells from Snowflake
- **Prodview / Snowflake** — daily production retrieve
- **PA Allocations** — monthly ValNav factors
- **Public Sales Data and Ratios** — Accumap sales gas + ratios
- **Survey Import** — directional/log surveys
- **Type Curves Import** — Excel type curves into `PCE_TC`
- **Whitson+ Mass Upload** (placeholder) and **Exports** (placeholder)

**Screenshot placeholder:** the operations panel from the main window.

**Speaker notes:**  
These map 1-to-1 to what the team already does each month. Whitson and
Exports are placeholders for the next phase — we’ll get to that at the
end.

---

## Slide 6 — Daily flow: Prodview / Snowflake

- Default mode: **Snowflake → CDA + Production rebuild**
- Window is **automatic** (rolling ~18 months, ends “today minus a
  short lag”)
- Replaces matching `PCE_CDA` rows, rebuilds `PCE_Production` for
  affected wells, and runs the type-curve sync
- One click. One log. One summary line with row counts and duration

**Screenshot placeholder:** Prodview / Snowflake dialog mid-run, with
log lines visible.

**Speaker notes:**  
This is the daily cadence. Engineers used to chase “which days do I
re-pull?” The app now picks the right window, deletes only what should
change, and re-inserts in one go. No date pickers, no surprises.

---

## Slide 7 — Monthly flow: PA + Public Sales

- **PA Allocations (ValNav)** writes monthly factors to
  `Allocation_Factors`
- **Public Sales** merges Accumap sales gas, recomputes sales ratios
- Allocations propagate to `PCE_CDA` and `PCE_Production`
  automatically (S2 gas, gas sales, condensate sales, sales CGR)
- **Re-running PA preserves existing `Sales_Gas`** so Accumap data
  isn’t lost
- Existing month? App **deletes and rebuilds that month** consistently

**Screenshot placeholder:** PA dialog completion summary; Public
Sales dialog with month list.

**Speaker notes:**  
The month-end story used to be: do PA, then redo sales by hand. Now
the second step is a single dialog, and re-running the first one
doesn’t wipe the second’s data. This was a real foot-gun in the old
process.

---

## Slide 8 — Well Master, Surveys, Type Curves

- **Well Master** — Snowflake-driven new well import; safe edits;
  composite name resolution
- **Surveys** — UWI matching with composite-name preference; clear
  unmatched report
- **Type Curves** — Excel → `PCE_TC`; sync into `PCE_Production` at
  `ImportDate`; correct prefixing for non-YE2 rows
- Everything is **logged**, **named**, and **repeatable**

**Screenshot placeholder:** one screenshot per item if space allows
(or a 1×3 collage).

**Speaker notes:**  
These are the “supporting” imports, but they used to involve as much
careful spreadsheet work as the main flow. Now they are routine and
consistent.

---

## Slide 9 — How this saves engineers time

- Daily Snowflake pull: **minutes, hands-off** vs. multi-hour spreadsheet round-trip
- Month-end: PA + Public Sales runs in a **single sitting**, not a multi-day stitching exercise
- New well onboarding: **insert in Well Master**, then run Prodview when ready — no separate spreadsheet to update
- Re-running a month is **safe and idempotent** — no fear of breaking historical data
- Engineers focus on **interpretation**, not data plumbing

**Screenshot placeholder:** before/after timeline image (we can sketch
two horizontal bars: Excel vs App).

**Speaker notes:**  
This is the slide I want the room to remember. It’s not about “fancy
software,” it’s about the recurring work the team had to do every
single month, and how much of that is now automated and consistent.

---

## Slide 10 — Reliability and auditability

- Every run prints a **header**, **steps**, and a **summary** with
  row counts and duration
- **Cancel** during long jobs is supported
- **Date caps** prevent writing into days Snowflake isn’t ready for
- **Exception-flagged** wells in `PCE_WM` are skipped automatically
- Type curve and YE2 well rows are **protected** during rebuilds

**Screenshot placeholder:** log panel showing a successful run summary.

**Speaker notes:**  
Quick reassurance for the engineering audience: no silent overwrites,
the app is verbose about what it did, and there are guardrails for the
data we don’t want touched.

---

## Slide 11 — What’s next

- **Whitson+ mass upload** (currently placeholder)
- **Exports / reports** dialog (placeholder)
- More **automated reconciliation** between sources
- Smaller asks: better progress indicators, more name-resolution rules
- Owners and timelines — _<fill in for your team>_

**Screenshot placeholder:** Whitson and Exports buttons on the main
window (or a roadmap sketch).

**Speaker notes:**  
We’re shipping value now, but there’s a clear next phase. I want to
prioritize based on what causes the most manual work today — please
push items at me after this talk.

---

## Slide 12 — Q&A

- Questions
- Where to file requests / bugs: <link or email>
- User guide: `USER_GUIDE.md` in the repo

**Screenshot placeholder:** logo / closing.

**Speaker notes:**  
Thanks. Two minutes of Q&A or grab me after.
