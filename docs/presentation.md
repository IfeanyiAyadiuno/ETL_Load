# Production Update System — Engineering Overview

> 10–15 minute talk for engineers and operations staff. Goal: explain
> what was built, why it exists, and how much time and risk it removes
> from daily and monthly production work.

---

## Slide 1 — Title

**Production Update System**

A centralized data platform for daily and monthly production at Pacific Canbriam Energy.

Presented by: <Your Name> · <Date>

**Speaker notes**
Quick framing: this project replaced an Excel-only workflow. We built two
things at once — a SQL Server data model that didn't exist before, and a
desktop application that loads and maintains it. The next 12 minutes are
about what that gives the team back.

---

## Slide 2 — The problem we were solving

**Everything lived in Excel.**

- No central database — each task had its own workbook
- Manual copy-paste between Snowflake, ValNav, Accumap, and spreadsheets
- Allocation factors and sales ratios derived by hand — easy to break
- Re-running a month was risky; results varied between engineers
- Onboarding meant "learn the spreadsheets," not "learn the data"

**Speaker notes**
The old process worked, but it depended on whoever owned the workbook.
A broken formula, a misaligned column, or two engineers editing copies
meant rerunning the whole month from scratch.

---

## Slide 3 — What we built

Two deliverables, designed to work together.

**A SQL Server data model**
- `PCE_WM` — Well Master
- `PCE_CDA` — daily allocation
- `PCE_Production` — daily production with sequences and cumulatives
- `Allocation_Factors` — monthly PA / Public Sales factors
- `PCE_Surveys`, `PCE_TC` — surveys and type curves

**A PyQt5 desktop application**
- One window, one operation per task
- Loads from Snowflake, ValNav, Accumap, and Excel
- Writes back to the SQL tables with logged, repeatable runs

One source of truth. One tool to keep it current.

**Speaker notes**
The database itself is part of the deliverable. Before this project, this
schema did not exist — production lived in disconnected workbooks. The
app gives the team a single place to drive every refresh.

---

## Slide 4 — How data flows

**Sources**
- Snowflake / Prodview (daily wellhead, gathered, pressures)
- ValNav (monthly S2 / condensate factors)
- Accumap (monthly public sales gas)
- Excel imports (surveys, type curves)

**Destination — SQL Server**
- `PCE_WM` → `PCE_CDA` → `PCE_Production`
- `Allocation_Factors` propagates into CDA / Production
- `PCE_Surveys` and `PCE_TC` are kept in sync with production

**Speaker notes**
Each source has exactly one ingestion path in the app, and each
ingestion path has a clear target table. There is no longer a "which
spreadsheet wins" question.

---

## Slide 5 — The eight operations

- **Well Master** — maintain `PCE_WM`; import new wells from Snowflake
- **Prodview / Snowflake** — daily production retrieve
- **PA Allocations** — monthly ValNav factors
- **Public Sales Data and Ratios** — Accumap sales gas + ratios
- **Survey Import** — directional / log surveys
- **Type Curves Import** — Excel type curves into `PCE_TC`
- **Whitson+ Mass Upload** — placeholder
- **Exports / Reports** — placeholder

**Speaker notes**
These map one-to-one to what the team already does each month. Whitson+
and Exports are placeholders for the next phase.

---

## Slide 6 — Daily flow: Prodview / Snowflake

- Default mode: **Snowflake → CDA + Production rebuild**
- Window is automatic: rolling **18 months**, ends at **today − 2 days**
- Six Snowflake queries pulled in one connection (ECF, Gas WH, CGR / water, WGR, pressures, allocation)
- Replaces matching `PCE_CDA` rows; rebuilds `PCE_Production` for affected wells
- Sequences, cumulatives, monthly averages, on-production year — all in code
- Type curves and exception wells are protected automatically

**Speaker notes**
This is the daily cadence. Engineers used to chase "which days do I
re-pull?" The app picks the window, deletes only what should change,
re-inserts in one go, and prints a summary line with row counts.

---

## Slide 7 — Monthly flow: PA + Public Sales

- **PA Allocations (ValNav)** writes monthly factors to `Allocation_Factors`
- **Public Sales** merges Accumap sales gas and recomputes sales CGR
- Allocation changes propagate to `PCE_CDA` and `PCE_Production` automatically
- Re-running PA **preserves existing `Sales_Gas`** so Accumap data isn't lost
- Existing month? The app deletes and rebuilds **that month** consistently

**Speaker notes**
The old month-end story was: do PA, then redo sales by hand. Now sales
is its own dialog, and re-running PA does not wipe public-sales data.
That cross-step interaction was the most common foot-gun in the
spreadsheet world.

---

## Slide 8 — Well Master, Surveys, Type Curves

- **Well Master** — Snowflake-driven new well preview; safe edits; composite-name resolution
- **Surveys** — UWI matching with composite-name preference; unmatched rows reported as a CSV
- **Type Curves** — Excel → `PCE_TC` → synced into `PCE_Production` at `ImportDate`
- Everything is logged, named, and repeatable

**Speaker notes**
These supporting imports used to involve as much careful spreadsheet
work as the main flow. They are routine now, and consistent across
engineers.

---

## Slide 9 — How this saves engineers time

| Task | Before (Excel only) | After |
|------|---------------------|-------|
| Daily Snowflake → CDA → Production | Half-day spreadsheet round-trip | One dialog, hands-off, logged summary |
| Monthly PA (ValNav) | Manual factor calc + manual propagation | One dialog; CDA / Production update automatically |
| Public Sales (Accumap) | Manual UWI matching; risk of erasing PA data | One dialog; unmatched UWIs reported; PA data preserved |
| New well onboarding | Update multiple workbooks | Add in Well Master, run Prodview when ready |
| Survey / Type Curve imports | Manual mapping and pasting | Bulk + mapped imports with audit reports |
| Re-running a month | Risky; could overwrite | Safe and idempotent |

Engineers stop doing data plumbing and spend their time on interpretation.

**Speaker notes**
This is the slide I want the room to remember. It is not about "fancy
software." It is about the recurring work the team had to do every
month, and how much of that is now automated.

---

## Slide 10 — Reliability and auditability

- Every run prints a header, step lines, and a summary with row counts and duration
- Cancel during long jobs is supported
- Date caps prevent writing into days Snowflake isn't ready for
- Exception-flagged wells in `PCE_WM` are skipped automatically
- Type-curve and YE2 well rows are protected during rebuilds
- Concurrent engineers work against the same SQL tables — no merging workbooks

**Speaker notes**
Quick reassurance: no silent overwrites; the app is verbose about what
it did; there are guardrails for the data we don't want touched; and
the database removes the "whose copy is correct" problem.

---

## Slide 11 — What's next

- **Whitson+ mass upload** — replace the current placeholder
- **Exports / reports** — first-class dialog, scheduled extracts
- **Automated reconciliation** between Snowflake, Accumap, and ValNav
- Smaller asks — better progress signals, more name-resolution rules
- Owners and timelines: <fill in for your team>

**Speaker notes**
Shipping value now, but there is a clear next phase. I want to
prioritize based on what causes the most manual work today — push items
at me after this talk.

---

## Slide 12 — Q&A

- Questions
- Where to file requests / bugs: <link or email>
- Documentation: `docs/USER_GUIDE.md` and `docs/DEV_GUIDE.md` in the repo

**Speaker notes**
Thanks. Two minutes of Q&A or grab me after.
