# Code review: redundancies and optimization (behavior-preserving)

**Scope:** All Python modules in the project folder (root level next to `production_update_gui.py`). **Server DDL** for `PCE_TC` and optional views is DBA-maintained; the app ships Python-embedded SQL only.

**Inventory (Phase 1):** 35 files, **13,691** total lines (`wc -l`) before Phase 2; **13,693** after Phase 2 (`prodview_update_gui.py` +2 lines).

---

## Cross-file patterns (summary)

| Pattern | Verdict |
|--------|---------|
| `safe_float` / `get_string_value` in `type.py` vs `type_curves_import.py` | **Blocked merge** — different rounding precision and NaN handling; unifying changes numeric output. |
| Nested `def log(msg): (log_callback or print)(msg)` in `prodview_update_gui.py` | **Refactored (Phase 2)** — single `_emit_log` + `functools.partial`. |
| Repeated `executemany` / `fast_executemany` batches | **Gated (Phase 3)** — shared helper risky without tests (SQL shape, commits, batch sizes differ). |
| Dialog layout + `styles.configure_dialog_window_mode` | **No action** — already centralized; further DRY is marginal. |

---

## Per-file review (largest first)

### `well_master_gui.py`

**Lines:** 1686  
**Changes:** None.  
**Rationale:** Single large PyQt surface; real win is structural split (tabs/workers), not a safe mechanical delete—defer without behavior risk.

### `af.py`

**Lines:** 1142  
**Changes:** None.  
**Rationale:** Allocation-factor / Excel logic is domain-heavy; no redundant wrappers identified without deeper domain tests.

### `survey_import.py`

**Lines:** 926  
**Changes:** None.  
**Rationale:** Import pipeline and SQL batching are cohesive; centralizing with other modules risks import cycles and subtle row-order assumptions.

### `cda.py`

**Lines:** 777  
**Changes:** None.  
**Rationale:** Standalone Snowflake→CDA script style; overlaps conceptually with `prodview_update_gui` but merging paths would change operational entrypoints.

### `prodview_update_gui.py`

**Lines:** 718 (inventory) → **720** (after Phase 2).  
**Changes:** Phase 2 — module-level `_emit_log`, `from functools import partial`, and `log = partial(_emit_log, log_callback)` in `run_prodview_update` and `run_quick_update`.  
**Rationale:** Removes three identical nested `log` closures; net +2 lines for explicit helper (same runtime behavior).

### `monthly_loader_gui.py`

**Lines:** 607  
**Changes:** None.  
**Rationale:** ValNav month path touches `sales_allocation_updates`; no safe dedup without allocation regression tests.

### `prodview_update_dialog.py`

**Lines:** 599  
**Changes:** None.  
**Rationale:** Worker/thread UI wiring is standard; no duplicate logic beyond shared styles import.

### `type_curves_import_dialog.py`

**Lines:** 598  
**Changes:** None.  
**Rationale:** Recently aligned with checklist UX; no redundant abstractions flagged.

### `type_curves_import.py`

**Lines:** 592  
**Changes:** None.  
**Rationale:** `safe_float` / `get_string_value` intentionally local to TC pipeline; do not merge with `type.py` without equivalence proof.

### `production_update.py`

**Lines:** 546  
**Changes:** None.  
**Rationale:** Core CDA→Production and pandas calcs; `print`-based logging is intentional for CLI/full rebuild; changing to shared logger is cosmetic only—skipped.

### `production_update_gui.py`

**Lines:** 514  
**Changes:** None.  
**Rationale:** Thin launcher importing many dialogs; trimming imports risks breaking optional flows.

### `survey_import_dialog.py`

**Lines:** 452  
**Changes:** None.  
**Rationale:** Dialog-only; patterns match other `*_dialog.py` files without harmful duplication.

### `sales_ratios_dialog.py`

**Lines:** 436  
**Changes:** None.  
**Rationale:** Preflight + worker mirror other dialogs; business logic stays in `sales_ratios_gui`.

### `styles.py`

**Lines:** 434  
**Changes:** None.  
**Rationale:** Central style strings; edits are high-blast-radius for UI consistency.

### `monthly_loader_dialog.py`

**Lines:** 398  
**Changes:** None.  
**Rationale:** Standard worker pattern; no redundant wrappers.

### `sales_allocation_updates.py`

**Lines:** 394  
**Changes:** None.  
**Rationale:** SQL-heavy; plan forbids query changes in this pass.

### `survey_mapping_dialog.py`

**Lines:** 386  
**Changes:** None.  
**Rationale:** No changes needed.

### `whitson_mass_upload_dialog.py`

**Lines:** 378  
**Changes:** None.  
**Rationale:** No changes needed.

### `settings_dialog.py`

**Lines:** 361  
**Changes:** None.  
**Rationale:** Config persistence is user-facing; leave stable.

### `well_master_db.py`

**Lines:** 293  
**Changes:** None.  
**Rationale:** DB helpers; no duplicate public APIs found.

### `sales_ratios_gui.py`

**Lines:** 267  
**Changes:** None.  
**Rationale:** Month-loop orchestration; keep as single module for clarity.

### `gas_idrec_production_peek.py`

**Lines:** 189  
**Changes:** None.  
**Rationale:** Small CLI utility; no changes needed.

### `test_well_lookup.py`

**Lines:** 163  
**Changes:** None.  
**Rationale:** Ad-hoc test script; no changes needed.

### `accumap_unmatched_cli.py`

**Lines:** 156  
**Changes:** None.  
**Rationale:** CLI wrapper; no changes needed.

### `purge_exception_wells.py`

**Lines:** 116  
**Changes:** None.  
**Rationale:** Operational script; destructive SQL intentionally explicit.

### `log_format.py`

**Lines:** 112  
**Changes:** None.  
**Rationale:** Shared formatting helpers; already DRY.

### `snowflake_connector.py`

**Lines:** 105  
**Changes:** None.  
**Rationale:** Thin connector; no changes needed.

### `exports_dialog.py`

**Lines:** 103  
**Changes:** None.  
**Rationale:** Placeholder UI; no changes needed.

### `type.py`

**Lines:** 77  
**Changes:** None.  
**Rationale:** Legacy wrappers; `safe_float` differs from `type_curves_import.safe_float`—do not merge without tests.

### `db_connection.py`

**Lines:** 49  
**Changes:** None.  
**Rationale:** Minimal connection helper; no changes needed.

### `well_master_delegates.py`

**Lines:** 45  
**Changes:** None.  
**Rationale:** Qt delegates; no changes needed.

### `scripts/accumap_unmatched_uwis.py`

**Lines:** 28  
**Changes:** None.  
**Rationale:** Thin launcher; no changes needed.

### `well_master_cda_worker.py` (removed)

**Note:** Previously a thin `QThread` bridge to per-well CDA populate after Well Master import. **Removed:** new wells update **`PCE_WM`** only; users run **Prodview / Snowflake** manually to refresh **`PCE_CDA`** and **`PCE_Production`**.

### `app_paths.py`

**Lines:** 16  
**Changes:** None.  
**Rationale:** Path constants; no changes needed.

### `pyo.py`

**Lines:** 1  
**Changes:** None.  
**Rationale:** Trivial stub; no changes needed.

---

## Phase 3 (gated) — not executed

- **Unified batch insert helper** across `production_update`, `prodview_update_gui`, `cda`, `survey_import`, `monthly_loader_gui`, `type_curves_import`: deferred until shared tests and DBA sign-off on commit boundaries and batch sizes.
- **Merge `type.py` parsers into `type_curves_import`:** deferred until golden-file or unit tests prove identical floats/strings.

---

## Phase 2 verification (`prodview_update_gui.py`)

- `wc -l prodview_update_gui.py` → **720** lines  
- `python -m py_compile prodview_update_gui.py` → OK
