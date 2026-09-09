# Handoff file inventory

**Purpose:** What to keep vs remove when delivering this project as a **local VSCode / Windows folder** (not a GitHub repo).

**Legend**

| Necessary? | Meaning |
|------------|---------|
| **Yes — run (source)** | Required to run `python production_update_gui.py` from source |
| **Yes — run (exe)** | Must ship inside `dist/PCE_RE_Production_Update V2.4/` (or beside the `.exe`) |
| **Yes — local config** | Must exist on each PC; use `.example` templates to create (do not treat as public repo files) |
| **Optional — maintain** | App runs without it; keep for DBAs, developers, or documentation |
| **No — remove** | Sample data, caches, Git/IDE artifacts, or duplicates — delete before handoff |

**Handoff action**

| Action | Meaning |
|--------|---------|
| **Keep** | Leave in the delivered folder |
| **Remove** | Delete before handoff |
| **Create on site** | Copy from `.example`; fill with real values locally |
| **Network paths** | Live on shared drive; paths only in `settings.ini` |

---

## 1. Clean handoff checklist (no GitHub footprint)

Do this on the folder you zip or copy to the shared drive:

1. **Delete** `.git/`, `.gitignore`, `.gitattributes` (entire Git history and metadata).
2. **Delete** `.cursor/`, `.vscode/`, `.idea/` if present.
3. **Delete** all `__pycache__/`, `.pytest_cache/`, `build/`, `.DS_Store`, `Thumbs.db`.
4. **Delete** root sample Excel/CSV/PPT files (listed in §4).
5. **Delete** empty `sql/` folder and duplicate `requirements/` folder (not used by the app).
6. **Keep** source `.py`, `images/`, `whitson_imperial.ini`, `survey_mapping_presets.json`, templates, `docs/`, `scripts/`, `tests/` (optional but recommended for maintainers).
7. **Create on site** per PC: `settings.ini`, `.env`, `access_token.txt` (Whitson token file; created at runtime if missing).
8. **Optional:** Build exe with PyInstaller and hand off `dist/PCE_RE_Production_Update V2.4/` instead of (or in addition to) source.

Do **not** include real passwords in the delivered zip. Use `settings.ini.example` and `.env.example` only; recipient copies and fills locally.

---

## 2. Core application (Python) — **Yes — run (source)** · **Keep**

All modules imported (directly or indirectly) from `production_update_gui.py`. Removing any of these breaks the GUI or a menu action.

| File | Role |
|------|------|
| `production_update_gui.py` | Main entry point |
| `password_dialog.py`, `app_password.py` | Startup password gate |
| `app_paths.py` | Paths to `settings.ini`, logos, `whitson_imperial.ini` |
| `settings_dialog.py` | Settings UI |
| `styles.py`, `log_format.py` | Shared UI / log formatting |
| `db_connection.py`, `snowflake_connector.py`, `ssl_trust.py` | SQL Server & Snowflake |
| `well_master_gui.py`, `well_master_db.py`, `well_master_delegates.py`, `well_master_additional_fields_dialog.py` | Well Master |
| `prodview_update_dialog.py`, `prodview_update_gui.py`, `prodview_date_bounds.py` | Prodview / Snowflake |
| `production_update.py` | Full rebuild engine (also CLI) |
| `monthly_loader_dialog.py`, `monthly_loader_gui.py`, `valnav_columns.py` | ValNav (PA) |
| `ngl_monthly_update.py`, `ngl_allocation_load.py` | NGL ratios / bulk NGL load module |
| `sales_ratios_dialog.py`, `sales_ratios_gui.py`, `sales_allocation_updates.py` | Public Sales |
| `survey_import_dialog.py`, `survey_import.py`, `survey_mapping_dialog.py` | Survey import |
| `type_curves_import_dialog.py`, `type_curves_import.py`, `sync_typecurves_to_production.py` | Type curves |
| `monthly_forecasts_import_dialog.py`, `monthly_forecasts_import.py` | Monthly forecasts |
| `exports_dialog.py`, `exports_gathered_monthly.py` | Exports |
| `whitson_mass_upload_dialog.py`, `whitson_production_push.py`, `whitson_well_attributes.py` | Whitson+ push |
| `whitson_api.py`, `whitson_connect.py`, `whitson_credentials.py`, `whitson_imperial_units.py` | Whitson API / auth |
| `pce_production_schema.py`, `pce_rebuild_pipeline.py`, `pce_frcst_prd_rebuild.py` | Production rebuild pipeline |
| `pipelines/__init__.py` | Pipeline package export |
| `accumap_unmatched_cli.py` | `--accumap-unmatched` CLI from main entry |

---

## 3. Runtime assets & config — **Keep** (templates) / **Create on site** (secrets)

| Path | Necessary? | Handoff action | Notes |
|------|------------|----------------|-------|
| `images/pce_logo.png` | Yes — run (source + exe) | Keep | Main window logo |
| `images/PCE Icon white.jpg` | Yes — run (source + exe) | Keep | Header icon (`app_paths` expects this exact name) |
| `whitson_imperial.ini` | Yes — run (source + exe) | Keep | Metric→imperial factors for Whitson |
| `survey_mapping_presets.json` | Yes — run (source + exe) | Keep | Survey column mapping presets |
| `settings.ini.example` | Optional — maintain | Keep | Template only |
| `settings.ini` | Yes — local config | Create on site | Real SQL, `[WHITSON]`, `[PATHS]`; **do not ship secrets in zip** |
| `.env.example` | Optional — maintain | Keep | Template only |
| `.env` | Yes — local config | Create on site | Snowflake credentials |
| `access_token.txt` | Yes — local config | Create on site | Whitson OAuth cache; written at runtime |

Excel workbooks (ValNav, Accumap, Survey, etc.) are **not** in this repo for runtime — paths point to **network drives** via `[PATHS]` in `settings.ini`.

---

## 4. Root sample / scratch data — **No — remove**

These are **not** read by the application. Safe to delete before handoff.

| Path | Notes |
|------|-------|
| `Book1.ods`, `Book1.xlsx` | Scratch spreadsheets |
| `Copy of PCE-Wells-WM-Xfields.xlsx` | Sample / working copy |
| `Missing_Directional_Surveys.xlsx` | Sample |
| `NGL_summary table.xlsx`, `NGL_summary table-edited.xlsx` | Sample |
| `On_Prod_Year.xlsx` | Sample |
| `PCE_TCs.xlsx`, `PCE_TCs_MTHLY.xlsx`, `TC YE25.xlsx` | Sample type-curve files |
| `ProductionUpdate.pptx` | Presentation; not used by app |
| `Survey.xlsx`, `Wells_Surveys.xlsx`, `Whitson_Mass_Upload.xlsx` | Sample / local copies |
| `Well_Mapping.csv` | Reference data; not loaded from repo root |
| `a15_survey.csv`, `a15_survey.xlsx` | Sample survey |
| `test.xlsx` | Test scratch file |

---

## 5. Duplicate / unused folders — **No — remove**

| Path | Notes |
|------|-------|
| `requirements/` | Duplicate of `images/`, `settings.ini`, `whitson_imperial.ini` — **not referenced by code** |
| `sql/` | Empty directory; DDL lives under `scripts/*.sql` |

---

## 6. Git, IDE, and build artifacts — **No — remove**

| Path | Notes |
|------|-------|
| `.git/` | Remove for “never on GitHub” handoff |
| `.gitignore`, `.gitattributes` | Remove with `.git/` |
| `.cursor/`, `.vscode/`, `.idea/` | IDE / agent metadata |
| `__pycache__/`, `*.pyc` | Python cache |
| `.pytest_cache/` | Test cache |
| `build/`, `dist/` | PyInstaller output (keep `dist/` **only** if that *is* the delivered product) |
| `.DS_Store`, `Thumbs.db` | OS junk |

---

## 7. Dependencies & packaging — **Optional — maintain** · **Keep**

| Path | Necessary? | Notes |
|------|------------|-------|
| `requirements.txt` | Yes for source install | Runtime pip packages |
| `requirements-dev.txt` | Optional | pytest, pyinstaller |
| `PCE_RE_Production_Update V2.4.spec` | Optional | PyInstaller spec for `.exe` build |
| `pytest.ini` | Optional | Test runner config |

---

## 8. Documentation — **Optional — maintain** · **Keep**

| Path | Audience |
|------|----------|
| `README.md` | Quick start, packaging |
| `docs/PRODUCTION_UPDATE_GUIDE.md` | Operator manual |
| `docs/DATABASE_INDEXES.md` | DBA index notes |
| `docs/REFACTOR_CHANGELOG.md` | Developer refactor history |
| `docs/HANDOFF_FILE_INVENTORY.md` | This file |

| Path | Handoff |
|------|---------|
| `LICENSE` | Keep or remove per company policy (not required to run app) |

---

## 9. Tests — **Optional — maintain** · **Keep** (recommended)

| Path | Notes |
|------|-------|
| `tests/*.py` | Full pytest suite; not needed to **run** GUI |
| `tests/__pycache__/` | **Remove** (cache) |

Run before handoff: `pip install -r requirements-dev.txt` then `python -m pytest -q`

---

## 10. Scripts folder — **Optional — maintain** · **Keep**

Not imported by the GUI at startup; used for one-off DBA/IT tasks and CLI helpers.

| Path | Used by |
|------|---------|
| `scripts/*.sql` | SSMS migrations / one-time DDL (run manually) |
| `scripts/backfill_wm_additional_fields.py` | Bulk WM additional-fields from Excel |
| `scripts/whitson_upload.py` | Push one well to Whitson (CLI) |
| `scripts/ngl_allocation_load.py` | Bulk historical NGL Excel load (CLI wrapper) |
| `scripts/match_enersight_well_mapping.py` | Enersight name matching |
| `scripts/diagnose_gathered_monthly_export.py` | Support / debug |
| `scripts/diagnose_gathered_water_snowflake.py` | Support / debug |
| `scripts/__pycache__/` | **Remove** |

---

## 11. CLI utilities (root) — **Optional — maintain** · **Keep**

| Command | File |
|---------|------|
| Full Prodview rebuild (no GUI) | `production_update.py` |
| Survey import (no GUI) | `survey_import.py` |
| Purge exception wells | `purge_exception_wells.py` |
| Accumap unmatched audit | `accumap_unmatched_cli.py` (also via `production_update_gui.py --accumap-unmatched`) |

---

## 12. Deployed `.exe` bundle (what operators need)

If you hand off the **built** app instead of source, each PC needs this folder:

```
dist/PCE_RE_Production_Update V2.4/
├── PCE_RE_Production_Update V2.4.exe
├── _internal/                    # PyInstaller runtime (all Python + deps)
├── images/                       # logo + icon
├── whitson_imperial.ini
├── survey_mapping_presets.json
├── settings.ini                  # create on site
└── .env                          # create on site
```

`access_token.txt` appears next to the exe when Whitson+ is used.

Source code (`*.py`, `tests/`, `scripts/`, `docs/`) is **not** required on operator PCs if only the exe is deployed.

---

## 13. Summary counts (approximate)

| Category | Action |
|----------|--------|
| ~45 Python modules + `pipelines/` | **Keep** — app |
| 4 runtime assets (`images/`, `whitson_imperial.ini`, `survey_mapping_presets.json`) | **Keep** |
| 2 config templates (`.env.example`, `settings.ini.example`) | **Keep** |
| 3 local secrets (`settings.ini`, `.env`, `access_token.txt`) | **Create on site** |
| ~15 root Excel/CSV/PPT samples | **Remove** |
| `requirements/` duplicate + empty `sql/` | **Remove** |
| Git + IDE + caches | **Remove** |
| `docs/`, `scripts/`, `tests/`, `README.md` | **Keep** (maintainer handoff) |

---

*Internal use — Pacific Canbriam Energy LTD.*
