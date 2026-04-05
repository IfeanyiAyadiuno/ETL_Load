# Packaging the Production Update app on Windows

Build a Windows `.exe` with **PyInstaller**. Use a **folder** build (`--onedir`) for faster startup than a single-file exe.

---

## Prerequisites

- **Python 3.10+ (64-bit)** from [python.org](https://www.python.org/downloads/windows/). Enable **Add python.exe to PATH** during install.
- **SQL Server ODBC driver** on target PCs (for `pyodbc`) — separate from packaging; install per IT standard.

---

## 1. Open a terminal in the project folder

Command Prompt or PowerShell:

```bat
cd C:\path\to\ETL_Load
```

---

## 2. Virtual environment and dependencies

```bat
python -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
pip install pyinstaller
```

---

## 3. Build (recommended: onedir + windowed)

```bat
pyinstaller --name ProductionUpdate --windowed --onedir --clean ^
  --add-data "settings.ini;." ^
  production_update_gui.py
```

**`--add-data`:** First path is the file on disk; second is the folder inside the bundle (`.` = next to the exe). Use a **semicolon** `;` as the separator on Windows.

- If `settings.ini` is not in the project root, remove that line or change the path.
- To ship extra files (templates, a default `.env`), add more lines, for example:

```bat
  --add-data "PCE_TCs_MTHLY.xlsx;." ^
  --add-data "Whitson_Mass_Upload.xlsx;." ^
```

---

## 4. Output and deployment

- Executable: `dist\ProductionUpdate\ProductionUpdate.exe`
- **Ship the entire** `dist\ProductionUpdate\` **folder**, not only the `.exe`.
- Place **`.env`** and **`settings.ini`** next to `ProductionUpdate.exe` if they are not bundled (the app resolves paths from the folder containing the exe when frozen).

---

## 5. Smoke test

Run `ProductionUpdate.exe` from `dist\ProductionUpdate\`. If it exits with an import error, add `--hidden-import` for the missing package or submodule and rebuild.

Example (extend as needed after the first error):

```bat
pyinstaller --name ProductionUpdate --windowed --onedir --clean ^
  --hidden-import pyodbc ^
  --hidden-import snowflake.connector ^
  --hidden-import dotenv ^
  --hidden-import openpyxl ^
  --hidden-import pandas ^
  --hidden-import numpy ^
  --hidden-import production_update ^
  --hidden-import prodview_update_gui ^
  --hidden-import cda ^
  production_update_gui.py
```

After a working command is stable, run `pyinstaller` once with `--clean` to generate `ProductionUpdate.spec`, then future builds can use:

```bat
pyinstaller --clean ProductionUpdate.spec
```

Edit the `.spec` file to persist `--add-data`, `hiddenimports`, and `excludes` (e.g. `matplotlib`, `tkinter`) instead of a long command line.

---

## 6. PowerShell line continuation

In **PowerShell**, use a backtick at the end of each line instead of `^`:

```powershell
pyinstaller --name ProductionUpdate --windowed --onedir --clean `
  --add-data "settings.ini;." `
  production_update_gui.py
```

---

## Notes

- Build on **Windows** to produce a Windows `.exe`.
- **One-file** (`--onefile`) is possible but usually **slower to start** (extracts to a temp folder). Prefer `--onedir` for this stack (PyQt5, pandas, ODBC).
