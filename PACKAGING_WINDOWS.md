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
  --hidden-import sales_allocation_updates ^
  --hidden-import accumap_unmatched_cli ^
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

---

## Troubleshooting: “Failed to start embedded python interpreter”

That message comes from PyInstaller’s **bootloader** before your Python code runs. The interpreter DLL or another core file is missing, blocked, or not loadable.

### 1. Use a full python.org install (not Store / embedded)

Install **64-bit Python from [python.org](https://www.python.org/downloads/windows/)**. Avoid **Windows Store Python** and **embeddable** zip distributions for building; they often break PyInstaller analysis or the bundled runtime.

In a new terminal:

```bat
where python
python -c "import sys; print(sys.executable)"
```

Confirm it points to a normal install (e.g. `C:\Users\...\AppData\Local\Programs\Python\Python3xx\python.exe`), not Store or embeddable paths.

### 2. Antivirus / Defender

Security software often quarantines PyInstaller **bootloader** files (`runw.exe` / build output under `build\` and `dist\`) or DLLs in `_internal`.

- Check **Protection history** for quarantined items and restore them.
- Add exclusions for your **project folder** and **`dist\ProductionUpdate`** while testing.
- Reinstall PyInstaller if bootloaders were removed:  
  `pip uninstall pyinstaller -y && pip install pyinstaller`

### 3. Verify the onedir bundle

Open `dist\ProductionUpdate\`:

- There should be a folder named **`_internal`** (PyInstaller 5.13+ / 6.x).
- Inside `_internal`, confirm **`python3xx.dll`** exists (version matches your build Python).

If `_internal` is empty, incomplete, or `python3xx.dll` is missing, delete `build` and `dist`, then rebuild with `--clean`.

### 4. Microsoft Visual C++ Redistributable

The Python DLL depends on the **VC++ runtime**. On the PC **running** the exe, install the latest **x64** “Visual C++ Redistributable” from Microsoft if it is not already present.

### 5. Rebuild with a console to see the real error

Windowed apps hide errors. Temporarily build **with a console**:

```bat
pyinstaller --name ProductionUpdate --console --onedir --clean production_update_gui.py
```

Run `dist\ProductionUpdate\ProductionUpdate.exe` from **cmd**. If a different message appears (DLL load failed, path, etc.), use that text for the next fix.

### 6. Don’t run through odd paths

Run the exe from a **normal folder** on a local drive. Avoid **symlinks**, **network-only** locations, or **zip “extract and run”** without extracting the **whole** `ProductionUpdate` folder.

### 7. Same bitness everywhere

Use **64-bit Python** to build and run on **64-bit Windows**. Mixing 32-bit Python with 64-bit OS (or the reverse) causes hard-to-read loader failures.

---

If the console build starts but then crashes with a **Python** traceback, that is a separate issue (missing `--hidden-import`, etc.); use section **5. Smoke test** above.
