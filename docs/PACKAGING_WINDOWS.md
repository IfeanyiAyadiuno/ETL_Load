# Packaging on Windows (PyInstaller)

This turns **`production_update_gui.py`** into a **folder** named `dist\ProductionUpdate\` containing **`ProductionUpdate.exe`** plus support files. Users run the **exe** from that folder (or a copy of the whole folder on a share).

Use **64-bit Python from [python.org](https://www.python.org/downloads/windows/)** (not the Microsoft Store “Python” app) on the machine that builds the exe.

---

## 1. One-time setup (build PC)

In **Command Prompt** or **PowerShell**, go to the project folder (where `production_update_gui.py` lives):

```bat
cd C:\path\to\your\project
python -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
pip install pyinstaller
```

---

## 2. Build (use the spec file)

The project folder already includes **`ProductionUpdate.spec`** (it lists hidden imports and bundles **`settings.ini`**). After the step above:

```bat
pyinstaller --clean ProductionUpdate.spec
```

**Output:** `dist\ProductionUpdate\ProductionUpdate.exe` and a subfolder **`_internal`** (required — do not delete).

**Ship:** Copy the **entire** `dist\ProductionUpdate\` folder. Put **`.env`** next to the exe if it is not bundled (same folder as `ProductionUpdate.exe`). Adjust **`settings.ini`** in that folder if paths differ on the target PC.

---

## 3. If you must change the build

Edit **`ProductionUpdate.spec`** (then run the same `pyinstaller --clean ProductionUpdate.spec`):

- **`datas`** — extra files copied next to the app (same idea as `--add-data`).
- **`hiddenimports`** — add a module name if the exe starts then crashes with **ImportError** / **ModuleNotFoundError** in a traceback.

Then rebuild with the command in section 2.

---

## 4. Command line vs PowerShell

Long commands in **cmd** can use **`^`** at the end of a line to continue. In **PowerShell**, use **`` ` ``** (backtick) instead. If you are not editing the spec, you only need the short build command in section 2.

---

## 5. When the exe will not start

- **Window flashes, no message** — Temporarily turn on a console: in `ProductionUpdate.spec`, under `EXE(...)`, set `console=True`, rebuild, run the exe from **cmd**, read the error, then set `console=False` again.
- **“Failed to start embedded python interpreter”** — Build with **python.org** 64-bit Python; check antivirus did not quarantine `dist\` or `build\`; open `dist\ProductionUpdate\_internal\` and confirm **`python3xx.dll`** is present (if not, delete `build` and `dist`, rebuild).
- **DLL / VC++ errors on the PC that runs the exe** — Install **Microsoft Visual C++ Redistributable (x64)** on that PC.
- **ImportError / ModuleNotFoundError in a traceback** — Add the module to **`hiddenimports`** in `ProductionUpdate.spec` and rebuild.

**ODBC** for SQL Server is still required on **each user PC** — it is not bundled by PyInstaller.

---

## Why a folder build, not one file

**`--onedir`** (what the spec uses) starts faster than a single huge **`.exe`** that unpacks itself every launch. This app uses PyQt5 and pandas; a folder build is the usual choice.
