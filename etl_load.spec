# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec for ETL_Load (onedir). Build: python build.py"""
import os

block_cipher = None

_spec_dir = os.path.dirname(os.path.abspath(SPEC))

# Runtime files next to the executable (user can edit .env / settings.ini in dist folder)
_datas = [
    (os.path.join(_spec_dir, "settings.ini"), "."),
    (os.path.join(_spec_dir, "PCE_TCs_MTHLY.xlsx"), "."),
    (os.path.join(_spec_dir, "Whitson_Mass_Upload.xlsx"), "."),
]
_env_path = os.path.join(_spec_dir, ".env")
if os.path.isfile(_env_path):
    _datas.append((_env_path, "."))

# Project modules (covers dynamic / lazy imports)
_project_hidden = [
    "af",
    "cda",
    "db_connection",
    "log_format",
    "monthly_loader_dialog",
    "monthly_loader_gui",
    "prodview_update_dialog",
    "prodview_update_gui",
    "production_update",
    "purge_exception_wells",
    "query",
    "sales_ratios_dialog",
    "sales_ratios_gui",
    "snowflake_connector",
    "styles",
    "survey_import",
    "survey_import_dialog",
    "type",
    "type_curves_import_dialog",
    "update",
    "well_master_gui",
    "whitson_mass_upload_dialog",
]

hiddenimports = _project_hidden + [
    "pyodbc",
    "snowflake.connector",
    "pandas",
    "numpy",
    "openpyxl",
    "PyQt5",
    "PyQt5.QtCore",
    "PyQt5.QtGui",
    "PyQt5.QtWidgets",
    "dotenv",
]

excludes = [
    "matplotlib",
    "matplotlib.backends",
    "scipy",
    "IPython",
    "ipykernel",
    "jupyter",
    "notebook",
    "pytest",
    "tkinter",
]

a = Analysis(
    [os.path.join(_spec_dir, "production_update_gui.py")],
    pathex=[_spec_dir],
    binaries=[],
    datas=_datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=excludes,
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="ETL_Load",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    name="ETL_Load",
)
