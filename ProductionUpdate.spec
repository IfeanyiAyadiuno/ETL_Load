# -*- mode: python ; coding: utf-8 -*-
#
# One-folder build (onedir): ProductionUpdate.exe + _internal/ beside it.
# Faster cold start than a single-file exe for PyQt + pandas.
# Ship the entire dist/ProductionUpdate/ folder (do not omit _internal).

a = Analysis(
    ['production_update_gui.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=[
        'PyQt5.sip',
        'pyodbc',
        'pandas',
        'numpy',
        'snowflake.connector',
        'monthly_forecasts_import',
        'monthly_forecasts_import_dialog',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='ProductionUpdate',
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
<<<<<<< HEAD

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
=======
coll = COLLECT(
    exe,
    a.binaries,
>>>>>>> f5ee999d712ff5d5d8c35142a0ad09dec93a524a
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='ProductionUpdate',
)
