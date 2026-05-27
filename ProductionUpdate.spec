# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['production_update_gui.py'],
    pathex=[],
    binaries=[],
<<<<<<< HEAD
    datas=[],
    hiddenimports=['PyQt5.sip', 'pyodbc', 'pandas', 'numpy', 'snowflake.connector'],
=======
    datas=[('images', 'images')],
    hiddenimports=[
        'PyQt5.sip',
        'pyodbc',
        'pandas',
        'numpy',
        'snowflake.connector',
        'monthly_forecasts_import',
        'monthly_forecasts_import_dialog',
    ],
>>>>>>> 3ca5e5cfbb80b7cbf57c1386ffb03c33104e0c63
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
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='ProductionUpdate',
)
