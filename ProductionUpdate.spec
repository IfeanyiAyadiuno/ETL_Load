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
    upx_exclude=[],
    runtime_tmpdir=None,
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
    strip=False,
    upx=True,
    upx_exclude=[],
    name='ProductionUpdate',
)
