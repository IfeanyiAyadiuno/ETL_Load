#!/usr/bin/env python3
"""
Thin launcher: same as production_update_gui.py --accumap-unmatched (terminal output).

Prints matched (Accumap UWI -> PCE_WM well) and unmatched UWIs; optional -o CSV with both.

From repo root (Accumap path from settings.ini if you omit -a):
  python scripts/accumap_unmatched_uwis.py -m "Aug 2025"
  python scripts/accumap_unmatched_uwis.py -m "Aug 2025" -a "I:/path/Accumap.xlsx" -o out.csv

From elsewhere:
  python /path/to/ETL_Load/scripts/accumap_unmatched_uwis.py -m "Aug 2025"
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_rp = str(_REPO_ROOT)
if _rp not in sys.path:
    sys.path.insert(0, _rp)

from accumap_unmatched_cli import main

if __name__ == "__main__":
    raise SystemExit(main())
