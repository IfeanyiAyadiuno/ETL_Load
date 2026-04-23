"""Prodview quick update uses rolling date bounds (see prodview_date_bounds)."""

from datetime import date
from unittest.mock import patch

import prodview_date_bounds as pdb


def test_quick_update_window_ordering():
    with patch.object(pdb, "_today", return_value=date(2026, 1, 10)):
        s, e = pdb.quick_update_date_range()
        assert s < e
