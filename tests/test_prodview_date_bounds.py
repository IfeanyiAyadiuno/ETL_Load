"""Calendar bounds for Prodview quick update and full-rebuild caps."""

from datetime import date
from unittest.mock import patch

import prodview_date_bounds as pdb


def test_prodview_effective_end_date_respects_lag():
    with patch.object(pdb, "_today", return_value=date(2026, 4, 19)):
        assert pdb.prodview_effective_end_date() == date(2026, 4, 17)


def test_quick_update_date_range_18_months():
    with patch.object(pdb, "_today", return_value=date(2026, 4, 19)):
        s, e = pdb.quick_update_date_range()
        assert e == date(2026, 4, 17)
        assert s == date(2024, 10, 17)
