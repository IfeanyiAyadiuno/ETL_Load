"""Allocation_Factors month overlap for window-scoped sales refresh."""

from datetime import date
from unittest.mock import MagicMock

from production_update import _allocation_factor_months_overlapping


def test_allocation_factor_months_overlapping_filters_by_range():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        (date(2024, 10, 1),),
        (date(2024, 11, 1),),
        (date(2024, 12, 1),),
        (date(2025, 1, 1),),
    ]
    months = _allocation_factor_months_overlapping(
        mock_cursor,
        date(2024, 12, 15),
        date(2025, 1, 10),
    )
    assert months == [date(2024, 12, 1), date(2025, 1, 1)]
