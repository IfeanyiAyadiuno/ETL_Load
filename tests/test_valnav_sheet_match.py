"""ValNav worksheet name resolution for PA monthly loader."""

import unittest
from datetime import datetime

from monthly_loader_gui import resolve_valnav_sheet_name


class TestResolveValnavSheetName(unittest.TestCase):
    def test_matches_abbreviated_month(self):
        month = datetime(2026, 4, 1)
        self.assertEqual(
            resolve_valnav_sheet_name(["Jan 2026", "Apr 2026", "May 2026"], month),
            "Apr 2026",
        )

    def test_matches_full_month_name(self):
        month = datetime(2026, 4, 1)
        self.assertEqual(
            resolve_valnav_sheet_name(["April 2026 data"], month),
            "April 2026 data",
        )

    def test_rejects_wrong_year(self):
        month = datetime(2026, 4, 1)
        with self.assertRaisesRegex(ValueError, "Apr 2026 is not in the ValNav Excel file"):
            resolve_valnav_sheet_name(["Apr 2025", "Mar 2026"], month)

    def test_missing_lists_available(self):
        month = datetime(2026, 4, 1)
        with self.assertRaises(ValueError) as ctx:
            resolve_valnav_sheet_name(["Jan 2026", "Feb 2026", "Mar 2026"], month)
        msg = str(ctx.exception)
        self.assertIn("Apr 2026 is not in the ValNav Excel file", msg)
        self.assertIn("Jan 2026, Feb 2026, Mar 2026", msg)


if __name__ == "__main__":
    unittest.main()
