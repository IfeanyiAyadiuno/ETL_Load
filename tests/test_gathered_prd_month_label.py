"""Gathered production [Month] label: Gath PRD + Enersight name minus 'well'."""

import unittest

from production_update import (
    GATH_PRD_MONTH_PREFIX,
    gathered_prd_month_label,
)


class TestGatheredPrdMonthLabel(unittest.TestCase):
    def test_l16_well(self):
        self.assertEqual(gathered_prd_month_label("L-16 Well"), "Gath PRD L-16")

    def test_case_insensitive_well(self):
        self.assertEqual(gathered_prd_month_label("L-16 WELL"), "Gath PRD L-16")

    def test_missing_enersight(self):
        self.assertEqual(gathered_prd_month_label(None), GATH_PRD_MONTH_PREFIX)
        self.assertEqual(gathered_prd_month_label(""), GATH_PRD_MONTH_PREFIX)

    def test_only_well_word(self):
        self.assertEqual(gathered_prd_month_label("Well"), GATH_PRD_MONTH_PREFIX)


if __name__ == "__main__":
    unittest.main()
