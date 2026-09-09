"""Tests for gathered monthly export helpers (no live SQL)."""

import unittest

from exports_gathered_monthly import (
    UNITS_IMPERIAL,
    UNITS_METRIC,
    apply_imperial_volumes,
    finalize_export_dataframe,
    month_label,
    month_labels_between,
    parse_month_label,
    validate_month_range,
)
from pce_frcst_prd_rebuild import E3M3_TO_MCF, M3_TO_BBL_COND, M3_TO_BBL_WATER


class TestExportsGatheredMonthly(unittest.TestCase):
    def test_parse_month_label(self):
        self.assertEqual(parse_month_label("Jan 2024"), (2024, 1))
        self.assertEqual(parse_month_label("Dec 2025"), (2025, 12))

    def test_parse_month_label_invalid(self):
        with self.assertRaises(ValueError):
            parse_month_label("January 2024")
        with self.assertRaises(ValueError):
            parse_month_label("Jan")

    def test_month_labels_between_single_and_span(self):
        from datetime import date

        self.assertEqual(
            month_labels_between(date(2024, 6, 1), date(2024, 6, 1)),
            ["Jun 2024"],
        )
        self.assertEqual(
            month_labels_between(date(2024, 11, 1), date(2025, 2, 1)),
            ["Nov 2024", "Dec 2024", "Jan 2025", "Feb 2025"],
        )

    def test_month_labels_between_empty_when_max_before_min(self):
        from datetime import date

        self.assertEqual(
            month_labels_between(date(2025, 1, 1), date(2024, 12, 1)),
            [],
        )

    def test_validate_month_range(self):
        validate_month_range("Jan 2024", "Dec 2024")
        with self.assertRaises(ValueError):
            validate_month_range("Dec 2024", "Jan 2024")

    def test_apply_imperial_volumes(self):
        gas, cond, water = apply_imperial_volumes(10.0, 20.0, 30.0)
        self.assertAlmostEqual(gas, 10.0 * E3M3_TO_MCF)
        self.assertAlmostEqual(cond, 20.0 * M3_TO_BBL_COND)
        self.assertAlmostEqual(water, 30.0 * M3_TO_BBL_WATER)

    def test_finalize_export_dataframe_metric_headers(self):
        import pandas as pd

        raw = pd.DataFrame(
            {
                "UWI": ["U1"],
                "Composite Name": ["C1"],
                "Month": ["2024-01"],
                "SumGas": [1.0],
                "SumCond": [2.0],
                "SumWater": [3.0],
                "SumHoursOn": [744.0],
            }
        )
        out = finalize_export_dataframe(raw, UNITS_METRIC)
        self.assertIn("Gathered Gas (e³m³)", out.columns)
        self.assertEqual(out.loc[0, "Gathered Gas (e³m³)"], 1.0)
        self.assertEqual(out.loc[0, "Hours On (total)"], 744.0)

    def test_finalize_export_dataframe_imperial_conversion(self):
        import pandas as pd

        raw = pd.DataFrame(
            {
                "UWI": ["U1"],
                "Composite Name": [None],
                "Month": ["2024-02"],
                "SumGas": [10.0],
                "SumCond": [20.0],
                "SumWater": [30.0],
                "SumHoursOn": [500.0],
            }
        )
        out = finalize_export_dataframe(raw, UNITS_IMPERIAL)
        self.assertAlmostEqual(
            out.loc[0, "Gathered Gas (Mcf)"], 10.0 * E3M3_TO_MCF
        )
        self.assertEqual(out.loc[0, "Hours On (total)"], 500.0)
        self.assertAlmostEqual(
            out.loc[0, "Gathered Condensate (bbl)"], 20.0 * M3_TO_BBL_COND
        )
        self.assertAlmostEqual(
            out.loc[0, "Gathered Water (bbl)"], 30.0 * M3_TO_BBL_WATER
        )

    def test_month_label_roundtrip(self):
        y, m = parse_month_label(month_label(2023, 7))
        self.assertEqual((y, m), (2023, 7))


if __name__ == "__main__":
    unittest.main()
