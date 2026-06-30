"""Tests for PCE_CDA load helpers (sort order, no SQL)."""

import unittest
from datetime import date

import pandas as pd

from production_update import _cda_select_sql, _sort_cda_dataframe


class TestCdaSelectSql(unittest.TestCase):
    def test_end_cap_pushed_to_sql(self):
        sql, params = _cda_select_sql(end_cap=date(2026, 4, 17))
        self.assertIn("ProdDate <= ?", sql)
        self.assertEqual(params, [date(2026, 4, 17)])

    def test_start_and_end_cap(self):
        sql, params = _cda_select_sql(
            start_date=date(2020, 1, 1),
            end_cap=date(2026, 4, 17),
        )
        self.assertIn("ProdDate >= ?", sql)
        self.assertIn("ProdDate <= ?", sql)
        self.assertEqual(params, [date(2020, 1, 1), date(2026, 4, 17)])


class TestSortCdaDataframe(unittest.TestCase):
    def test_sort_order_matches_legacy_sql(self):
        df = pd.DataFrame(
            {
                "Source_Well_Name": ["B-1", "YE2-1", "A-1 - TC", "A-1"],
                "Date": ["2024-01-03", "2024-01-01", "2024-01-02", "2024-01-01"],
            }
        )
        out = _sort_cda_dataframe(df)
        self.assertEqual(list(out["Source_Well_Name"]), ["A-1", "B-1", "YE2-1", "A-1 - TC"])


if __name__ == "__main__":
    unittest.main()
