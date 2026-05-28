"""Tests for PCE_CDA load helpers (sort order, no SQL)."""

import unittest

import pandas as pd

from production_update import _sort_cda_dataframe


class TestSortCdaDataframe(unittest.TestCase):
    def test_sort_order_matches_legacy_sql(self):
        df = pd.DataFrame(
            {
                "Source_Well_Name": ["B-1", "YE2-1", "A-1 - TC", "A-1"],
                "Date": ["2024-01-03", "2024-01-01", "2024-01-02", "2024-01-01"],
            }
        )
        out = _sort_cda_dataframe(df)
        self.assertEqual(list(out["Source_Well_Name"]), ["B-1", "A-1", "YE2-1", "A-1 - TC"])


if __name__ == "__main__":
    unittest.main()
