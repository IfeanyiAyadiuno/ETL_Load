"""ValNav NGL column resolution and PA UWI matching integration tests."""

import unittest

import pandas as pd

from ngl_monthly_update import read_ngl_monthly_from_valnav
from sales_allocation_updates import (
    production_well_name_from_wm,
    resolve_valnav_uwi_to_well_name,
)
from valnav_columns import resolve_valnav_ngl_columns, strip_valnav_column_names


class TestValnavNglIntegration(unittest.TestCase):
    def test_resolve_valnav_ngl_columns_full_set(self):
        df = pd.DataFrame(
            columns=[
                "McDaniel database",
                "NGL-C2",
                "NGL-C3",
                "NGL-C4",
                "NGL-C5",
                "NGLs",
            ]
        )
        col_ngl = resolve_valnav_ngl_columns(df)
        self.assertIsNotNone(col_ngl)
        self.assertEqual(set(col_ngl.keys()), {"NGL-C2", "NGL-C3", "NGL-C4", "NGL-C5", "NGLs"})

    def test_resolve_valnav_ngl_columns_missing_returns_none(self):
        df = pd.DataFrame(columns=["McDaniel database", "NGL-C2"])
        self.assertIsNone(resolve_valnav_ngl_columns(df))

    def test_resolve_valnav_uwi_to_well_name_leading_digit_strip(self):
        pce_uwi_dict = {"00/b-049-d/094-a-05/2": "Pad Well"}
        self.assertEqual(
            resolve_valnav_uwi_to_well_name("200/B-049-D/094-A-05/2", pce_uwi_dict),
            "Pad Well",
        )

    def test_production_well_name_from_wm_uses_composite(self):
        self.assertEqual(
            production_well_name_from_wm("PAD-A COMPOSITE", "WELL-A"),
            "PAD-A COMPOSITE",
        )
        self.assertEqual(
            production_well_name_from_wm(None, "WELL-B"),
            "WELL-B",
        )
        self.assertEqual(
            production_well_name_from_wm("  ", "WELL-C"),
            "WELL-C",
        )

    def test_resolve_valnav_uwi_to_well_name_exact_match(self):
        pce_uwi_dict = {"100/16-28-084-25w6/0": "AB Well"}
        self.assertEqual(
            resolve_valnav_uwi_to_well_name("100/16-28-084-25W6/0", pce_uwi_dict),
            "AB Well",
        )

    def test_read_ngl_monthly_from_valnav_end_to_end(self):
        df_valnav = pd.DataFrame(
            [
                {
                    "McDaniel database": "200/B-049-D/094-A-05/2",
                    "NGL-C2": 62.0,
                    "NGL-C3": 1.0,
                    "NGL-C4": 2.0,
                    "NGL-C5": 3.0,
                    "NGLs": 4.0,
                },
            ]
        )
        strip_valnav_column_names(df_valnav)
        pce_uwi_dict = {"00/b-049-d/094-a-05/2": "Survey Well"}
        col_ngl = resolve_valnav_ngl_columns(df_valnav)
        self.assertIsNotNone(col_ngl)
        monthly = read_ngl_monthly_from_valnav(
            df_valnav,
            col_uwi="McDaniel database",
            col_ngl=col_ngl,
            year=2022,
            month=8,
            pce_uwi_dict=pce_uwi_dict,
        )
        self.assertEqual(len(monthly), 1)
        row = monthly.iloc[0]
        self.assertEqual(row["WellName"], "Survey Well")
        self.assertAlmostEqual(row["NGL-C2"], 62.0)
        self.assertAlmostEqual(row["NGLs"], 4.0)


if __name__ == "__main__":
    unittest.main()
