"""PA UWI matching integration tests (NGL volumes now come from Allocation_Factors)."""

import unittest

import pandas as pd

from sales_allocation_updates import (
    production_well_name_from_wm,
    resolve_valnav_uwi_to_well_name,
)
from valnav_columns import resolve_valnav_ngl_columns


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

if __name__ == "__main__":
    unittest.main()
