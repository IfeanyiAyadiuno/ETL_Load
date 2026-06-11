"""Tests for NGL Excel → Allocation_Factors bulk load helpers."""

import unittest
from datetime import date
from unittest.mock import patch

import pandas as pd

from ngl_allocation_load import (
    NGL_AF_FIELDS,
    month_start_date,
    parse_production_date,
    read_monthly_ngl_excel,
    resolve_excel_uwi_to_well_name,
)
from sales_allocation_updates import resolve_valnav_uwi_to_well_name


class TestNglAllocationLoad(unittest.TestCase):
    def test_parse_production_date_yyyymm(self):
        self.assertEqual(parse_production_date(202208), (2022, 8))
        self.assertEqual(parse_production_date("202208"), (2022, 8))

    def test_parse_production_date_timestamp(self):
        ts = pd.Timestamp("2022-08-15")
        self.assertEqual(parse_production_date(ts), (2022, 8))

    def test_month_start_date(self):
        self.assertEqual(month_start_date(2022, 8), date(2022, 8, 1))

    def test_resolve_excel_uwi_to_well_name(self):
        pce_uwi_dict = {"00/b-049-d/094-a-05/2": "Pad Well"}
        self.assertEqual(
            resolve_excel_uwi_to_well_name("200/B-049-D/094-A-05/2", pce_uwi_dict),
            "Pad Well",
        )
        self.assertEqual(
            resolve_valnav_uwi_to_well_name("200/B-049-D/094-A-05/2", pce_uwi_dict),
            "Pad Well",
        )

    @patch("ngl_allocation_load.pd.read_excel")
    def test_read_monthly_ngl_excel(self, mock_read_excel):
        mock_read_excel.return_value = pd.DataFrame(
            [
                {
                    "PRODUCTION_DATE": 202208,
                    "UWI": "100/16-28-084-25W6/0",
                    "NGL-C2": 10.0,
                    "NGL-C3": 1.0,
                    "NGL-C4": 2.0,
                    "NGL-C5": 3.0,
                    "PA_NGLs": 4.0,
                },
                {
                    "PRODUCTION_DATE": 202208,
                    "UWI": "",
                    "NGL-C2": 99.0,
                    "NGL-C3": 0.0,
                    "NGL-C4": 0.0,
                    "NGL-C5": 0.0,
                    "PA_NGLs": 0.0,
                },
            ]
        )
        out = read_monthly_ngl_excel("dummy.xlsx")
        self.assertEqual(len(out), 1)
        row = out.iloc[0]
        self.assertEqual(row["Uwi"], "100/16-28-084-25W6/0")
        self.assertEqual(row["Year"], 2022)
        self.assertEqual(row["Month"], 8)
        self.assertEqual(row["MonthStartDate"], date(2022, 8, 1))
        self.assertAlmostEqual(row["NGL_C2"], 10.0)
        self.assertAlmostEqual(row["PA_NGLs"], 4.0)
        self.assertEqual(len(NGL_AF_FIELDS), 5)


if __name__ == "__main__":
    unittest.main()
