"""Tests for NGL daily compare helpers (no SQL / Excel file)."""

import unittest

import pandas as pd

from datetime import date

from ngl_daily_compare import (
    build_staging_insert_rows,
    compute_daily_ngl_columns,
    compute_fraction_value,
    compute_ratio_value,
    days_in_month,
    find_unmatched_excel_uwis,
    find_unmatched_prod_uwis,
    normalize_uwi,
    parse_production_date,
    trim_sql_uwi_for_match,
    uwi_match_key,
)


class TestNglDailyCompare(unittest.TestCase):
    def test_parse_production_date_yyyymm(self):
        self.assertEqual(parse_production_date(202208), (2022, 8))
        self.assertEqual(parse_production_date("202209"), (2022, 9))

    def test_parse_production_date_invalid(self):
        with self.assertRaises(ValueError):
            parse_production_date("")

    def test_normalize_uwi(self):
        self.assertEqual(normalize_uwi("  100/16-28-084-25W6/0  "), "100/16-28-084-25W6/0")
        self.assertIsNone(normalize_uwi(None))
        self.assertIsNone(normalize_uwi("   "))

    def test_trim_sql_uwi_for_match(self):
        self.assertEqual(
            trim_sql_uwi_for_match("1100/16-28-084-25W6/0"),
            "100/16-28-084-25W6/0",
        )
        self.assertEqual(
            trim_sql_uwi_for_match("100/16-28-084-25W6/0"),
            "00/16-28-084-25W6/0",
        )
        self.assertEqual(
            trim_sql_uwi_for_match("200/b-049-D/094-A-05/2\n"),
            "00/b-049-D/094-A-05/2",
        )
        self.assertEqual(
            trim_sql_uwi_for_match("02/a-028-I/094-B-08/0"),
            "02/a-028-I/094-B-08/0",
        )
        self.assertEqual(
            trim_sql_uwi_for_match("102/05-32-084-25W6/0"),
            "02/05-32-084-25W6/0",
        )
        self.assertEqual(
            trim_sql_uwi_for_match("200/a-056-A/094-B-09/2"),
            "00/a-056-A/094-B-09/2",
        )
        self.assertEqual(
            trim_sql_uwi_for_match("100/01-06-085-25W6/0"),
            "00/01-06-085-25W6/0",
        )
        self.assertEqual(
            trim_sql_uwi_for_match("100/10-30-084-25W6/0"),
            "00/10-30-084-25W6/0",
        )

    def test_uwi_match_key_case_insensitive(self):
        self.assertEqual(
            uwi_match_key("00/b-049-d/094-a-05/2", strip_leading_digit=False),
            uwi_match_key("00/B-049-D/094-A-05/2", strip_leading_digit=False),
        )
        self.assertEqual(
            uwi_match_key("200/B-049-D/094-A-05/2", strip_leading_digit=True),
            uwi_match_key("00/b-049-d/094-a-05/2", strip_leading_digit=False),
        )

    def test_compute_ratio(self):
        self.assertAlmostEqual(
            compute_ratio_value(176.6, 1000.0, 50.0),
            8.83,
            places=2,
        )

    def test_compute_ratio_zero_gas(self):
        self.assertIsNone(compute_ratio_value(176.6, 0.0, 50.0))

    def test_compute_fraction_august(self):
        self.assertAlmostEqual(
            compute_fraction_value(176.6, 2022, 8),
            176.6 / days_in_month(2022, 8),
            places=6,
        )

    def test_compute_daily_ngl_columns_ratio_and_fraction(self):
        monthly = pd.DataFrame(
            [
                {
                    "Uwi": "UWI-1",
                    "Year": 2022,
                    "Month": 8,
                    "NGL-C2": 176.6,
                    "NGL-C3": 0.0,
                    "NGL-C4": 0.0,
                    "NGL-C5": 0.0,
                    "PA_NGLs": 0.0,
                }
            ]
        )
        prod = pd.DataFrame(
            [
                {"Uwi": "UWI-1", "ProdDate": pd.Timestamp("2022-08-01"), "GatheredGas": 100.0},
                {"Uwi": "UWI-1", "ProdDate": pd.Timestamp("2022-08-02"), "GatheredGas": 300.0},
            ]
        )
        out = compute_daily_ngl_columns(prod, monthly)
        self.assertAlmostEqual(out.loc[0, "NGL-C2_R"], 176.6 / 400 * 100, places=6)
        self.assertAlmostEqual(out.loc[1, "NGL-C2_R"], 176.6 / 400 * 300, places=6)
        expected_f = 176.6 / 31
        self.assertAlmostEqual(out.loc[0, "NGL-C2_F"], expected_f, places=6)
        self.assertAlmostEqual(out.loc[1, "NGL-C2_F"], expected_f, places=6)

    def test_sql_uwi_leading_digit_matches_excel(self):
        monthly = pd.DataFrame(
            [
                {
                    "Uwi": uwi_match_key("100/16-28-084-25W6/0"),
                    "Year": 2022,
                    "Month": 8,
                    "NGL-C2": 31.0,
                    "NGL-C3": 0.0,
                    "NGL-C4": 0.0,
                    "NGL-C5": 0.0,
                    "PA_NGLs": 0.0,
                }
            ]
        )
        prod = pd.DataFrame(
            [
                {
                    "UwiRaw": "1100/16-28-084-25W6/0",
                    "Uwi": uwi_match_key("1100/16-28-084-25W6/0", strip_leading_digit=True),
                    "ProdDate": pd.Timestamp("2022-08-15"),
                    "GatheredGas": 100.0,
                }
            ]
        )
        out = compute_daily_ngl_columns(prod, monthly)
        self.assertFalse(pd.isna(out.loc[0, "NGL-C2_F"]))
        self.assertAlmostEqual(out.loc[0, "NGL-C2_F"], 31.0 / 31, places=6)

    def test_sql_uwi_100_bc_prefix_strips_to_00_matches_excel(self):
        monthly = pd.DataFrame(
            [
                {
                    "Uwi": uwi_match_key("00/10-30-084-25W6/0"),
                    "Year": 2022,
                    "Month": 8,
                    "NGL-C2": 31.0,
                    "NGL-C3": 0.0,
                    "NGL-C4": 0.0,
                    "NGL-C5": 0.0,
                    "PA_NGLs": 0.0,
                }
            ]
        )
        prod = pd.DataFrame(
            [
                {
                    "UwiRaw": "100/10-30-084-25W6/0",
                    "Uwi": uwi_match_key("100/10-30-084-25W6/0", strip_leading_digit=True),
                    "ProdDate": pd.Timestamp("2022-08-01"),
                    "GatheredGas": 50.0,
                }
            ]
        )
        out = compute_daily_ngl_columns(prod, monthly)
        self.assertFalse(pd.isna(out.loc[0, "NGL-C2_F"]))

    def test_sql_uwi_100_16_28_strips_to_00_matches_excel(self):
        monthly = pd.DataFrame(
            [
                {
                    "Uwi": uwi_match_key("00/16-28-084-25W6/0"),
                    "Year": 2022,
                    "Month": 8,
                    "NGL-C2": 31.0,
                    "NGL-C3": 0.0,
                    "NGL-C4": 0.0,
                    "NGL-C5": 0.0,
                    "PA_NGLs": 0.0,
                }
            ]
        )
        prod = pd.DataFrame(
            [
                {
                    "UwiRaw": "100/16-28-084-25W6/0",
                    "Uwi": uwi_match_key("100/16-28-084-25W6/0", strip_leading_digit=True),
                    "ProdDate": pd.Timestamp("2022-08-01"),
                    "GatheredGas": 50.0,
                }
            ]
        )
        out = compute_daily_ngl_columns(prod, monthly)
        self.assertFalse(pd.isna(out.loc[0, "NGL-C2_F"]))

    def test_sql_uwi_1100_alberta_strips_once_to_100(self):
        self.assertEqual(
            uwi_match_key("1100/16-28-084-25W6/0", strip_leading_digit=True),
            uwi_match_key("100/16-28-084-25W6/0", strip_leading_digit=False),
        )

    def test_sql_uwi_100_01_prefix_strips_to_00_matches_excel(self):
        monthly = pd.DataFrame(
            [
                {
                    "Uwi": uwi_match_key("00/01-06-085-25W6/0"),
                    "Year": 2022,
                    "Month": 8,
                    "NGL-C2": 31.0,
                    "NGL-C3": 0.0,
                    "NGL-C4": 0.0,
                    "NGL-C5": 0.0,
                    "PA_NGLs": 0.0,
                }
            ]
        )
        prod = pd.DataFrame(
            [
                {
                    "UwiRaw": "100/01-06-085-25W6/0",
                    "Uwi": uwi_match_key("100/01-06-085-25W6/0", strip_leading_digit=True),
                    "ProdDate": pd.Timestamp("2022-08-01"),
                    "GatheredGas": 50.0,
                }
            ]
        )
        out = compute_daily_ngl_columns(prod, monthly)
        self.assertFalse(pd.isna(out.loc[0, "NGL-C2_F"]))

    def test_sql_uwi_02_prefix_not_stripped_matches_excel(self):
        monthly = pd.DataFrame(
            [
                {
                    "Uwi": uwi_match_key("02/A-028-I/094-B-08/0"),
                    "Year": 2022,
                    "Month": 8,
                    "NGL-C2": 62.0,
                    "NGL-C3": 0.0,
                    "NGL-C4": 0.0,
                    "NGL-C5": 0.0,
                    "PA_NGLs": 0.0,
                }
            ]
        )
        prod = pd.DataFrame(
            [
                {
                    "UwiRaw": "02/a-028-I/094-B-08/0",
                    "Uwi": uwi_match_key("02/a-028-I/094-B-08/0", strip_leading_digit=True),
                    "ProdDate": pd.Timestamp("2022-08-10"),
                    "GatheredGas": 200.0,
                }
            ]
        )
        out = compute_daily_ngl_columns(prod, monthly)
        self.assertFalse(pd.isna(out.loc[0, "NGL-C2_F"]))

    def test_sql_uwi_case_insensitive_matches_excel(self):
        monthly = pd.DataFrame(
            [
                {
                    "Uwi": uwi_match_key("00/b-049-d/094-a-05/2"),
                    "Year": 2022,
                    "Month": 8,
                    "NGL-C2": 62.0,
                    "NGL-C3": 0.0,
                    "NGL-C4": 0.0,
                    "NGL-C5": 0.0,
                    "PA_NGLs": 0.0,
                }
            ]
        )
        prod = pd.DataFrame(
            [
                {
                    "UwiRaw": "200/B-049-D/094-A-05/2",
                    "Uwi": uwi_match_key("200/B-049-D/094-A-05/2", strip_leading_digit=True),
                    "ProdDate": pd.Timestamp("2022-08-10"),
                    "GatheredGas": 200.0,
                }
            ]
        )
        out = compute_daily_ngl_columns(prod, monthly)
        self.assertFalse(pd.isna(out.loc[0, "NGL-C2_F"]))
        self.assertAlmostEqual(out.loc[0, "NGL-C2_F"], 62.0 / 31, places=6)

    def test_find_unmatched_excel_uwis(self):
        monthly = pd.DataFrame(
            [
                {
                    "Uwi": uwi_match_key("100/16-28-084-25W6/0"),
                    "Year": 2022,
                    "Month": 8,
                    "NGL-C2": 10.0,
                    "NGL-C3": 0.0,
                    "NGL-C4": 0.0,
                    "NGL-C5": 0.0,
                    "PA_NGLs": 0.0,
                },
                {
                    "Uwi": uwi_match_key("MISSING-UWI"),
                    "Year": 2022,
                    "Month": 8,
                    "NGL-C2": 5.0,
                    "NGL-C3": 0.0,
                    "NGL-C4": 0.0,
                    "NGL-C5": 0.0,
                    "PA_NGLs": 0.0,
                },
            ]
        )
        prod = pd.DataFrame(
            [
                {
                    "UwiRaw": "1100/16-28-084-25W6/0",
                    "Uwi": uwi_match_key("1100/16-28-084-25W6/0", strip_leading_digit=True),
                    "ProdDate": pd.Timestamp("2022-08-01"),
                    "GatheredGas": 50.0,
                }
            ]
        )
        computed = compute_daily_ngl_columns(prod, monthly)
        matched, unmatched, not_in_prod = find_unmatched_excel_uwis(monthly, computed)
        self.assertEqual(matched, 1)
        self.assertEqual(unmatched, (uwi_match_key("MISSING-UWI"),))
        self.assertEqual(not_in_prod, (uwi_match_key("MISSING-UWI"),))

    def test_find_unmatched_prod_uwis(self):
        monthly = pd.DataFrame(
            [
                {
                    "Uwi": uwi_match_key("100/16-28-084-25W6/0"),
                    "Year": 2022,
                    "Month": 8,
                    "NGL-C2": 10.0,
                    "NGL-C3": 0.0,
                    "NGL-C4": 0.0,
                    "NGL-C5": 0.0,
                    "PA_NGLs": 0.0,
                }
            ]
        )
        prod = pd.DataFrame(
            [
                {
                    "UwiRaw": "1100/16-28-084-25W6/0",
                    "Uwi": uwi_match_key("1100/16-28-084-25W6/0", strip_leading_digit=True),
                    "ProdDate": pd.Timestamp("2022-08-01"),
                    "GatheredGas": 50.0,
                },
                {
                    "UwiRaw": "1999/99-99-999-99W9/9",
                    "Uwi": uwi_match_key("1999/99-99-999-99W9/9", strip_leading_digit=True),
                    "ProdDate": pd.Timestamp("2022-08-01"),
                    "GatheredGas": 25.0,
                },
            ]
        )
        computed = compute_daily_ngl_columns(prod, monthly)
        matched, unmatched, not_in_excel = find_unmatched_prod_uwis(monthly, computed)
        self.assertEqual(matched, 1)
        self.assertEqual(unmatched, ("1999/99-99-999-99W9/9",))
        self.assertEqual(not_in_excel, (uwi_match_key("1999/99-99-999-99W9/9", strip_leading_digit=True),))

    def test_build_staging_insert_rows(self):
        ngl_cols = [
            "NGL-C2_R",
            "NGL-C3_R",
            "NGL-C4_R",
            "NGL-C5_R",
            "PA_NGLs_R",
            "NGL-C2_F",
            "NGL-C3_F",
            "NGL-C4_F",
            "NGL-C5_F",
            "PA_NGLs_F",
        ]
        to_write = pd.DataFrame(
            [
                {
                    "UwiRaw": "100/10-30-084-25W6/0",
                    "ProdDate": pd.Timestamp("2022-08-01"),
                    "NGL-C2_R": 1.5,
                    "NGL-C3_R": None,
                    "NGL-C4_R": 0.0,
                    "NGL-C5_R": None,
                    "PA_NGLs_R": None,
                    "NGL-C2_F": 2.0,
                    "NGL-C3_F": None,
                    "NGL-C4_F": None,
                    "NGL-C5_F": None,
                    "PA_NGLs_F": None,
                }
            ]
        )
        rows = build_staging_insert_rows(to_write, ngl_cols)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "100/10-30-084-25W6/0")
        self.assertEqual(rows[0][1], date(2022, 8, 1))
        self.assertEqual(rows[0][2], 1.5)
        self.assertIsNone(rows[0][3])
        self.assertEqual(rows[0][4], 0.0)
        self.assertEqual(rows[0][7], 2.0)

    def test_no_excel_match_leaves_nan(self):
        monthly = pd.DataFrame(
            [
                {
                    "Uwi": "OTHER",
                    "Year": 2022,
                    "Month": 8,
                    "NGL-C2": 10.0,
                    "NGL-C3": 0.0,
                    "NGL-C4": 0.0,
                    "NGL-C5": 0.0,
                    "PA_NGLs": 0.0,
                }
            ]
        )
        prod = pd.DataFrame(
            [
                {"Uwi": "UWI-1", "ProdDate": pd.Timestamp("2022-08-01"), "GatheredGas": 50.0},
            ]
        )
        out = compute_daily_ngl_columns(prod, monthly)
        self.assertTrue(pd.isna(out.loc[0, "NGL-C2_R"]))


if __name__ == "__main__":
    unittest.main()
