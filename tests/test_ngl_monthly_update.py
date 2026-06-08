"""Tests for monthly NGL ratio helpers (no SQL / Excel file)."""

import unittest
from datetime import date

import pandas as pd

from ngl_monthly_update import (
    add_last_valid_ratio_coefs,
    apply_gas_hurdle_to_ratio,
    build_staging_insert_rows,
    compute_daily_ngl_ratio_columns,
    read_ngl_monthly_from_valnav,
    rolling_gathered_gas_avg,
)


class TestNglMonthlyUpdate(unittest.TestCase):
    def test_read_ngl_monthly_from_valnav_pa_match(self):
        df_valnav = pd.DataFrame(
            [
                {
                    "McDaniel database": "100/16-28-084-25W6/0",
                    "NGL-C2": 100.0,
                    "NGL-C3": 0.0,
                    "NGL-C4": 0.0,
                    "NGL-C5": 0.0,
                    "NGLs": 50.0,
                },
                {
                    "McDaniel database": "UNKNOWN-UWI",
                    "NGL-C2": 10.0,
                    "NGL-C3": 0.0,
                    "NGL-C4": 0.0,
                    "NGL-C5": 0.0,
                    "NGLs": 0.0,
                },
            ]
        )
        pce_uwi_dict = {"100/16-28-084-25w6/0": "Well-A"}
        col_ngl = {
            "NGL-C2": "NGL-C2",
            "NGL-C3": "NGL-C3",
            "NGL-C4": "NGL-C4",
            "NGL-C5": "NGL-C5",
            "NGLs": "NGLs",
        }
        out = read_ngl_monthly_from_valnav(
            df_valnav,
            col_uwi="McDaniel database",
            col_ngl=col_ngl,
            year=2022,
            month=8,
            pce_uwi_dict=pce_uwi_dict,
        )
        self.assertEqual(len(out), 1)
        self.assertEqual(out.iloc[0]["WellName"], "Well-A")
        self.assertAlmostEqual(out.iloc[0]["NGL-C2"], 100.0)
        self.assertAlmostEqual(out.iloc[0]["NGLs"], 50.0)

    def test_rolling_gathered_gas_avg_prior_three_months(self):
        prod = pd.DataFrame(
            [
                {
                    "WellName": "Well-A",
                    "ProdDate": pd.Timestamp("2022-05-15"),
                    "GatheredGas": 80.0,
                },
                {
                    "WellName": "Well-A",
                    "ProdDate": pd.Timestamp("2022-06-15"),
                    "GatheredGas": 100.0,
                },
                {
                    "WellName": "Well-A",
                    "ProdDate": pd.Timestamp("2022-07-15"),
                    "GatheredGas": 120.0,
                },
                {
                    "WellName": "Well-A",
                    "ProdDate": pd.Timestamp("2022-08-01"),
                    "GatheredGas": 50.0,
                },
            ]
        )
        avg = rolling_gathered_gas_avg(prod)
        self.assertTrue(pd.isna(avg.iloc[0]))
        self.assertAlmostEqual(avg.iloc[3], 100.0, places=6)

    def test_apply_gas_hurdle_normal_day_keeps_raw_r(self):
        df = pd.DataFrame(
            [
                {
                    "WellName": "Well-A",
                    "ProdDate": pd.Timestamp("2022-08-01"),
                    "GatheredGas": 100.0,
                    "NGL-C2_R": 25.0,
                    "RollingGasAvg": 100.0,
                }
            ]
        )
        out, replaced = apply_gas_hurdle_to_ratio(df, "NGL-C2_R")
        self.assertEqual(replaced, 0)
        self.assertAlmostEqual(out.iloc[0]["NGL-C2_R"], 25.0, places=6)

    def test_apply_gas_hurdle_spike_uses_previous_r(self):
        df = pd.DataFrame(
            [
                {
                    "WellName": "Well-A",
                    "ProdDate": pd.Timestamp("2022-08-01"),
                    "GatheredGas": 100.0,
                    "NGL-C2_R": 50.0,
                    "RollingGasAvg": 100.0,
                },
                {
                    "WellName": "Well-A",
                    "ProdDate": pd.Timestamp("2022-08-02"),
                    "GatheredGas": 600.0,
                    "NGL-C2_R": 300.0,
                    "RollingGasAvg": 100.0,
                },
            ]
        )
        out, replaced = apply_gas_hurdle_to_ratio(df, "NGL-C2_R")
        self.assertEqual(replaced, 1)
        spike_row = out.loc[out["ProdDate"] == pd.Timestamp("2022-08-02")].iloc[0]
        self.assertAlmostEqual(spike_row["NGL-C2_R"], 50.0, places=6)

    def test_apply_gas_hurdle_spike_without_previous_keeps_raw_r(self):
        df = pd.DataFrame(
            [
                {
                    "WellName": "Well-A",
                    "ProdDate": pd.Timestamp("2022-08-01"),
                    "GatheredGas": 600.0,
                    "NGL-C2_R": 300.0,
                    "RollingGasAvg": 100.0,
                }
            ]
        )
        out, replaced = apply_gas_hurdle_to_ratio(df, "NGL-C2_R")
        self.assertEqual(replaced, 0)
        self.assertAlmostEqual(out.iloc[0]["NGL-C2_R"], 300.0, places=6)

    def test_compute_daily_ngl_ratio_columns_gas_hurdle_spike(self):
        monthly = pd.DataFrame(
            [
                {
                    "WellName": "Well-A",
                    "Year": 2022,
                    "Month": 8,
                    "NGL-C2": 700.0,
                    "NGL-C3": 0.0,
                    "NGL-C4": 0.0,
                    "NGL-C5": 0.0,
                    "NGLs": 0.0,
                }
            ]
        )
        prod = pd.DataFrame(
            [
                {
                    "WellName": "Well-A",
                    "UwiRaw": "100/16-28-084-25W6/0",
                    "ProdDate": pd.Timestamp("2022-05-15"),
                    "GatheredGas": 100.0,
                },
                {
                    "WellName": "Well-A",
                    "UwiRaw": "100/16-28-084-25W6/0",
                    "ProdDate": pd.Timestamp("2022-06-15"),
                    "GatheredGas": 100.0,
                },
                {
                    "WellName": "Well-A",
                    "UwiRaw": "100/16-28-084-25W6/0",
                    "ProdDate": pd.Timestamp("2022-07-15"),
                    "GatheredGas": 100.0,
                },
                {
                    "WellName": "Well-A",
                    "UwiRaw": "100/16-28-084-25W6/0",
                    "ProdDate": pd.Timestamp("2022-08-01"),
                    "GatheredGas": 100.0,
                },
                {
                    "WellName": "Well-A",
                    "UwiRaw": "100/16-28-084-25W6/0",
                    "ProdDate": pd.Timestamp("2022-08-02"),
                    "GatheredGas": 600.0,
                },
            ]
        )
        out = compute_daily_ngl_ratio_columns(prod, monthly)
        aug1 = out.loc[out["ProdDate"] == pd.Timestamp("2022-08-01")].iloc[0]
        aug2 = out.loc[out["ProdDate"] == pd.Timestamp("2022-08-02")].iloc[0]
        self.assertAlmostEqual(aug1["NGL-C2_R"], 100.0, places=6)
        self.assertAlmostEqual(aug2["NGL-C2_R"], 100.0, places=6)

    def test_zero_ngl_uses_last_valid_ratio(self):
        monthly = pd.DataFrame(
            [
                {
                    "WellName": "Well-A",
                    "Year": 2022,
                    "Month": 8,
                    "NGL-C2": 400.0,
                    "NGL-C3": 0.0,
                    "NGL-C4": 0.0,
                    "NGL-C5": 0.0,
                    "NGLs": 0.0,
                },
                {
                    "WellName": "Well-A",
                    "Year": 2022,
                    "Month": 9,
                    "NGL-C2": 0.0,
                    "NGL-C3": 0.0,
                    "NGL-C4": 0.0,
                    "NGL-C5": 0.0,
                    "NGLs": 0.0,
                },
            ]
        )
        prod = pd.DataFrame(
            [
                {
                    "WellName": "Well-A",
                    "UwiRaw": "Well-A",
                    "ProdDate": pd.Timestamp("2022-08-15"),
                    "GatheredGas": 100.0,
                },
                {
                    "WellName": "Well-A",
                    "UwiRaw": "Well-A",
                    "ProdDate": pd.Timestamp("2022-09-15"),
                    "GatheredGas": 200.0,
                },
            ]
        )
        out = compute_daily_ngl_ratio_columns(prod, monthly)
        self.assertAlmostEqual(out.loc[0, "NGL-C2_R"], 400.0, places=6)
        self.assertAlmostEqual(out.loc[1, "NGL-C2_R"], 800.0, places=6)

    def test_add_last_valid_ratio_coefs_forward_fill(self):
        monthly = pd.DataFrame(
            [
                {
                    "WellName": "Well-A",
                    "Year": 2022,
                    "Month": 8,
                    "NGL-C2": 300.0,
                    "NGL-C3": 0.0,
                    "NGL-C4": 0.0,
                    "NGL-C5": 0.0,
                    "NGLs": 0.0,
                },
                {
                    "WellName": "Well-A",
                    "Year": 2022,
                    "Month": 9,
                    "NGL-C2": 0.0,
                    "NGL-C3": 0.0,
                    "NGL-C4": 0.0,
                    "NGL-C5": 0.0,
                    "NGLs": 0.0,
                },
            ]
        )
        prod_month_gas = pd.DataFrame(
            [
                {"WellName": "Well-A", "ProdYear": 2022, "ProdMonth": 8, "MonthGasSum": 300.0},
                {"WellName": "Well-A", "ProdYear": 2022, "ProdMonth": 9, "MonthGasSum": 600.0},
            ]
        )
        out = add_last_valid_ratio_coefs(monthly, prod_month_gas)
        aug = out[(out["ProdYear"] == 2022) & (out["ProdMonth"] == 8)].iloc[0]
        sep = out[(out["ProdYear"] == 2022) & (out["ProdMonth"] == 9)].iloc[0]
        self.assertAlmostEqual(aug["__NGL-C2_ratio_coef"], 1.0, places=6)
        self.assertAlmostEqual(sep["__NGL-C2_ratio_coef"], 1.0, places=6)

    def test_compute_daily_ngl_ratio_columns_ratio_only(self):
        monthly = pd.DataFrame(
            [
                {
                    "WellName": "Well-A",
                    "Year": 2022,
                    "Month": 8,
                    "NGL-C2": 176.6,
                    "NGL-C3": 0.0,
                    "NGL-C4": 0.0,
                    "NGL-C5": 0.0,
                    "NGLs": 0.0,
                }
            ]
        )
        prod = pd.DataFrame(
            [
                {
                    "WellName": "Well-A",
                    "UwiRaw": "Well-A",
                    "ProdDate": pd.Timestamp("2022-08-01"),
                    "GatheredGas": 100.0,
                },
                {
                    "WellName": "Well-A",
                    "UwiRaw": "Well-A",
                    "ProdDate": pd.Timestamp("2022-08-02"),
                    "GatheredGas": 300.0,
                },
            ]
        )
        out = compute_daily_ngl_ratio_columns(prod, monthly)
        self.assertAlmostEqual(out.loc[0, "NGL-C2_R"], 176.6 / 400 * 100, places=6)
        self.assertAlmostEqual(out.loc[1, "NGL-C2_R"], 176.6 / 400 * 300, places=6)
        self.assertNotIn("NGL-C2_F", out.columns)

    def test_build_staging_insert_rows_ratio_only(self):
        ngl_cols = [
            "NGL-C2_R",
            "NGL-C3_R",
            "NGL-C4_R",
            "NGL-C5_R",
            "PA_NGLs_R",
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

    def test_no_monthly_match_leaves_nan(self):
        monthly = pd.DataFrame(
            [
                {
                    "WellName": "Other-Well",
                    "Year": 2022,
                    "Month": 8,
                    "NGL-C2": 10.0,
                    "NGL-C3": 0.0,
                    "NGL-C4": 0.0,
                    "NGL-C5": 0.0,
                    "NGLs": 0.0,
                }
            ]
        )
        prod = pd.DataFrame(
            [
                {
                    "WellName": "Well-A",
                    "UwiRaw": "Well-A",
                    "ProdDate": pd.Timestamp("2022-08-01"),
                    "GatheredGas": 50.0,
                },
            ]
        )
        out = compute_daily_ngl_ratio_columns(prod, monthly)
        self.assertTrue(pd.isna(out.loc[0, "NGL-C2_R"]))


if __name__ == "__main__":
    unittest.main()
