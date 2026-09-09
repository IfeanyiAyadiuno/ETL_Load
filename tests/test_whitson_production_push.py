"""Tests for Whitson production push helpers (no live SQL or API)."""

import tempfile
import unittest
from datetime import date
from pathlib import Path

import pandas as pd

from prodview_date_bounds import prodview_effective_end_date
from whitson_imperial_units import (
    WhitsonImperialConfigError,
    apply_imperial_to_rates,
    build_payload_point,
    load_whitson_imperial_factors,
)
from whitson_production_push import (
    build_whitson_payload,
    effective_end_date,
)

_SAMPLE_INI = """\
[metric_to_imperial]
gathered_gas_e3m3_per_day_to_mcf_per_day = 35.49373
condensate_m3_per_day_to_bbl_per_day = 6.29287
gath_water_m3_per_day_to_bbl_per_day = 6.29010
tubing_pressure_kpa_to_psi = 0.145038
casing_pressure_kpa_to_psi = 0.145038
choke_size_multiplier = 1.0
"""


class TestWhitsonImperialIni(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.ini_path = Path(self._tmpdir.name) / "whitson_imperial.ini"
        self.ini_path.write_text(_SAMPLE_INI, encoding="utf-8")

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_load_factors(self):
        f = load_whitson_imperial_factors(self.ini_path)
        self.assertAlmostEqual(f.gas_e3m3_to_mcf, 35.49373)
        self.assertAlmostEqual(f.water_m3_to_bbl, 6.29010)
        self.assertEqual(f.choke_multiplier, 1.0)

    def test_missing_required_raises(self):
        bad = Path(self._tmpdir.name) / "bad.ini"
        bad.write_text("[metric_to_imperial]\n", encoding="utf-8")
        with self.assertRaises(WhitsonImperialConfigError):
            load_whitson_imperial_factors(bad)

    def test_apply_imperial_rates(self):
        f = load_whitson_imperial_factors(self.ini_path)
        out = apply_imperial_to_rates(
            gathered_gas_e3m3=1.0,
            cond_m3=2.0,
            gath_water_m3=3.0,
            tubing_kpa=100.0,
            casing_kpa=200.0,
            choke=5.0,
            factors=f,
        )
        self.assertAlmostEqual(out["qg_sc"], 35.49373)
        self.assertAlmostEqual(out["qo_sc"], 2.0 * 6.29287)
        self.assertAlmostEqual(out["qw_sc"], 3.0 * 6.29010)
        self.assertAlmostEqual(out["p_tubing"], 100.0 * 0.145038)

    def test_gath_water_not_alloc_in_payload(self):
        f = load_whitson_imperial_factors(self.ini_path)
        row = {
            "Gathered Gas (e³m³/d)": 1.0,
            "Condensate WH (m³/d)": 1.0,
            "Gath. Water Rate (m³/d)": 10.0,
            "Alloc. Water Rate (m³)": 999.0,
            "Tubing Pressure (kPa)": None,
            "Casing Pressure (kPa)": None,
            "Choke Size": None,
        }
        pt = build_payload_point(row, f, date_iso="2024-01-01T00:00:00.000000Z")
        self.assertAlmostEqual(pt["qw_sc"], 10.0 * 6.29010)

    def test_build_whitson_payload_from_dataframe(self):
        f = load_whitson_imperial_factors(self.ini_path)
        df = pd.DataFrame(
            [
                {
                    "Date": pd.Timestamp("2024-06-01"),
                    "Gathered Gas (e³m³/d)": 2.0,
                    "Condensate WH (m³/d)": 1.0,
                    "Gath. Water Rate (m³/d)": 0.5,
                    "Tubing Pressure (kPa)": 10.0,
                    "Casing Pressure (kPa)": 20.0,
                    "Choke Size": 3.0,
                }
            ]
        )
        payload = build_whitson_payload(df, f)
        self.assertEqual(len(payload), 1)
        self.assertIn("2024-06-01", payload[0]["date"])
        self.assertAlmostEqual(payload[0]["qg_sc"], 2.0 * 35.49373)


class TestEffectiveEndDate(unittest.TestCase):
    def test_cap_when_after_prodview(self):
        far = date(2099, 1, 1)
        capped = effective_end_date(far, apply_prodview_cap=True)
        self.assertEqual(capped, prodview_effective_end_date())

    def test_no_cap(self):
        d = date(2020, 1, 1)
        self.assertEqual(effective_end_date(d, apply_prodview_cap=False), d)


if __name__ == "__main__":
    unittest.main()
