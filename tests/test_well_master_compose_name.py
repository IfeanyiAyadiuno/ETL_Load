"""Well Master composite name from component fields."""

import unittest

from well_master_db import WellMasterDB


class TestWellMasterComposeName(unittest.TestCase):
    def test_compose_name_all_parts(self):
        self.assertEqual(
            WellMasterDB.compose_name("L-16", "Montney", "MF", "HZ"),
            "L-16 - Montney - MF - HZ",
        )

    def test_compose_name_missing_part(self):
        self.assertIsNone(WellMasterDB.compose_name("L-16", "Montney", "", "HZ"))

    def test_normalize_composite_value(self):
        self.assertIsNone(WellMasterDB._normalize_composite_value(None))
        self.assertIsNone(WellMasterDB._normalize_composite_value("  "))
        self.assertEqual(
            WellMasterDB._normalize_composite_value("  L-16 - Montney - MF - HZ  "),
            "L-16 - Montney - MF - HZ",
        )

    def test_out_of_sync_detected(self):
        stored = "L-16 - Old Layer - MF - HZ"
        computed = WellMasterDB.compose_name("L-16", "Montney", "MF", "HZ")
        self.assertNotEqual(
            WellMasterDB._normalize_composite_value(stored),
            computed,
        )

    def test_sanitize_text_field_trims_whitespace(self):
        self.assertEqual(
            WellMasterDB.sanitize_text_field("  02/a-028-I/094-B-08/0  "),
            "02/a-028-I/094-B-08/0",
        )
        self.assertIsNone(WellMasterDB.sanitize_text_field("   "))

    def test_sanitize_well_update_trims_all_text_fields(self):
        cleaned = WellMasterDB.sanitize_well_update(
            {
                "well_name": "  L-16  ",
                "value_nav_uwi": " 100/16-28-084-25W6/0 ",
                "pad_name": " Pad A ",
                "exception": " n ",
            }
        )
        self.assertEqual(cleaned["well_name"], "L-16")
        self.assertEqual(cleaned["value_nav_uwi"], "100/16-28-084-25W6/0")
        self.assertEqual(cleaned["pad_name"], "Pad A")
        self.assertEqual(cleaned["exception"], "N")

    def test_normalize_bounded_accepts_allowed_values(self):
        self.assertEqual(WellMasterDB.normalize_bounded("Bounded"), "Bounded")
        self.assertEqual(WellMasterDB.normalize_bounded(" unbounded "), "Unbounded")
        self.assertIsNone(WellMasterDB.normalize_bounded(""))
        self.assertIsNone(WellMasterDB.normalize_bounded(None))
        self.assertIsNone(WellMasterDB.normalize_bounded(1.0))

    def test_prepare_well_update_rejects_bounded_in_numeric_field(self):
        prepared, err = WellMasterDB._prepare_well_update_for_sql(
            {
                "well_name": "L-16",
                "lateral_length": "Bounded",
            }
        )
        self.assertIsNone(prepared)
        self.assertIn("must be a number", err)

    def test_prepare_well_update_normalizes_bounded(self):
        prepared, err = WellMasterDB._prepare_well_update_for_sql(
            {
                "well_name": "L-16",
                "bounded": "bounded",
                "lateral_length": 2500,
            }
        )
        self.assertIsNone(err)
        self.assertEqual(prepared["bounded"], "Bounded")
        self.assertEqual(prepared["lateral_length"], 2500.0)


if __name__ == "__main__":
    unittest.main()
