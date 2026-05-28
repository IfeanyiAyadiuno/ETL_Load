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


if __name__ == "__main__":
    unittest.main()
