"""Tests for PCE_WM additional-field mapping and Excel header normalization."""

import unittest
from datetime import date

from well_master_db import ADDITIONAL_FIELD_COLUMNS, WellMasterDB

from scripts.backfill_wm_additional_fields import (
    normalize_excel_header,
    dedupe_first_per_uwi,
    _EXCEL_HEADER_TO_KEY,
)


class TestAdditionalFieldCoercion(unittest.TestCase):
    def test_float_blank_becomes_none(self):
        self.assertIsNone(WellMasterDB._coerce_additional_float(""))
        self.assertIsNone(WellMasterDB._coerce_additional_float("  "))
        self.assertIsNone(WellMasterDB._coerce_additional_float("-"))
        self.assertIsNone(WellMasterDB._coerce_additional_float("n/a"))

    def test_float_parses_commas(self):
        self.assertAlmostEqual(WellMasterDB._coerce_additional_float("1,234.5"), 1234.5)

    def test_int_parses_from_float_string(self):
        self.assertEqual(WellMasterDB._coerce_additional_int("12.0"), 12)

    def test_date_iso_format(self):
        self.assertEqual(
            WellMasterDB._coerce_additional_date("2024-03-15"),
            date(2024, 3, 15),
        )

    def test_date_slash_format(self):
        self.assertEqual(
            WellMasterDB._coerce_additional_date("03/15/2024"),
            date(2024, 3, 15),
        )

    def test_format_date_for_ui(self):
        self.assertEqual(
            WellMasterDB._format_additional_value_for_ui(date(2024, 1, 2), "date"),
            "2024-01-02",
        )


class TestAdditionalFieldColumnMap(unittest.TestCase):
    def test_columns_defined(self):
        self.assertEqual(len(ADDITIONAL_FIELD_COLUMNS), 22)

    def test_excel_header_aliases_cover_wm_keys(self):
        wm_keys = {key for key, _sql, _typ in ADDITIONAL_FIELD_COLUMNS}
        mapped_keys = {v for v in _EXCEL_HEADER_TO_KEY.values() if v}
        self.assertTrue(wm_keys.issubset(mapped_keys))


class TestNormalizeExcelHeader(unittest.TestCase):
    def test_wrapped_words_joined(self):
        self.assertEqual(
            normalize_excel_header("Bottom\nHole\nLatitude"),
            "bottom hole latitude",
        )

    def test_extra_spaces_collapsed(self):
        self.assertEqual(
            normalize_excel_header("  Surface   Hole  Longitude  "),
            "surface hole longitude",
        )

    def test_maps_to_known_key(self):
        norm = normalize_excel_header("Bottom\nHole\nLatitude")
        self.assertEqual(_EXCEL_HEADER_TO_KEY.get(norm), "bottom_hole_latitude")


class TestDedupeFirstPerUwi(unittest.TestCase):
    def test_keeps_first_row_per_uwi(self):
        import pandas as pd

        data = pd.DataFrame(
            [
                ["UWI-1", 1.0],
                ["UWI-2", 2.0],
                ["UWI-1", 9.0],
                ["", 3.0],
                ["UWI-2", 8.0],
            ]
        )
        deduped, skipped = dedupe_first_per_uwi(data, uwi_col=0)
        self.assertEqual(skipped, 2)
        self.assertEqual(len(deduped), 2)
        self.assertEqual(deduped.iloc[0, 0], "UWI-1")
        self.assertEqual(deduped.iloc[0, 1], 1.0)
        self.assertEqual(deduped.iloc[1, 0], "UWI-2")
        self.assertEqual(deduped.iloc[1, 1], 2.0)


if __name__ == "__main__":
    unittest.main()
