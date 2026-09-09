"""Tests for Whitson WM well attribute payload builders (no live SQL or API)."""

import unittest
import unittest.mock
from unittest.mock import MagicMock

from whitson_imperial_units import WhitsonImperialFactors
from whitson_well_attributes import (
    WellMetadata,
    _FETCH_WM_METADATA_SQL,
    build_whitson_well_create_payload,
    build_whitson_well_patch_payload,
    fetch_well_metadata_for_whitson,
    sync_whitson_well_attributes,
)

# Identity conversions so tests assert raw values, independent of the shipped INI.
_IDENTITY_FACTORS = WhitsonImperialFactors(
    gas_e3m3_to_mcf=1.0,
    cond_m3_to_bbl=1.0,
    water_m3_to_bbl=1.0,
    tubing_kpa_to_psi=1.0,
    casing_kpa_to_psi=1.0,
    choke_multiplier=1.0,
    fluid_pumped_m3_to_bbl=1.0,
    prop_pumped_tonnes_to_lb=1.0,
    lateral_length_m_to_ft=1.0,
)


class TestWhitsonWellAttributePayloads(unittest.TestCase):
    def test_fault_block_maps_to_sub_field_not_formation(self):
        meta = WellMetadata(
            formation="Montney",
            sub_field="North Fault",
        )
        create = build_whitson_well_create_payload(
            meta, project_id=1, name="PAD-A", uwi_api="100/01-01-001-01W6/0"
        )
        self.assertEqual(create["formation"], "Montney")
        self.assertEqual(create["sub_field"], "North Fault")
        self.assertNotIn("custom_attributes", create)

        patch = build_whitson_well_patch_payload(42, meta)
        self.assertEqual(patch["id"], 42)
        self.assertEqual(patch["sub_field"], "North Fault")

    def test_null_pad_name_omitted_from_create_and_patch(self):
        meta = WellMetadata(formation="Doe Creek", l_w=2500.0)
        create = build_whitson_well_create_payload(
            meta, project_id=5, name="WELL-1", uwi_api="UWI-1",
            factors=_IDENTITY_FACTORS,
        )
        self.assertNotIn("pad_name", create)
        self.assertEqual(create["l_w"], 2500.0)

        patch = build_whitson_well_patch_payload(9, meta, _IDENTITY_FACTORS)
        self.assertNotIn("pad_name", patch)
        self.assertEqual(patch["l_w"], 2500.0)

    def test_lateral_length_converted_metres_to_feet(self):
        meta = WellMetadata(l_w=1000.0)
        create = build_whitson_well_create_payload(
            meta, project_id=1, name="WELL-L", uwi_api=None
        )
        # Shipped factor is 3.28084 m -> ft.
        self.assertAlmostEqual(create["l_w"], 1000.0 * 3.28084, places=3)

    def test_surface_coordinates_on_create(self):
        meta = WellMetadata(surf_lat=55.123, surf_long=-120.456, pad_name="PAD-9")
        create = build_whitson_well_create_payload(
            meta, project_id=1, name="WELL-9", uwi_api=None
        )
        self.assertAlmostEqual(create["surf_lat"], 55.123)
        self.assertAlmostEqual(create["surf_long"], -120.456)
        self.assertEqual(create["pad_name"], "PAD-9")

    def test_toe_coordinates_on_create_and_patch(self):
        meta = WellMetadata(
            surf_lat=55.1,
            surf_long=-120.1,
            toe_lat=55.09,
            toe_long=-120.11,
        )
        create = build_whitson_well_create_payload(
            meta, project_id=1, name="WELL-TOE", uwi_api="UWI-1"
        )
        self.assertAlmostEqual(create["bothole_lat_toe"], 55.09)
        self.assertAlmostEqual(create["bothole_long_toe"], -120.11)

        patch = build_whitson_well_patch_payload(7, meta)
        self.assertEqual(patch["id"], 7)
        self.assertAlmostEqual(patch["bothole_lat_toe"], 55.09)
        self.assertAlmostEqual(patch["bothole_long_toe"], -120.11)

    def test_fetch_sql_prefers_new_coordinate_columns(self):
        self.assertIn(
            "COALESCE(wm.[Surface Hole Latitude], wm.[Surface Location Latitude (NAD83)])",
            _FETCH_WM_METADATA_SQL,
        )
        self.assertIn(
            "COALESCE(wm.[Bottom Hole Latitude], wm.[Bottom Location Latitude (NAD83)])",
            _FETCH_WM_METADATA_SQL,
        )

    def test_fetch_metadata_maps_toe_from_row_indices(self):
        conn = unittest.mock.MagicMock()
        cur = conn.cursor.return_value
        cur.fetchone.return_value = (
            "PAD-A",
            "Montney",
            "Block 1",
            2500.0,
            55.5,
            -120.5,
            55.4,
            -120.6,
            1500.0,
            450.0,
            "2024-01-15",
            "100/01-01-001-01W6/0",
        )
        meta = fetch_well_metadata_for_whitson(conn, "WELL-1")
        self.assertAlmostEqual(meta.surf_lat, 55.5)
        self.assertAlmostEqual(meta.surf_long, -120.5)
        self.assertAlmostEqual(meta.toe_lat, 55.4)
        self.assertAlmostEqual(meta.toe_long, -120.6)
        self.assertAlmostEqual(meta.fluid_pumped, 1500.0)
        self.assertAlmostEqual(meta.prop_pumped, 450.0)
        self.assertEqual(meta.first_prod_date, "2024-01-15")
        self.assertEqual(meta.uwi_api, "100/01-01-001-01W6/0")


class TestSyncWhitsonWellAttributes(unittest.TestCase):
    def test_sync_calls_patch_only(self):
        whitson = MagicMock()
        whitson.edit_well_info.return_value = MagicMock(status_code=200, text="")

        meta = WellMetadata(
            pad_name="PAD-A",
            sub_field="Block 1",
            toe_lat=55.2,
            toe_long=-120.3,
        )
        ok = sync_whitson_well_attributes(whitson, 100, meta)
        self.assertTrue(ok)
        whitson.edit_well_info.assert_called_once()
        patch_arg = whitson.edit_well_info.call_args[0][0]
        self.assertEqual(patch_arg[0]["id"], 100)
        self.assertEqual(patch_arg[0]["pad_name"], "PAD-A")
        self.assertEqual(patch_arg[0]["sub_field"], "Block 1")
        self.assertAlmostEqual(patch_arg[0]["bothole_lat_toe"], 55.2)
        self.assertAlmostEqual(patch_arg[0]["bothole_long_toe"], -120.3)
        whitson.edit_custom_attribute_bulk.assert_not_called()

    def test_sync_skips_patch_when_no_native_fields(self):
        whitson = MagicMock()
        meta = WellMetadata()
        ok = sync_whitson_well_attributes(whitson, 50, meta)
        self.assertTrue(ok)
        whitson.edit_well_info.assert_not_called()
        whitson.edit_custom_attribute_bulk.assert_not_called()

    def test_sync_returns_false_on_patch_failure(self):
        whitson = MagicMock()
        whitson.edit_well_info.return_value = MagicMock(
            status_code=422, text="validation error"
        )
        meta = WellMetadata(formation="F1")
        ok = sync_whitson_well_attributes(whitson, 1, meta)
        self.assertFalse(ok)


if __name__ == "__main__":
    unittest.main()
