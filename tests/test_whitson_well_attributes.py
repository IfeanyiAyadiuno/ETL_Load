"""Tests for Whitson WM well attribute payload builders (no live SQL or API)."""

import unittest
from unittest.mock import MagicMock

from whitson_well_attributes import (
    LAYER_PRODUCER_ATTRIBUTE_NAME,
    WellMetadata,
    build_layer_producer_custom_bulk,
    build_whitson_well_create_payload,
    build_whitson_well_patch_payload,
    sync_whitson_well_attributes,
)


class TestWhitsonWellAttributePayloads(unittest.TestCase):
    def test_fault_block_maps_to_sub_field_not_formation(self):
        meta = WellMetadata(
            formation="Montney",
            sub_field="North Fault",
            layer_producer="Upper Montney",
        )
        create = build_whitson_well_create_payload(
            meta, project_id=1, name="PAD-A", uwi_api="100/01-01-001-01W6/0"
        )
        self.assertEqual(create["formation"], "Montney")
        self.assertEqual(create["sub_field"], "North Fault")
        self.assertNotIn("layer_producer", create)
        self.assertEqual(
            create["custom_attributes"][0]["attribute_name"],
            LAYER_PRODUCER_ATTRIBUTE_NAME,
        )
        self.assertEqual(create["custom_attributes"][0]["value"], "Upper Montney")

        patch = build_whitson_well_patch_payload(42, meta)
        self.assertEqual(patch["id"], 42)
        self.assertEqual(patch["sub_field"], "North Fault")
        self.assertNotIn("layer_producer", patch)

    def test_null_pad_name_omitted_from_create_and_patch(self):
        meta = WellMetadata(formation="Doe Creek", l_w=2500.0)
        create = build_whitson_well_create_payload(
            meta, project_id=5, name="WELL-1", uwi_api="UWI-1"
        )
        self.assertNotIn("pad_name", create)
        self.assertEqual(create["l_w"], 2500.0)

        patch = build_whitson_well_patch_payload(9, meta)
        self.assertNotIn("pad_name", patch)
        self.assertEqual(patch["l_w"], 2500.0)

    def test_surface_coordinates_on_create(self):
        meta = WellMetadata(surf_lat=55.123, surf_long=-120.456, pad_name="PAD-9")
        create = build_whitson_well_create_payload(
            meta, project_id=1, name="WELL-9", uwi_api=None
        )
        self.assertAlmostEqual(create["surf_lat"], 55.123)
        self.assertAlmostEqual(create["surf_long"], -120.456)
        self.assertEqual(create["pad_name"], "PAD-9")

    def test_layer_producer_bulk_empty_when_missing(self):
        self.assertEqual(build_layer_producer_custom_bulk(1, None), [])
        self.assertEqual(build_layer_producer_custom_bulk(1, "  "), [])

    def test_layer_producer_bulk_single_entry(self):
        bulk = build_layer_producer_custom_bulk(7, "Lower")
        self.assertEqual(len(bulk), 1)
        self.assertEqual(bulk[0]["well_id"], 7)
        self.assertEqual(bulk[0]["attribute_name"], LAYER_PRODUCER_ATTRIBUTE_NAME)
        self.assertEqual(bulk[0]["value"], "Lower")


class TestSyncWhitsonWellAttributes(unittest.TestCase):
    def test_sync_calls_patch_and_custom_bulk(self):
        whitson = MagicMock()
        patch_resp = MagicMock(status_code=200, text="")
        custom_resp = MagicMock(status_code=200, text="")
        whitson.edit_well_info.return_value = patch_resp
        whitson.edit_custom_attribute_bulk.return_value = custom_resp

        meta = WellMetadata(
            pad_name="PAD-A",
            sub_field="Block 1",
            layer_producer="Layer X",
        )
        ok = sync_whitson_well_attributes(whitson, 100, meta)
        self.assertTrue(ok)
        whitson.edit_well_info.assert_called_once()
        patch_arg = whitson.edit_well_info.call_args[0][0]
        self.assertEqual(patch_arg[0]["id"], 100)
        self.assertEqual(patch_arg[0]["pad_name"], "PAD-A")
        self.assertEqual(patch_arg[0]["sub_field"], "Block 1")
        whitson.edit_custom_attribute_bulk.assert_called_once()
        bulk_arg = whitson.edit_custom_attribute_bulk.call_args[0][0]
        self.assertEqual(bulk_arg[0]["attribute_name"], LAYER_PRODUCER_ATTRIBUTE_NAME)

    def test_sync_skips_custom_bulk_without_layer_producer(self):
        whitson = MagicMock()
        whitson.edit_well_info.return_value = MagicMock(status_code=200, text="")
        meta = WellMetadata(pad_name="PAD-B")
        ok = sync_whitson_well_attributes(whitson, 50, meta)
        self.assertTrue(ok)
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
