"""Unit tests for gathered production → PCE_FRCST_PRD unit conversion."""

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REBUILD_SOURCE = (ROOT / "pce_frcst_prd_rebuild.py").read_text(encoding="utf-8")

E3M3_TO_MCF = 35.49373
M3_TO_BBL_COND = 6.29287
M3_TO_BBL_WATER = 6.29010


class TestPceFrcstPrdRebuild(unittest.TestCase):
    def test_gathered_insert_sql_applies_unit_conversions(self):
        self.assertIn(f"E3M3_TO_MCF = {E3M3_TO_MCF}", REBUILD_SOURCE)
        self.assertIn(f"M3_TO_BBL_COND = {M3_TO_BBL_COND}", REBUILD_SOURCE)
        self.assertIn(f"M3_TO_BBL_WATER = {M3_TO_BBL_WATER}", REBUILD_SOURCE)
        self.assertIn(f"* {{gas_factor}}", REBUILD_SOURCE)
        self.assertIn("[Gathered Gas (e³m³/d)]", REBUILD_SOURCE)
        self.assertIn("[Gathered Condensate (m³/d)]", REBUILD_SOURCE)
        self.assertIn("[Alloc. Water Rate (m³)]", REBUILD_SOURCE)

    def test_gathered_insert_uses_production_pad_with_prd_fallback(self):
        self.assertIn("gathered_frcst_prd_pad_sql", REBUILD_SOURCE)
        self.assertIn("{gathered_pad}", REBUILD_SOURCE)
        from production_update import gathered_frcst_prd_pad_sql

        pad_sql = gathered_frcst_prd_pad_sql()
        self.assertIn("p.[Pad Name]", pad_sql)
        self.assertIn("ca.[Pad Name]", pad_sql)
        self.assertIn(" PRD", pad_sql)


if __name__ == "__main__":
    unittest.main()
