"""Unit tests for gathered production → PCE_FRCST_PRD unit conversion and rebuild."""

import unittest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

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
        self.assertIn("* {gas_factor}", REBUILD_SOURCE)
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

    def test_gathered_insert_caps_at_lag_date(self):
        self.assertIn("CAST(p.[Date] AS DATE) <= ?", REBUILD_SOURCE)

    def test_rebuild_passes_effective_end_to_gathered_insert(self):
        from pce_frcst_prd_rebuild import rebuild_pce_frcst_prd

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_cursor.rowcount = 10
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        eff = date(2026, 6, 23)
        with patch("pce_frcst_prd_rebuild.get_sql_conn", return_value=mock_conn):
            with patch(
                "pce_frcst_prd_rebuild.prodview_effective_end_date",
                return_value=eff,
            ) as mock_end:
                out = rebuild_pce_frcst_prd(log=lambda _m: None)

        mock_end.assert_called_once_with(None)
        gathered_calls = [
            c
            for c in mock_cursor.execute.call_args_list
            if len(c[0]) > 1 and c[0][1] == (eff,)
        ]
        self.assertEqual(len(gathered_calls), 1)
        self.assertEqual(out["effective_end_date"], str(eff))
        self.assertEqual(out["forecast_rows"], 10)
        self.assertEqual(out["gathered_rows"], 10)

    def test_rebuild_respects_custom_data_lag_days(self):
        from pce_frcst_prd_rebuild import rebuild_pce_frcst_prd

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_cursor.rowcount = 1
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        eff = date(2026, 6, 18)
        with patch("pce_frcst_prd_rebuild.get_sql_conn", return_value=mock_conn):
            with patch(
                "pce_frcst_prd_rebuild.prodview_effective_end_date",
                return_value=eff,
            ) as mock_end:
                out = rebuild_pce_frcst_prd(log=lambda _m: None, data_lag_days=5)

        mock_end.assert_called_once_with(5)
        self.assertEqual(out["data_lag_days"], 5)
        self.assertEqual(out["effective_end_date"], str(eff))


if __name__ == "__main__":
    unittest.main()
