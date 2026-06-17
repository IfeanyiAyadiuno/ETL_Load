"""Post-rebuild pipeline step order contract."""

from datetime import date
from unittest.mock import MagicMock, patch

from pce_rebuild_pipeline import run_post_production_rebuild_steps


def test_post_rebuild_pipeline_step_order():
    calls = []

    def log(msg):
        calls.append(msg)

    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor

    with patch(
        "sync_typecurves_to_production.sync_tc_to_production",
        side_effect=lambda **kw: calls.append("tc"),
    ) as mock_tc, patch(
        "production_update.sync_wm_uwi_to_downstream_sql",
        side_effect=lambda cur: calls.append("uwi"),
    ) as mock_uwi, patch(
        "production_update._refresh_ngl_from_allocation_factors",
        return_value=True,
    ) as mock_ngl, patch(
        "production_update.sync_production_wm_metadata_from_wm_sql",
        side_effect=lambda *a, **kw: calls.append("metadata"),
    ) as mock_meta, patch(
        "pce_frcst_prd_rebuild.rebuild_pce_frcst_prd",
        side_effect=lambda **kw: calls.append("frcst"),
    ) as mock_frcst:
        ok = run_post_production_rebuild_steps(
            log,
            conn=conn,
            date_window=(date(2024, 1, 1), date(2024, 6, 30)),
        )

    assert ok is True
    mock_tc.assert_called_once()
    mock_uwi.assert_called_once()
    mock_ngl.assert_called_once()
    mock_meta.assert_called()
    mock_frcst.assert_called_once()
    assert calls.index("tc") < calls.index("uwi") < calls.index("metadata") < calls.index("frcst")
