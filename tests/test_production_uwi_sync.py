"""PCE_Production [UWI] backfill from PCE_WM after rebuild."""

from unittest.mock import MagicMock

from production_update import sync_production_uwi_from_wm_sql


def test_sync_production_uwi_sql_matches_wm_rules():
    cursor = MagicMock()
    sync_production_uwi_from_wm_sql(cursor)

    sql = cursor.execute.call_args[0][0]
    assert "[UWI]" in sql
    assert "[Value Navigator UWI]" in sql
    assert "[Composite Name]" in sql
    assert "Exception" in sql
    assert "% - TC" in sql
    assert "YE2%" in sql
    assert cursor.execute.call_args[0][1:] == ()


def test_sync_production_uwi_sql_optional_date_filter():
    cursor = MagicMock()
    sync_production_uwi_from_wm_sql(cursor, "2024-01-01", "2024-06-30")

    sql, params = cursor.execute.call_args[0]
    assert "BETWEEN ? AND ?" in sql
    assert params == ["2024-01-01", "2024-06-30"]
