"""Allocation_Factors [UWI] sync from PCE_WM on rebuild."""

from unittest.mock import MagicMock

from production_update import (
    sync_allocation_factors_uwi_from_wm_sql,
    sync_wm_uwi_to_downstream_sql,
)


def test_sync_allocation_factors_uwi_sql_shape():
    cursor = MagicMock()
    sync_allocation_factors_uwi_from_wm_sql(cursor)

    sql = cursor.execute.call_args[0][0]
    assert "Allocation_Factors" in sql
    assert "PCE_WM" in sql
    assert "[UWI]" in sql
    assert "[Value Navigator UWI]" in sql
    assert "[Well Name]" in sql
    assert "Exception" in sql


def test_sync_wm_uwi_to_downstream_calls_production_and_af():
    cursor = MagicMock()
    sync_wm_uwi_to_downstream_sql(cursor, "2024-01-01", "2024-06-30")

    assert cursor.execute.call_count == 2
    prod_sql, prod_params = cursor.execute.call_args_list[0][0]
    af_sql = cursor.execute.call_args_list[1][0][0]
    assert "PCE_Production" in prod_sql
    assert "BETWEEN ? AND ?" in prod_sql
    assert prod_params == ["2024-01-01", "2024-06-30"]
    assert "Allocation_Factors" in af_sql
