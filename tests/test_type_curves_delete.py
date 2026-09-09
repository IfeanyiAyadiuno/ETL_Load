"""Type-curve delete removes PCE_TC and matching PCE_Production rows."""

from datetime import date
from unittest.mock import MagicMock, patch

from type_curves_import import (
    _delete_production_for_tc_import_pairs,
    _fetch_tc_import_pairs,
    delete_typecurves_from_tc,
)


def test_fetch_tc_import_pairs_batches_and_skips_blank_names():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("Well A - TC", date(2026, 1, 1)), ("", date(2026, 1, 2))],
        [("Well B - TC", date(2026, 2, 1))],
    ]
    names = [f"Well {c} - TC" for c in "AB"] + [f"extra-{i}" for i in range(50)]

    pairs = _fetch_tc_import_pairs(mock_cursor, names)

    assert pairs == [
        ("Well A - TC", date(2026, 1, 1)),
        ("Well B - TC", date(2026, 2, 1)),
    ]
    assert mock_cursor.execute.call_count == 2


def test_delete_production_for_tc_import_pairs_deletes_by_well_and_date():
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 1
    pairs = [
        ("07-01-085-26W6M - TC", date(2026, 6, 17)),
        ("YE23 McD LM NFB TC-1P", date(2025, 12, 31)),
    ]

    total = _delete_production_for_tc_import_pairs(mock_cursor, pairs)

    assert total == 2
    calls = mock_cursor.execute.call_args_list
    assert "DELETE FROM dbo.PCE_Production WHERE [Well Name] = ? AND [Date] = ?" in calls[0][0][0]
    assert calls[0][0][1:] == ("07-01-085-26W6M - TC", date(2026, 6, 17))
    assert calls[1][0][1:] == ("YE23 McD LM NFB TC-1P", date(2025, 12, 31))


def test_delete_typecurves_reads_pairs_before_tc_delete():
    execute_log = []
    mock_cursor = MagicMock()

    def record_execute(sql, *args):
        execute_log.append((sql.strip(), args))
        if "SELECT [Well Name], [ImportDate]" in sql:
            mock_cursor.fetchall.return_value = [
                ("07-01-085-26W6M - TC", date(2026, 6, 17)),
            ]
        mock_cursor.rowcount = 1

    mock_cursor.execute.side_effect = record_execute
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor

    with patch("type_curves_import.get_sql_conn", return_value=mock_conn):
        n = delete_typecurves_from_tc(["07-01-085-26W6M - TC"])

    assert n == 1
    assert "SELECT [Well Name], [ImportDate]" in execute_log[0][0]
    assert "DELETE FROM dbo.PCE_TC" in execute_log[1][0]
    assert "DELETE FROM dbo.PCE_Production WHERE [Well Name] = ? AND [Date] = ?" in execute_log[2][0]
    assert execute_log[2][1] == ("07-01-085-26W6M - TC", date(2026, 6, 17))
    assert "DELETE FROM dbo.PCE_Production WHERE [Well Name] IN" in execute_log[3][0]
    mock_conn.commit.assert_called_once()


def test_delete_typecurves_does_not_run_post_rebuild():
    import inspect

    source = inspect.getsource(delete_typecurves_from_tc)
    assert "run_post_production_rebuild_steps" not in source
    assert "sync_tc_to_production" not in source
