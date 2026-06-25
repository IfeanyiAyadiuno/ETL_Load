"""Unit tests for monthly forecast import helpers."""

import inspect
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from monthly_forecasts_import import (
    append_monthly_forecasts_from_excel,
    delete_forecast_months,
    distinct_forecast_months_in_df,
    fetch_distinct_forecast_months,
    forecast_month_display_label,
    forecast_months_already_in_db,
    normalize_forecast_month_label,
    preview_monthly_forecast_import,
    validate_forecast_dates,
    validate_forecast_month_labels,
)


def test_validate_forecast_dates_string_without_year():
    df = pd.DataFrame({"Date": ["Apr", "April 1"], "UWI": ["A", "B"]})
    warnings = validate_forecast_dates(df)
    assert len(warnings) == 2
    assert "no year" in warnings[0].lower()


def test_validate_forecast_dates_year_1900():
    df = pd.DataFrame({"Date": [pd.Timestamp("1900-04-01")], "UWI": ["A"]})
    warnings = validate_forecast_dates(df)
    assert len(warnings) == 1
    assert "1900" in warnings[0]


def test_validate_forecast_dates_clean():
    df = pd.DataFrame({"Date": [pd.Timestamp("2026-04-01")], "UWI": ["A"]})
    warnings = validate_forecast_dates(df)
    assert warnings == []


def test_validate_forecast_dates_blank():
    df = pd.DataFrame({"Date": [None, ""], "UWI": ["A", "B"]})
    warnings = validate_forecast_dates(df)
    assert len(warnings) == 2
    assert "blank" in warnings[0].lower()


def test_normalize_forecast_month_label_accepts_year_first():
    assert normalize_forecast_month_label("2026 May") == "2026 May"
    assert normalize_forecast_month_label("2026 Apr") == "2026 April"
    assert normalize_forecast_month_label(" 2026 december ") == "2026 December"


def test_normalize_forecast_month_label_rejects_month_year():
    assert normalize_forecast_month_label("May 2026") is None
    assert normalize_forecast_month_label("Apr 2026") is None
    assert normalize_forecast_month_label("May") is None
    assert normalize_forecast_month_label("") is None


def test_validate_forecast_month_labels_errors():
    df = pd.DataFrame(
        {
            "Month": ["2026 May", "May 2026", ""],
            "Date": [date(2026, 5, 1)] * 3,
            "UWI": ["A", "B", "C"],
        }
    )
    errors = validate_forecast_month_labels(df)
    assert len(errors) == 2
    assert "May 2026" in errors[0]
    assert "blank" in errors[1].lower()


def test_validate_forecast_month_labels_missing_column():
    df = pd.DataFrame({"Date": [date(2026, 5, 1)], "UWI": ["A"]})
    errors = validate_forecast_month_labels(df)
    assert len(errors) == 1
    assert "Month column missing" in errors[0]


def test_distinct_forecast_months_in_df():
    df = pd.DataFrame(
        {
            "Month": ["2026 May", "2026 May", "2026 Apr"],
            "Date": [date(2026, 5, 1), date(2026, 5, 2), date(2026, 4, 1)],
            "UWI": ["A", "B", "C"],
        }
    )
    assert distinct_forecast_months_in_df(df) == ["2026 April", "2026 May"]


def test_forecast_month_display_label_uses_month_column():
    assert forecast_month_display_label(2026, 4, "Apr 2026") == "Apr 2026"


def test_forecast_month_display_label_fallback():
    assert forecast_month_display_label(2026, 4, None) == "2026 April"
    assert forecast_month_display_label(2026, 4, "   ") == "2026 April"


def test_forecast_months_already_in_db_detects_overlap():
    mock_conn = MagicMock()
    with patch(
        "monthly_forecasts_import.fetch_distinct_forecast_months",
        return_value=["2026 May", "2025 Dec"],
    ):
        conflicts = forecast_months_already_in_db(["2026 May"], conn=mock_conn)
    assert conflicts == ["2026 May"]


def test_forecast_months_already_in_db_case_insensitive():
    mock_conn = MagicMock()
    with patch(
        "monthly_forecasts_import.fetch_distinct_forecast_months",
        return_value=["2026 May"],
    ):
        conflicts = forecast_months_already_in_db(["2026 may"], conn=mock_conn)
    assert conflicts == ["2026 May"]


def test_append_import_no_key_replace_delete():
    source = inspect.getsource(append_monthly_forecasts_from_excel)
    assert "_delete_forecast_keys" not in source
    assert "Replacing existing rows" not in source
    assert 'cur.execute(f"DELETE FROM {TARGET_TABLE}")' not in source


def test_fetch_distinct_forecast_months_uses_month_column_only():
    mock_rows = [("2026 May",), ("2026 Apr",), ("2025 Dec",)]
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = mock_rows
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("monthly_forecasts_import.get_sql_conn", return_value=mock_conn):
        months = fetch_distinct_forecast_months()

    assert months == ["2026 May", "2026 Apr", "2025 Dec"]
    sql = mock_cursor.execute.call_args[0][0]
    assert "[Month]" in sql
    assert "YEAR(CAST(mf.[Date]" not in sql


def test_delete_forecast_months_matches_month_column():
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 42
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("monthly_forecasts_import.get_sql_conn", return_value=mock_conn):
        with patch(
            "monthly_forecasts_import._run_frcst_prd_rebuild",
            return_value={
                "forecast_rows": 100,
                "gathered_rows": 200,
                "effective_end_date": "2026-06-23",
            },
        ):
            result = delete_forecast_months(["2026 May"])

    assert result["deleted_forecast_rows"] == 42
    assert result["months_removed"] == 1
    assert result["frcst_prd_rebuild"]["gathered_rows"] == 200
    delete_sql = mock_cursor.execute.call_args_list[0][0][0]
    assert "[Month]" in delete_sql
    assert mock_cursor.execute.call_args_list[0][0][1] == ("2026 May",)


def test_append_blocks_when_month_already_in_db():
    df = pd.DataFrame(
        {
            "Date": [date(2026, 6, 1)],
            "UWI": ["100/01-01-001-01W6"],
            "Month": ["2026 June"],
            "CDGR_Mcf_d": [1.0],
        }
    )
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("monthly_forecasts_import.read_monthly_forecast_excel", return_value=df):
        with patch("monthly_forecasts_import.get_sql_conn", return_value=mock_conn):
            with patch(
                "monthly_forecasts_import.forecast_months_already_in_db",
                return_value=["2026 June"],
            ):
                with pytest.raises(ValueError, match="Remove selected months"):
                    append_monthly_forecasts_from_excel("fake.xlsx")


def test_append_inserts_without_delete():
    df = pd.DataFrame(
        {
            "Date": [date(2026, 6, 1), date(2026, 6, 2)],
            "UWI": ["100/01-01-001-01W6", "100/01-01-002-01W6"],
            "Month": ["2026 June", "2026 June"],
            "CDGR_Mcf_d": [1.0, 2.0],
        }
    )
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("monthly_forecasts_import.read_monthly_forecast_excel", return_value=df):
        with patch("monthly_forecasts_import.get_sql_conn", return_value=mock_conn):
            with patch(
                "monthly_forecasts_import.forecast_months_already_in_db",
                return_value=[],
            ):
                with patch(
                    "monthly_forecasts_import._run_frcst_prd_rebuild",
                    return_value={
                        "forecast_rows": 50,
                        "gathered_rows": 500,
                        "effective_end_date": "2026-06-23",
                    },
                ):
                    result = append_monthly_forecasts_from_excel("fake.xlsx")

    assert result["inserted"] == 2
    assert result["months_added"] == ["2026 June"]
    assert mock_cursor.executemany.call_count >= 1
    delete_calls = [
        c
        for c in mock_cursor.execute.call_args_list
        if c[0] and "DELETE" in str(c[0][0]).upper()
    ]
    assert delete_calls == []


def test_preview_blocks_duplicate_months():
    df = pd.DataFrame(
        {
            "Date": [date(2026, 5, 1)],
            "UWI": ["A"],
            "Month": ["2026 May"],
        }
    )
    with patch("monthly_forecasts_import.read_monthly_forecast_excel", return_value=df):
        with patch(
            "monthly_forecasts_import.forecast_months_already_in_db",
            return_value=["2026 May"],
        ):
            with pytest.raises(ValueError, match="Remove selected months"):
                preview_monthly_forecast_import("fake.xlsx")
