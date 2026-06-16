"""Unit tests for monthly forecast import helpers."""

from datetime import date

import pandas as pd
import pytest

from monthly_forecasts_import import (
    append_monthly_forecasts_from_excel,
    distinct_forecast_keys,
    forecast_month_display_label,
    validate_forecast_dates,
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


def test_validate_forecast_dates_truncates_long_lists():
    dates = [f"Apr {i}" for i in range(20)]
    df = pd.DataFrame({"Date": dates, "UWI": ["A"] * 20})
    warnings = validate_forecast_dates(df, max_samples=3)
    assert len(warnings) == 4
    assert "total 20" in warnings[-1]


def test_distinct_forecast_keys_dedupes_and_skips_invalid():
    df = pd.DataFrame(
        {
            "Date": [
                pd.Timestamp("2026-04-01"),
                pd.Timestamp("2026-04-01"),
                None,
                pd.Timestamp("2026-05-01"),
            ],
            "UWI": ["100/01-01-001-01W6", "100/01-01-001-01W6", "X", "100/01-01-002-01W6"],
        }
    )
    keys = distinct_forecast_keys(df)
    assert keys == [
        (date(2026, 4, 1), "100/01-01-001-01W6"),
        (date(2026, 5, 1), "100/01-01-002-01W6"),
    ]


def test_forecast_month_display_label_uses_month_column():
    assert forecast_month_display_label(2026, 4, "Apr 2026") == "Apr 2026"


def test_forecast_month_display_label_fallback():
    assert forecast_month_display_label(2026, 4, None) == "Apr 2026"
    assert forecast_month_display_label(2026, 4, "   ") == "Apr 2026"


def test_append_import_no_full_table_delete():
    import inspect

    source = inspect.getsource(append_monthly_forecasts_from_excel)
    assert 'cur.execute(f"DELETE FROM {TARGET_TABLE}")' not in source


def test_fetch_distinct_forecast_months_labels_from_date_not_month_column():
    """List labels use [Date] calendar month, not repeated [Month] column text."""
    from unittest.mock import MagicMock, patch

    from monthly_forecasts_import import fetch_distinct_forecast_months

    mock_rows = [(2026, mo) for mo in range(1, 7)]
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = mock_rows
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("monthly_forecasts_import.get_sql_conn", return_value=mock_conn):
        months = fetch_distinct_forecast_months()

    assert len(months) == 6
    labels = [label for _y, _m, label in months]
    assert labels == [
        "Jan 2026",
        "Feb 2026",
        "Mar 2026",
        "Apr 2026",
        "May 2026",
        "Jun 2026",
    ]
    assert labels.count("2026 May") == 0
