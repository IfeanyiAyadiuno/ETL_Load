"""Full-table PCE_Production sequence rebuild from existing rates."""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd

from production_update import (
    PRODUCTION_SEQUENCE_RECALC_COLUMNS,
    _prepare_production_sequence_updates,
    _production_sequence_update_rows,
    _production_sequence_select_sql,
    rebuild_all_production_sequences_from_scratch,
)


def _sample_production_df():
    dates_a = [date(2024, 11, d) for d in range(28, 31)] + [
        date(2024, 12, d) for d in range(1, 3)
    ]
    dates_b = [date(2024, 12, d) for d in range(1, 3)]
    return pd.DataFrame(
        {
            "Date": dates_a + dates_b,
            "Well Name": ["A"] * len(dates_a) + ["B"] * len(dates_b),
            "Gas WH Production (10³m³)": [10.0] * (len(dates_a) + len(dates_b)),
            "Gas S2 Production (10³m³)": [0.0] * (len(dates_a) + len(dates_b)),
            "Gas Sales Production (10³m³)": [0.0] * (len(dates_a) + len(dates_b)),
            "Condensate Sales (m³/d)": [0.0] * (len(dates_a) + len(dates_b)),
            "Condensate WH (m³/d)": [0.0] * (len(dates_a) + len(dates_b)),
            "Gathered Gas (e³m³/d)": [10.0] * (len(dates_a) + len(dates_b)),
            "Gathered Condensate (m³/d)": [0.0] * (len(dates_a) + len(dates_b)),
            "Gath. Water Rate (m³/d)": [0.0] * (len(dates_a) + len(dates_b)),
            "Alloc. Water Rate (m³)": [0.0] * (len(dates_a) + len(dates_b)),
        }
    )


def test_prepare_production_sequence_updates_from_scratch():
    out = _prepare_production_sequence_updates(_sample_production_df())
    well_a = out.loc[out["Well Name"] == "A"]
    dec2 = well_a.loc[well_a["Date"] == date(2024, 12, 2)].iloc[0]
    assert dec2["Days Seq"] == 5
    assert dec2["Gas WH Cumulative Production (10³m³)"] == 50.0


def test_production_sequence_update_row_shape():
    out = _prepare_production_sequence_updates(_sample_production_df())
    rows = _production_sequence_update_rows(out)
    assert len(rows) == len(out)
    assert len(rows[0]) == len(PRODUCTION_SEQUENCE_RECALC_COLUMNS) + 2


def test_production_sequence_select_excludes_window_wells():
    sql, params = _production_sequence_select_sql(exclude_well_names=["A", "B"])
    assert "NOT IN" in sql
    assert params == ["A", "B"]


def test_rebuild_all_production_sequences_uses_staging_bulk_update():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor

    with patch(
        "production_update.fetch_pce_production_for_sequence_rebuild",
        return_value=_sample_production_df(),
    ) as mock_fetch, patch(
        "production_update._apply_sequence_updates_via_staging",
        return_value=7,
    ) as mock_apply:
        result = rebuild_all_production_sequences_from_scratch(conn=conn, log=lambda m: None)

    assert result["ok"] is True
    assert result["rows_updated"] == 7
    assert result["wells"] == 2
    mock_fetch.assert_called_once()
    mock_apply.assert_called_once()
    conn.commit.assert_called_once()


def test_routine_rebuild_skips_production_read_for_window_wells():
    conn = MagicMock()
    window_cda = _sample_production_df().loc[
        _sample_production_df()["Well Name"] == "A"
    ].copy()
    other = _sample_production_df().loc[
        _sample_production_df()["Well Name"] == "B"
    ].copy()

    with patch(
        "production_update.fetch_pce_production_for_sequence_rebuild",
        return_value=other,
    ) as mock_fetch, patch(
        "production_update._apply_sequence_updates_via_staging",
        return_value=7,
    ):
        result = rebuild_all_production_sequences_from_scratch(
            conn=conn,
            log=lambda m: None,
            window_cda_for_seq=window_cda,
            window_well_names=["A"],
        )

    assert result["ok"] is True
    assert result["wells"] == 2
    mock_fetch.assert_called_once_with(
        conn,
        exclude_well_names=["A"],
        log=mock_fetch.call_args.kwargs["log"],
    )
