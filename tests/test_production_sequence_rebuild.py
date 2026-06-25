"""Full-table PCE_Production sequence rebuild from existing rates."""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd

from production_update import (
    PRODUCTION_SEQUENCE_RECALC_COLUMNS,
    _prepare_production_sequence_updates,
    _production_sequence_update_rows,
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


def test_rebuild_all_production_sequences_from_scratch_calls_update():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor

    with patch(
        "production_update.fetch_pce_production_for_sequence_rebuild",
        return_value=_sample_production_df(),
    ) as mock_fetch, patch(
        "pce_production_schema.batch_executemany",
        side_effect=lambda cur, sql, rows, **kw: None,
    ) as mock_batch:
        result = rebuild_all_production_sequences_from_scratch(conn=conn, log=lambda m: None)

    assert result["ok"] is True
    assert result["rows_updated"] == 7
    assert result["wells"] == 2
    mock_fetch.assert_called_once_with(conn, log=mock_fetch.call_args.kwargs["log"])
    mock_batch.assert_called_once()
    conn.commit.assert_called_once()
