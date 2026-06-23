"""Routine update production patch: seeded seq/cum within a rolling window."""

from datetime import date

import pandas as pd

from production_update import (
    calculate_cumulatives,
    calculate_sequences,
    month_start_on_or_before,
)


def test_month_start_on_or_before():
    assert month_start_on_or_before(date(2026, 6, 15)) == date(2026, 6, 1)


def test_calculate_cumulatives_with_seed():
    df = pd.DataFrame(
        {
            "Well Name": ["A", "A"],
            "Date": [date(2026, 6, 1), date(2026, 6, 2)],
            "Gas WH Production (10³m³)": [10.0, 5.0],
            "Gas S2 Production (10³m³)": [0.0, 0.0],
            "Gas Sales Production (10³m³)": [0.0, 0.0],
            "Condensate Sales (m³/d)": [0.0, 0.0],
            "Condensate WH (m³/d)": [0.0, 0.0],
            "Gathered Gas (e³m³/d)": [0.0, 0.0],
            "Gathered Condensate (m³/d)": [0.0, 0.0],
            "Gath. Water Rate (m³/d)": [0.0, 0.0],
        }
    )
    seeds = {"A": {"Gas WH Cumulative Production (10³m³)": 100.0}}
    out = calculate_cumulatives(df, cum_seeds=seeds)
    assert out.loc[0, "Gas WH Cumulative Production (10³m³)"] == 110.0
    assert out.loc[1, "Gas WH Cumulative Production (10³m³)"] == 115.0


def test_calculate_sequences_with_seed():
    df = pd.DataFrame(
        {
            "Well Name": ["A", "A"],
            "Date": [date(2026, 6, 1), date(2026, 6, 2)],
            "Gas WH Production (10³m³)": [1.0, 0.0],
        }
    )
    out = calculate_sequences(
        df,
        days_seq_seed={"A": 100},
        day_seq_uprt_seed={"A": 50},
    )
    assert out.loc[0, "Days Seq"] == 101
    assert out.loc[1, "Days Seq"] == 102
    assert out.loc[0, "Day Seq UPRT"] == 51
    assert out.loc[1, "Day Seq UPRT"] == 51
