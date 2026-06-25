"""Routine update production calcs: seq/cum from first production (no patch seeds)."""

from datetime import date

import pandas as pd

from production_update import (
    calculate_cumulatives,
    calculate_sequences,
    filter_to_first_production,
    map_cda_well_names_to_production,
    month_start_on_or_before,
)


def test_month_start_on_or_before():
    assert month_start_on_or_before(date(2026, 6, 15)) == date(2026, 6, 1)


def test_map_cda_well_names_to_production():
    composite = {"CDA-SRC": "Composite Well A"}
    fallback = {"CDA-FB": "Fallback Well B"}
    names = map_cda_well_names_to_production(
        ["CDA-SRC", "CDA-FB", "CDA-SRC", "  "],
        composite,
        fallback,
    )
    assert names == ["Composite Well A", "Fallback Well B"]


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


def test_sequences_rebuilt_from_scratch_then_window_trim():
    """Quick update: full history calcs, insert only rows in rolling window."""
    dates = [date(2024, 11, d) for d in range(28, 31)] + [
        date(2024, 12, d) for d in range(1, 23)
    ]
    df = pd.DataFrame(
        {
            "Well Name": ["A"] * len(dates),
            "Date": dates,
            "Gas WH Production (10³m³)": [10.0] * len(dates),
            "Gathered Gas (e³m³/d)": [10.0] * len(dates),
        }
    )
    df = filter_to_first_production(df)
    out = calculate_sequences(df)
    out = calculate_cumulatives(
        out.assign(
            **{
                "Gas S2 Production (10³m³)": 0.0,
                "Gas Sales Production (10³m³)": 0.0,
                "Condensate Sales (m³/d)": 0.0,
                "Condensate WH (m³/d)": 0.0,
                "Gathered Condensate (m³/d)": 0.0,
                "Gath. Water Rate (m³/d)": 0.0,
            }
        )
    )
    window_start = date(2024, 12, 1)
    trimmed = out.loc[out["Date"] >= window_start].copy()
    dec21 = trimmed.loc[trimmed["Date"] == date(2024, 12, 21)].iloc[0]
    dec22 = trimmed.loc[trimmed["Date"] == date(2024, 12, 22)].iloc[0]
    assert dec21["Days Seq"] == 24
    assert dec22["Days Seq"] == 25
    assert dec21["Gas WH Cumulative Production (10³m³)"] == 240.0
    assert dec22["Gas WH Cumulative Production (10³m³)"] == 250.0
