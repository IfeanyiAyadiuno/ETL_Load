"""Regression: monthly averages bucket by calendar month (Period('M')), not trailing windows."""

from __future__ import annotations

import pandas as pd


def test_calculate_monthly_averages_splits_calendar_months_and_alloc_water_avg():
    from production_update import calculate_monthly_averages

    df = pd.DataFrame(
        {
            "Well Name": ["A"] * 4,
            "Date": [
                pd.Timestamp("2024-02-01"),
                pd.Timestamp("2024-02-02"),
                pd.Timestamp("2024-03-01"),
                pd.Timestamp("2024-03-02"),
            ],
            "Gas WH Production (10³m³)": [10.0, 20.0, 100.0, 200.0],
            "Gas S2 Production (10³m³)": [0.0, 0.0, 0.0, 0.0],
            "Gathered Gas (e³m³/d)": [0.0, 0.0, 0.0, 0.0],
            "Gathered Condensate (m³/d)": [0.0, 0.0, 0.0, 0.0],
            "Alloc. Water Rate (m³)": [4.0, 8.0, 60.0, 12.0],
        }
    )

    out = calculate_monthly_averages(df.copy())

    feb_gas = float(
        out.loc[out["Date"] == pd.Timestamp("2024-02-01"), "Gas WH Avg (10³m³)"].iloc[0]
    )
    assert feb_gas == 15.0

    mar_water = float(
        out.loc[out["Date"] == pd.Timestamp("2024-03-02"), "Alloc. Water Avg (m³)"].iloc[0]
    )
    assert mar_water == 36.0
