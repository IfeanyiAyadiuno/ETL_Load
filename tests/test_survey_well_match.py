"""Survey bulk well matching vs PCE_WM keys (composite file names, meridian M)."""

import pandas as pd
import pytest

from survey_import import (
    _normalize_column_names,
    _survey_file_match_key_variants,
    survey_well_name_matches_wm_keys,
)


@pytest.mark.parametrize(
    "file_well,wm_keys,expected",
    [
        (
            "B2-01-85-26W6M-T2-PnP",
            {"b2-1-85-26w6"},
            True,
        ),
        (
            "c-B98-D/94-A-5-T2-PnP",
            {"c-b98-d-94-a-5"},
            True,
        ),
        (
            "10-1-85-26W6-T2-NCS",
            {"10-1-85-26w6"},
            True,
        ),
        (
            "A2-01-85-26W6M-T3-PnP",
            {"a2-1-85-26w6"},
            True,
        ),
        (
            "B2-01-85-26W6M-T2-PnP",
            {"wrong-key"},
            False,
        ),
    ],
)
def test_survey_well_matches_wm_with_composite_and_slash(file_well, wm_keys, expected):
    assert survey_well_name_matches_wm_keys(file_well, wm_keys) is expected


def test_variants_include_tail_stripped_key():
    keys = set(_survey_file_match_key_variants("B2-01-85-26W6M-T2-PnP"))
    assert "b2-1-85-26w6" in keys


def test_normalize_nad83_surface_lat_long_headers():
    df = pd.DataFrame(
        {
            "Surface Location Latitude (NAD83)": [1.0],
            "Surface Location Longitude (NAD83)": [2.0],
        }
    )
    out = _normalize_column_names(df)
    assert list(out.columns) == ["Latitude", "Longitude"]


def test_normalize_duplicate_lat_long_prefers_last_column():
    """Legacy + NAD83 headers both map to Latitude/Longitude — keep rightmost (NAD83)."""
    df = pd.DataFrame(
        {
            "Latitude": [9.0],
            "Longitude": [8.0],
            "Surface Location Latitude (NAD83)": [1.0],
            "Surface Location Longitude (NAD83)": [2.0],
        }
    )
    out = _normalize_column_names(df)
    assert list(out.columns) == ["Latitude", "Longitude"]
    assert float(out["Latitude"].iloc[0]) == 1.0
    assert float(out["Longitude"].iloc[0]) == 2.0
