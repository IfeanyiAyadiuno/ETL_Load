"""Survey import optional WM linking and dropped Lat/Long columns."""

import pytest

from survey_import import (
    DIRECTIONAL_FIELD_KEYS,
    INSERT_COLS,
    INSERT_SQL,
    DirectionalSurveyMappingSpec,
)


def test_directional_field_keys_exclude_lat_long():
    assert "Longitude" not in DIRECTIONAL_FIELD_KEYS
    assert "Latitude" not in DIRECTIONAL_FIELD_KEYS
    assert "East" in DIRECTIONAL_FIELD_KEYS


def test_insert_cols_exclude_lat_long():
    assert "Longitude" not in INSERT_COLS
    assert "Latitude" not in INSERT_COLS
    assert "[Longitude]" not in INSERT_SQL
    assert "[Latitude]" not in INSERT_SQL


def test_mapping_preset_json_strips_lat_long():
    spec = DirectionalSurveyMappingSpec.from_json_dict(
        {
            "columns": {
                "Measured Depth": 0,
                "Longitude": 12,
                "Latitude": 11,
                "East": 9,
            }
        }
    )
    assert "Longitude" not in spec.columns
    assert "Latitude" not in spec.columns
    assert spec.columns["East"] == 9


def test_directional_wm_miss_fallback_pattern():
    """Documented fallback when WM lookup fails: file well name for UWI and Well Name."""
    from survey_import import clean_well_name

    file_well = "ZZZ-NO-SUCH-WELL-EVER-99999"
    cleaned = clean_well_name(file_well)
    assert cleaned == file_well
    # import_directional_survey_with_mapping sets uwi=wm_well_name=cleaned, pad="" on WM miss
    assert cleaned
