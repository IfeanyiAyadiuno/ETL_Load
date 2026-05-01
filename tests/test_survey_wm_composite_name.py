"""Survey import: WM Composite Name preferred over Well Name for resolved UWIs."""

import pytest

from sales_allocation_updates import (
    _survey_well_display_from_wm,
    resolve_accumap_uwi_to_survey_well_name,
)

from survey_import import _survey_display_from_wm_match_keys


@pytest.mark.parametrize(
    "composite,well_name,expected",
    [
        ("Composite A", "Well B", "Composite A"),
        ("", "Well B", "Well B"),
        (None, "Well B", "Well B"),
        ("   ", "Well B", "Well B"),
        (None, None, None),
    ],
)
def test_survey_well_display_prefers_composite(composite, well_name, expected):
    assert _survey_well_display_from_wm(composite, well_name) == expected


def test_resolve_accumap_uwi_survey_name_uses_dict():
    d = {"200/a/1/2": "COMPOSITE-ONLY"}
    assert resolve_accumap_uwi_to_survey_well_name("200/A/1/2", d) == "COMPOSITE-ONLY"


def test_resolve_accumap_falls_back_second_char_digit_strip():
    d = {"x-only": "from-variant"}
    assert resolve_accumap_uwi_to_survey_well_name("1x-only", d) == "from-variant"


def test_survey_display_from_wm_match_keys_prefers_wm_label():
    wm_by_key = {"b2-1-85-26w6": "Composite From WM"}
    assert (
        _survey_display_from_wm_match_keys("B2-01-85-26W6M-T2-PnP", wm_by_key)
        == "Composite From WM"
    )
