"""Survey bulk well matching vs PCE_WM keys (composite file names, meridian M)."""

import pytest

from survey_import import _survey_file_match_key_variants, survey_well_name_matches_wm_keys


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
