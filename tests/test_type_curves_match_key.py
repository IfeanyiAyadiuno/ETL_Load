"""Unit tests for type-curve well match keys (no DB)."""

import pytest

from type_curves_import import _tc_well_match_key


@pytest.mark.parametrize(
    "raw,expected_substring",
    [
        ("07-01-085-26W6M", "26w6"),
        ("07-01-085-26W6", "26w6"),
        ("A2-01-85-26W6M - T3 - PnP", "26w6"),
        ("26W10M", "26w10"),
        ("26W10", "26w10"),
    ],
)
def test_well_match_key_meridian_m_and_digit_collapse(raw, expected_substring):
    k = _tc_well_match_key(raw)
    assert expected_substring in k


def test_well_match_key_hyphen_normalization():
    k = _tc_well_match_key("Foo  /  Bar")
    assert k == "foo-bar"


def test_well_match_key_empty():
    assert _tc_well_match_key("") == ""
    assert _tc_well_match_key(None) == ""
