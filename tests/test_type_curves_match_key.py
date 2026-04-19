"""Unit tests for type-curve well match keys"""

import pytest

from type_curves_import import _tc_storage_base_name, _tc_well_match_key


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


def test_tc_storage_base_excel_longer():
    wm = "2-01-085-25W6"
    ex = "2-01-085-25W6-T3-PnP"
    assert _tc_storage_base_name(ex, wm) == ex


def test_tc_storage_base_wm_longer():
    wm = "2-01-085-25W6-EXTRA"
    ex = "2-01-085-25W6"
    assert _tc_storage_base_name(ex, wm) == wm


def test_tc_storage_base_tie_prefers_wm():
    wm = "A-B-C"
    ex = "D-E-F"
    assert len(ex) == len(wm)
    assert _tc_storage_base_name(ex, wm) == wm


def test_tc_storage_base_excel_only():
    assert _tc_storage_base_name("Solo-Well", None) == "Solo-Well"


def test_tc_storage_base_wm_only():
    assert _tc_storage_base_name(None, "WM-Only") == "WM-Only"
