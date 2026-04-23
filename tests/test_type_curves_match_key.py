"""Unit tests for type-curve well match keys"""

import pytest

from type_curves_import import (
    _assign_column_roles,
    _tc_pad_name_from_excel,
    _tc_storage_base_name,
    _tc_well_match_key,
    stored_well_name_file_only,
)


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


def test_file_only_ye23_no_tc_suffix():
    raw = "YE23 McD LM NFB TC-1P"
    assert stored_well_name_file_only(raw) == raw


def test_file_only_other_well_gets_tc_suffix():
    from type_curves_import import TC_SUFFIX

    s = stored_well_name_file_only("orphan-file-well-01")
    assert s.endswith(TC_SUFFIX)


def test_pad_column_prefers_pad_name_over_padding():
    cols = ["Padding", "Pad Name", "Well Name"]
    roles = _assign_column_roles(cols)
    assert roles["pad"] == 1


def test_tc_pad_prefix_normalizes_spaces():
    assert _tc_pad_name_from_excel("7-01 South") == "PCE-TC-7-01-South"


def test_tc_pad_prefix_idempotent():
    assert _tc_pad_name_from_excel("PCE-TC-7-01-South") == "PCE-TC-7-01-South"


def test_cum_condy_column_not_stolen_by_condensate_sales_cum():
    """Sales cumulative uses 'cond' inside 'condensate'; cum_cond role must require 'condy'."""
    cols = [
        "Well Name",
        "Condensate Sales Cum mbbl",
        "Cum Condy (Mbbl)",
    ]
    roles = _assign_column_roles(cols)
    assert roles["cond_sales_cum_mbbl"] == 1
    assert roles["cum_cond_mbbl"] == 2
