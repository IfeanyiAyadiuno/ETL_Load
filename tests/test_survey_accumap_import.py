"""Accumap Directional Survey import parsing and WM metadata."""

import pandas as pd
import pytest

from survey_import import (
    _normalize_accumap_header,
    _accumap_field_from_header,
    find_accumap_header_row,
    parse_accumap_survey_grid,
    _apply_accumap_wm_labels,
)
from sales_allocation_updates import resolve_accumap_uwi_to_survey_metadata


def test_normalize_accumap_header_strips_units():
    assert _normalize_accumap_header("MD (m)") == "md"
    assert _normalize_accumap_header("Inclination (°)") == "inclination"
    assert _normalize_accumap_header("Surface Hole UTM Easting (m)") == "surface hole utm easting"


def test_accumap_field_from_header_ignores_sort_uwi_and_zone():
    assert _accumap_field_from_header("sort uwi") is None
    assert _accumap_field_from_header("surface hole utm zone") is None
    assert _accumap_field_from_header("uwi") == "UWI"
    assert _accumap_field_from_header("ew") == "EW"


def test_find_accumap_header_row_with_title_rows():
    grid = pd.DataFrame(
        [
            ["Directional Survey", None, None],
            [None, None, None],
            [
                "Sort UWI",
                "UWI",
                "Subsea (m)",
                "Inclination (°)",
                "Azimuth (°)",
                "MD (m)",
                "TVD (m)",
                "Surface Hole UTM Easting (m)",
                "Surface Hole UTM Northing (m)",
                "Surface Hole UTM Zone",
                "EW (m)",
                "NS (m)",
            ],
            [
                "x",
                "100/16-28-084-25W6/00",
                100,
                0,
                90,
                500,
                480,
                567761.95,
                6243338.37,
                10,
                50.0,
                -25.0,
            ],
        ],
        dtype=object,
    )
    header_row, col_map = find_accumap_header_row(grid)
    assert header_row == 2
    assert col_map["UWI"] == 1
    assert col_map["Measured Depth"] == 5
    assert col_map["EW"] == 10
    assert col_map["Surface Hole UTM Easting"] == 7


def test_parse_accumap_computes_east_north_and_multi_uwi():
    headers = [
        "Sort UWI",
        "UWI",
        "Subsea (m)",
        "Inclination (°)",
        "Azimuth (°)",
        "MD (m)",
        "TVD (m)",
        "Surface Hole UTM Easting (m)",
        "Surface Hole UTM Northing (m)",
        "Surface Hole UTM Zone",
        "EW (m)",
        "NS (m)",
    ]
    row_a = ["x", "100/A/1/00", 1, 2, 3, 100, 90, 1000.0, 2000.0, 10, 10.0, 5.0]
    row_b = ["y", "200/B/2/00", 1, 2, 3, 200, 180, 3000.0, 4000.0, 10, 20.0, 30.0]
    grid = pd.DataFrame([headers, row_a, row_b], dtype=object)
    _, col_map = find_accumap_header_row(grid)
    rows = parse_accumap_survey_grid(grid, 0, col_map)
    assert len(rows) == 2
    assert rows[0]["UWI"] == "100/A/1/00"
    assert rows[0]["East"] == pytest.approx(1010.0)
    assert rows[0]["North"] == pytest.approx(2005.0)
    assert rows[1]["UWI"] == "200/B/2/00"
    assert rows[1]["Measured Depth"] == pytest.approx(200.0)


def test_apply_accumap_wm_labels_uses_metadata_and_uwi_fallback():
    meta = {"100/a/1/00": ("COMPOSITE-A", "PAD-A")}
    rows = [{"UWI": "100/A/1/00", "Measured Depth": 1.0}]
    labeled, linked, unlinked = _apply_accumap_wm_labels(rows, meta)
    assert labeled[0]["Well Name Cleaned"] == "COMPOSITE-A"
    assert labeled[0]["PAD"] == "PAD-A"
    assert linked == 1
    assert unlinked == 0

    rows2 = [{"UWI": "999/unknown/00", "Measured Depth": 1.0}]
    labeled2, linked2, unlinked2 = _apply_accumap_wm_labels(rows2, meta)
    assert labeled2[0]["Well Name Cleaned"] == "999/unknown/00"
    assert labeled2[0]["PAD"] == ""
    assert linked2 == 0
    assert unlinked2 == 1


def test_resolve_accumap_uwi_to_survey_metadata():
    meta = {"100/a/1/00": ("COMP-A", "PAD-1")}
    name, pad = resolve_accumap_uwi_to_survey_metadata("100/A/1/00", meta)
    assert name == "COMP-A"
    assert pad == "PAD-1"
    name2, pad2 = resolve_accumap_uwi_to_survey_metadata("missing", meta)
    assert name2 is None
    assert pad2 is None
