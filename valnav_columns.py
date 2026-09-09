"""Shared ValNav Excel column name resolution (PA monthly loader + NGL)."""

from __future__ import annotations

from typing import Dict, Optional, Tuple

import pandas as pd

_SUP_TO_DIGIT = str.maketrans(
    {
        "\u00b2": "2",
        "\u00b3": "3",
        "\u00b9": "1",
        "\u2070": "0",
        "\u2074": "4",
        "\u2075": "5",
        "\u2076": "6",
        "\u2077": "7",
        "\u2078": "8",
        "\u2079": "9",
    }
)


def norm_valnav_header(s: str) -> str:
    t = str(s).strip().replace("\xa0", " ")
    while "  " in t:
        t = t.replace("  ", " ")
    return t.translate(_SUP_TO_DIGIT).lower()


def strip_valnav_column_names(df: pd.DataFrame) -> None:
    df.columns = [str(c).strip().replace("\xa0", " ") for c in df.columns]


def resolve_valnav_column(df: pd.DataFrame, logical_name: str, *candidates: str) -> str:
    cols = list(df.columns)
    for want in candidates:
        if want in cols:
            return want
    by_norm: Dict[str, str] = {}
    for c in cols:
        k = norm_valnav_header(c)
        if k not in by_norm:
            by_norm[k] = c
    for want in candidates:
        k = norm_valnav_header(want)
        if k in by_norm:
            return by_norm[k]
    preview = ", ".join(repr(str(c)) for c in cols[:40])
    more = f" … (+{len(cols) - 40} more)" if len(cols) > 40 else ""
    raise KeyError(
        f"{logical_name}: no column matching {candidates!r}. "
        f"Sheet columns ({len(cols)}): {preview}{more}"
    )


def resolve_valnav_uwi_column(df: pd.DataFrame) -> str:
    return resolve_valnav_column(
        df,
        "UWI / McDaniel id",
        "McDaniel database",
        "McDaniel Database",
    )


def resolve_valnav_gas_column(df: pd.DataFrame) -> str:
    return resolve_valnav_column(
        df,
        "S2 gas volume",
        "Gas Actual Volume",
        "Gas actual volume",
        "Gas Actual Vol",
        "Gas Actual Volume (10³m³)",
        "Gas Actual Volume (103m3)",
        "Gas Actual Volume (e3m3)",
        "Gas Actual Volume (e³m³)",
        "Gas Actual Volume e3m3",
    )


def resolve_valnav_cond_column(df: pd.DataFrame) -> str:
    return resolve_valnav_column(
        df,
        "Allocation dispensed condensate",
        "Allocation Disp Condensate Volume (m³)",
        "Allocation Disp Condensate Volume (m3)",
        "Allocation Disp Condensate Volume",
        "Condensate Volume (m³)",
        "Condensate Volume (m3)",
        "Condensate Volume",
    )


def resolve_valnav_ngl_columns(df: pd.DataFrame) -> Optional[Dict[str, str]]:
    """
    Return {excel_col: actual_header} for NGL-C2…C5 and NGLs, or None if any missing.
    """
    required = {
        "NGL-C2": ("NGL-C2",),
        "NGL-C3": ("NGL-C3",),
        "NGL-C4": ("NGL-C4",),
        "NGL-C5": ("NGL-C5",),
        "NGLs": ("NGLs", "NGLS", "PA_NGLs", "PA_NGLS"),
    }
    out: Dict[str, str] = {}
    try:
        for key, candidates in required.items():
            out[key] = resolve_valnav_column(df, f"NGL {key}", *candidates)
    except KeyError:
        return None
    return out


def try_resolve_valnav_sales_columns(
    df: pd.DataFrame,
) -> Tuple[str, str, str]:
    """Return (uwi_col, gas_col, cond_col)."""
    return (
        resolve_valnav_uwi_column(df),
        resolve_valnav_gas_column(df),
        resolve_valnav_cond_column(df),
    )
