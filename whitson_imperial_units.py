"""
Load metric → imperial conversion factors for Whitson+ production push.
"""

from __future__ import annotations

import configparser
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional

from app_paths import get_whitson_imperial_ini_path

_SECTION = "metric_to_imperial"

_REQUIRED_KEYS = (
    "gathered_gas_e3m3_per_day_to_mcf_per_day",
    "condensate_m3_per_day_to_bbl_per_day",
    "gath_water_m3_per_day_to_bbl_per_day",
    "tubing_pressure_kpa_to_psi",
    "casing_pressure_kpa_to_psi",
)

@dataclass(frozen=True)
class WhitsonImperialFactors:
    gas_e3m3_to_mcf: float
    cond_m3_to_bbl: float
    water_m3_to_bbl: float
    tubing_kpa_to_psi: float
    casing_kpa_to_psi: float
    choke_multiplier: float


class WhitsonImperialConfigError(Exception):
    """Invalid or missing whitson_imperial.ini configuration."""


def _parse_float(
    section: configparser.SectionProxy,
    key: str,
    *,
    required: bool,
    default: float = 1.0,
) -> float:
    raw = section.get(key, fallback="").strip()
    if not raw:
        if required:
            raise WhitsonImperialConfigError(
                f"[{_SECTION}] missing required key: {key}"
            )
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise WhitsonImperialConfigError(
            f"[{_SECTION}] {key} must be numeric, got {raw!r}"
        ) from exc


def load_whitson_imperial_factors(
    ini_path: Optional[str | Path] = None,
) -> WhitsonImperialFactors:
    """Load conversion factors from whitson_imperial.ini."""
    path = Path(ini_path or get_whitson_imperial_ini_path())
    if not path.is_file():
        raise WhitsonImperialConfigError(f"Whitson imperial INI not found: {path}")

    cfg = configparser.ConfigParser(interpolation=None)
    cfg.read(path, encoding="utf-8")
    if _SECTION not in cfg:
        raise WhitsonImperialConfigError(f"Missing [{_SECTION}] section in {path}")

    section = cfg[_SECTION]

    return WhitsonImperialFactors(
        gas_e3m3_to_mcf=_parse_float(
            section, "gathered_gas_e3m3_per_day_to_mcf_per_day", required=True
        ),
        cond_m3_to_bbl=_parse_float(
            section, "condensate_m3_per_day_to_bbl_per_day", required=True
        ),
        water_m3_to_bbl=_parse_float(
            section, "gath_water_m3_per_day_to_bbl_per_day", required=True
        ),
        tubing_kpa_to_psi=_parse_float(
            section, "tubing_pressure_kpa_to_psi", required=True
        ),
        casing_kpa_to_psi=_parse_float(
            section, "casing_pressure_kpa_to_psi", required=True
        ),
        choke_multiplier=_parse_float(
            section,
            "choke_size_multiplier",
            required=False,
            default=0.03937,
        ),
    )


def _nn_metric(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        import math

        f = float(value)
        if math.isnan(f):
            return None
    except (TypeError, ValueError):
        return None
    return max(0.0, f)


def apply_imperial_to_rates(
    *,
    gathered_gas_e3m3: Any,
    cond_m3: Any,
    gath_water_m3: Any,
    tubing_kpa: Any,
    casing_kpa: Any,
    choke: Any,
    factors: WhitsonImperialFactors,
) -> dict[str, Optional[float]]:
    """Return imperial-scaled Whitson rate/pressure fields (None where input is null)."""

    def scale(v: Any, factor: float) -> Optional[float]:
        n = _nn_metric(v)
        if n is None:
            return None
        return n * factor

    return {
        "qg_sc": scale(gathered_gas_e3m3, factors.gas_e3m3_to_mcf),
        "qo_sc": scale(cond_m3, factors.cond_m3_to_bbl),
        "qw_sc": scale(gath_water_m3, factors.water_m3_to_bbl),
        "p_tubing": scale(tubing_kpa, factors.tubing_kpa_to_psi),
        "p_casing": scale(casing_kpa, factors.casing_kpa_to_psi),
        "choke_size": scale(choke, factors.choke_multiplier),
    }


def build_payload_point(
    row: Mapping[str, Any],
    factors: WhitsonImperialFactors,
    *,
    date_iso: str,
) -> dict:
    """One Whitson production_data point from a PCE_Production row."""
    imperial = apply_imperial_to_rates(
        gathered_gas_e3m3=row.get("Gathered Gas (e³m³/d)"),
        cond_m3=row.get("Condensate WH (m³/d)"),
        gath_water_m3=row.get("Gath. Water Rate (m³/d)"),
        tubing_kpa=row.get("Tubing Pressure (kPa)"),
        casing_kpa=row.get("Casing Pressure (kPa)"),
        choke=row.get("Choke Size"),
        factors=factors,
    )
    return {
        "date": date_iso,
        "qo_sc": imperial["qo_sc"],
        "qg_sc": imperial["qg_sc"],
        "qw_sc": imperial["qw_sc"],
        "p_wf_measured": None,
        "p_tubing": imperial["p_tubing"],
        "p_casing": imperial["p_casing"],
        "qg_gas_lift": None,
        "liquid_level": None,
        "choke_size": imperial["choke_size"],
        "line_pressure": None,
    }
