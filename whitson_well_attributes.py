"""
PCE_WM well metadata for Whitson+ create / PATCH sync.

Native fields: pad, formation, fault block, lateral length, surface lat/long,
bottomhole toe lat/long, and UWI on create.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

import numpy as np
import pandas as pd

import whitson_connect
from whitson_imperial_units import (
    WhitsonImperialFactors,
    load_whitson_imperial_factors,
)

_NATIVE_CREATE_KEYS = (
    "pad_name",
    "formation",
    "sub_field",
    "l_w",
    "surf_lat",
    "surf_long",
    "bothole_lat",
    "bothole_long",
    "fluid_pumped",
    "prop_pumped",
    "uwi_api",
)

_NATIVE_PATCH_KEYS = (
    "pad_name",
    "formation",
    "sub_field",
    "l_w",
    "surf_lat",
    "surf_long",
    "bothole_lat",
    "bothole_long",
    "fluid_pumped",
    "prop_pumped",
)

_FETCH_WM_METADATA_SQL = """
SELECT TOP 1
      NULLIF(LTRIM(RTRIM(CAST(wm.[Pad Name] AS NVARCHAR(4000)))), N'') AS PadName
    , NULLIF(LTRIM(RTRIM(CAST(wm.[Formation Producer] AS NVARCHAR(4000)))), N'') AS FormationProducer
    , NULLIF(LTRIM(RTRIM(CAST(wm.[Fault Block] AS NVARCHAR(4000)))), N'') AS FaultBlock
    , wm.[Lateral Length] AS LateralLength
    , COALESCE(wm.[Surface Hole Latitude], wm.[Surface Location Latitude (NAD83)]) AS SurfLat
    , COALESCE(wm.[Surface Hole Longitude], wm.[Surface Location Longitude (NAD83)]) AS SurfLong
    , COALESCE(wm.[Bottom Hole Latitude], wm.[Bottom Location Latitude (NAD83)]) AS ToeLat
    , COALESCE(wm.[Bottom Hole Longitude], wm.[Bottom Location Longitude (NAD83)]) AS ToeLong
    , wm.[Fluid Pumped (m³)] AS FluidPumped
    , wm.[Proppant Pumped (t)] AS PropPumped
    , NULLIF(LTRIM(RTRIM(CAST(wm.[Value Navigator UWI] AS NVARCHAR(4000)))), N'') AS UwiApi
FROM dbo.PCE_WM AS wm
WHERE (
          wm.[Well Name] = ?
       OR (
              NULLIF(RTRIM(CAST(wm.[Composite Name] AS NVARCHAR(4000))), N'') IS NOT NULL
          AND wm.[Composite Name] = ?
          )
      )
  AND (wm.[Exception] IS NULL OR wm.[Exception] = N'' OR wm.[Exception] = N'N')
"""


@dataclass
class WellMetadata:
    pad_name: Optional[str] = None
    formation: Optional[str] = None
    sub_field: Optional[str] = None
    l_w: Optional[float] = None
    surf_lat: Optional[float] = None
    surf_long: Optional[float] = None
    toe_lat: Optional[float] = None
    toe_long: Optional[float] = None
    fluid_pumped: Optional[float] = None
    prop_pumped: Optional[float] = None
    uwi_api: Optional[str] = None


@dataclass(frozen=True)
class AttributeOption:
    """A selectable attribute for the Push Attributes UI."""

    id: str
    label: str
    native_keys: Tuple[str, ...]


# Ordered registry of selectable attributes. Each entry maps a UI checkbox to the
# native Whitson patch keys it writes. Adding an entry here auto-adds a checkbox
# and selective-push support.
ATTRIBUTE_OPTIONS: Tuple[AttributeOption, ...] = (
    AttributeOption("pad", "Pad Name", ("pad_name",)),
    AttributeOption("formation", "Formation", ("formation",)),
    AttributeOption("fault_block", "Fault Block", ("sub_field",)),
    AttributeOption("lateral_length", "Lateral Length", ("l_w",)),
    AttributeOption(
        "surface_location", "Surface Location (lat/long)", ("surf_lat", "surf_long")
    ),
    AttributeOption(
        "bottomhole_location",
        "Bottomhole Location (lat/long)",
        ("bothole_lat", "bothole_long"),
    ),
    AttributeOption("fluid_pumped", "Fluid Pumped", ("fluid_pumped",)),
    AttributeOption("prop_pumped", "Proppant Pumped", ("prop_pumped",)),
)


def native_keys_for_selected(selected_ids: Iterable[str]) -> set[str]:
    """Native Whitson patch keys covered by the selected attribute option ids."""
    wanted = set(selected_ids)
    keys: set[str] = set()
    for opt in ATTRIBUTE_OPTIONS:
        if opt.id in wanted:
            keys.update(opt.native_keys)
    return keys


def _coerce_optional_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, float) and (np.isnan(value) or not np.isfinite(value)):
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float, np.integer, np.floating)):
        f = float(value)
        return f if np.isfinite(f) else None
    if isinstance(value, Decimal):
        f = float(value)
        return f if np.isfinite(f) else None
    if isinstance(value, str):
        s = value.strip().replace(",", "")
        if not s or s.lower() in ("-", "nan", "none", "n/a", "na"):
            return None
        try:
            f = float(s)
            return f if np.isfinite(f) else None
        except ValueError:
            return None
    if pd.isna(value):
        return None
    try:
        f = float(value)
        return f if np.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def _coerce_optional_str(value: Any) -> Optional[str]:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    if pd.isna(value):
        return None
    s = str(value).strip()
    return s if s and s.lower() != "nan" else None


def fetch_well_metadata_for_whitson(
    conn,
    production_well_name: str,
) -> WellMetadata:
    """Load WM fields for a PCE_Production well name (composite match)."""
    cur = conn.cursor()
    cur.execute(
        _FETCH_WM_METADATA_SQL,
        production_well_name,
        production_well_name,
    )
    row = cur.fetchone()
    if not row:
        return WellMetadata()

    return WellMetadata(
        pad_name=_coerce_optional_str(row[0]),
        formation=_coerce_optional_str(row[1]),
        sub_field=_coerce_optional_str(row[2]),
        l_w=_coerce_optional_float(row[3]),
        surf_lat=_coerce_optional_float(row[4]),
        surf_long=_coerce_optional_float(row[5]),
        toe_lat=_coerce_optional_float(row[6]),
        toe_long=_coerce_optional_float(row[7]),
        fluid_pumped=_coerce_optional_float(row[8]),
        prop_pumped=_coerce_optional_float(row[9]),
        uwi_api=_coerce_optional_str(row[10]),
    )


def _resolve_factors(
    factors: Optional[WhitsonImperialFactors],
) -> WhitsonImperialFactors:
    return factors if factors is not None else load_whitson_imperial_factors()


def _native_fields_from_metadata(
    metadata: WellMetadata,
    factors: WhitsonImperialFactors,
    *,
    include_uwi: bool,
    only_keys: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """Build native Whitson fields from WM metadata (with imperial conversions).

    When ``only_keys`` is provided, only those native keys are emitted.
    """
    allowed = set(only_keys) if only_keys is not None else None

    def wants(key: str) -> bool:
        return allowed is None or key in allowed

    out: Dict[str, Any] = {}
    if wants("pad_name") and metadata.pad_name:
        out["pad_name"] = metadata.pad_name
    if wants("formation") and metadata.formation:
        out["formation"] = metadata.formation
    if wants("sub_field") and metadata.sub_field:
        out["sub_field"] = metadata.sub_field
    if wants("l_w") and metadata.l_w is not None:
        out["l_w"] = metadata.l_w * factors.lateral_length_m_to_ft
    if wants("surf_lat") and metadata.surf_lat is not None:
        out["surf_lat"] = metadata.surf_lat
    if wants("surf_long") and metadata.surf_long is not None:
        out["surf_long"] = metadata.surf_long
    if wants("bothole_lat") and metadata.toe_lat is not None:
        out["bothole_lat"] = metadata.toe_lat
    if wants("bothole_long") and metadata.toe_long is not None:
        out["bothole_long"] = metadata.toe_long
    if wants("fluid_pumped") and metadata.fluid_pumped is not None:
        out["fluid_pumped"] = metadata.fluid_pumped * factors.fluid_pumped_m3_to_bbl
    if wants("prop_pumped") and metadata.prop_pumped is not None:
        out["prop_pumped"] = metadata.prop_pumped * factors.prop_pumped_tonnes_to_lb
    if include_uwi and wants("uwi_api") and metadata.uwi_api:
        out["uwi_api"] = metadata.uwi_api
    return out


def build_whitson_well_create_payload(
    metadata: WellMetadata,
    *,
    project_id: int,
    name: str,
    uwi_api: Optional[str] = None,
    factors: Optional[WhitsonImperialFactors] = None,
) -> Dict[str, Any]:
    """Create-well body with native WM fields."""
    payload: Dict[str, Any] = {
        "project_id": project_id,
        "name": name,
    }
    payload.update(
        _native_fields_from_metadata(
            metadata, _resolve_factors(factors), include_uwi=True
        )
    )
    if uwi_api and "uwi_api" not in payload:
        payload["uwi_api"] = uwi_api
    return payload


def build_whitson_well_patch_payload(
    well_id: int,
    metadata: WellMetadata,
    factors: Optional[WhitsonImperialFactors] = None,
    *,
    only_keys: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """PATCH /wells entry for native fields only."""
    patch = {"id": well_id}
    patch.update(
        _native_fields_from_metadata(
            metadata,
            _resolve_factors(factors),
            include_uwi=False,
            only_keys=only_keys,
        )
    )
    return patch


def build_selected_attributes_patch(
    well_id: int,
    metadata: WellMetadata,
    selected_ids: Iterable[str],
    factors: Optional[WhitsonImperialFactors] = None,
) -> Dict[str, Any]:
    """PATCH /wells entry limited to the selected attribute option ids."""
    return build_whitson_well_patch_payload(
        well_id,
        metadata,
        factors,
        only_keys=native_keys_for_selected(selected_ids),
    )


def sync_whitson_well_attributes(
    whitson: whitson_connect.WhitsonConnection,
    well_id: int,
    metadata: WellMetadata,
    *,
    factors: Optional[WhitsonImperialFactors] = None,
    log_cb: Optional[Callable[[str], None]] = None,
) -> bool:
    """
    PATCH native WM fields to Whitson+.
    Returns True if the API call succeeded (or nothing to sync).
    """
    def log(msg: str) -> None:
        if log_cb:
            log_cb(msg)

    patch = build_whitson_well_patch_payload(well_id, metadata, factors)
    if len(patch) <= 1:
        return True

    resp = whitson.edit_well_info([patch])
    if resp.status_code < 200 or resp.status_code >= 300:
        log(
            f"  WM attribute PATCH failed (HTTP {resp.status_code}): "
            f"{(resp.text or '')[:300]}"
        )
        return False

    log("  WM native attributes synced.")
    return True


def sync_selected_whitson_attributes(
    whitson: whitson_connect.WhitsonConnection,
    well_id: int,
    metadata: WellMetadata,
    selected_ids: Iterable[str],
    *,
    factors: Optional[WhitsonImperialFactors] = None,
    log_cb: Optional[Callable[[str], None]] = None,
) -> bool:
    """
    PATCH only the selected attribute option ids to Whitson+.
    Returns True if the API call succeeded (or nothing to sync).
    """
    def log(msg: str) -> None:
        if log_cb:
            log_cb(msg)

    patch = build_selected_attributes_patch(
        well_id, metadata, selected_ids, factors
    )
    if len(patch) <= 1:
        log("  No selected attributes had values to push.")
        return True

    resp = whitson.edit_well_info([patch])
    if resp.status_code < 200 or resp.status_code >= 300:
        log(
            f"  WM attribute PATCH failed (HTTP {resp.status_code}): "
            f"{(resp.text or '')[:300]}"
        )
        return False

    pushed = ", ".join(k for k in patch.keys() if k != "id")
    log(f"  Synced attributes: {pushed}")
    return True
