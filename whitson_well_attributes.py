"""
PCE_WM well metadata for Whitson+ create / PATCH / custom_attributes sync.

Phase 1: Pad, Formation, Fault Block (sub_field), Lateral Length, surface lat/long,
UWI, and Layer Producer (custom attribute only).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from numbers import Real
from typing import Any, Callable, Dict, List, Optional

import numpy as np
import pandas as pd

import whitson_connect

LAYER_PRODUCER_ATTRIBUTE_NAME = "Layer Producer"

_NATIVE_CREATE_KEYS = (
    "pad_name",
    "formation",
    "sub_field",
    "l_w",
    "surf_lat",
    "surf_long",
    "uwi_api",
)

_NATIVE_PATCH_KEYS = (
    "pad_name",
    "formation",
    "sub_field",
    "l_w",
    "surf_lat",
    "surf_long",
)

_FETCH_WM_METADATA_SQL = """
SELECT TOP 1
      NULLIF(LTRIM(RTRIM(CAST(wm.[Pad Name] AS NVARCHAR(4000)))), N'') AS PadName
    , NULLIF(LTRIM(RTRIM(CAST(wm.[Formation Producer] AS NVARCHAR(4000)))), N'') AS FormationProducer
    , NULLIF(LTRIM(RTRIM(CAST(wm.[Fault Block] AS NVARCHAR(4000)))), N'') AS FaultBlock
    , wm.[Lateral Length] AS LateralLength
    , NULLIF(LTRIM(RTRIM(CAST(wm.[Layer Producer] AS NVARCHAR(4000)))), N'') AS LayerProducer
    , wm.[Surface Location Latitude (NAD83)] AS SurfLat
    , wm.[Surface Location Longitude (NAD83)] AS SurfLong
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
    uwi_api: Optional[str] = None
    layer_producer: Optional[str] = None


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
    """Load phase-1 WM fields for a PCE_Production well name (composite match)."""
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
        layer_producer=_coerce_optional_str(row[4]),
        surf_lat=_coerce_optional_float(row[5]),
        surf_long=_coerce_optional_float(row[6]),
        uwi_api=_coerce_optional_str(row[7]),
    )


def _native_fields_from_metadata(
    metadata: WellMetadata,
    *,
    include_uwi: bool,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if metadata.pad_name:
        out["pad_name"] = metadata.pad_name
    if metadata.formation:
        out["formation"] = metadata.formation
    if metadata.sub_field:
        out["sub_field"] = metadata.sub_field
    if metadata.l_w is not None:
        out["l_w"] = metadata.l_w
    if metadata.surf_lat is not None:
        out["surf_lat"] = metadata.surf_lat
    if metadata.surf_long is not None:
        out["surf_long"] = metadata.surf_long
    if include_uwi and metadata.uwi_api:
        out["uwi_api"] = metadata.uwi_api
    return out


def build_whitson_well_create_payload(
    metadata: WellMetadata,
    *,
    project_id: int,
    name: str,
    uwi_api: Optional[str] = None,
) -> Dict[str, Any]:
    """Create-well body with native WM fields and optional Layer Producer custom attribute."""
    payload: Dict[str, Any] = {
        "project_id": project_id,
        "name": name,
    }
    payload.update(_native_fields_from_metadata(metadata, include_uwi=True))
    if uwi_api and "uwi_api" not in payload:
        payload["uwi_api"] = uwi_api
    layer = _coerce_optional_str(metadata.layer_producer)
    if layer:
        payload["custom_attributes"] = [
            {
                "attribute_name": LAYER_PRODUCER_ATTRIBUTE_NAME,
                "value": layer,
            }
        ]
    return payload


def build_whitson_well_patch_payload(
    well_id: int,
    metadata: WellMetadata,
) -> Dict[str, Any]:
    """PATCH /wells entry for native fields only."""
    patch = {"id": well_id}
    patch.update(_native_fields_from_metadata(metadata, include_uwi=False))
    return patch


def build_layer_producer_custom_bulk(
    well_id: int,
    layer_producer: Optional[str],
) -> List[Dict[str, Any]]:
    value = _coerce_optional_str(layer_producer)
    if not value:
        return []
    return [
        {
            "well_id": well_id,
            "attribute_name": LAYER_PRODUCER_ATTRIBUTE_NAME,
            "value": value,
        }
    ]


def sync_whitson_well_attributes(
    whitson: whitson_connect.WhitsonConnection,
    well_id: int,
    metadata: WellMetadata,
    *,
    log_cb: Optional[Callable[[str], None]] = None,
) -> bool:
    """
    PATCH native WM fields and set Layer Producer custom attribute when present.
    Returns True if all attempted API calls succeeded (or nothing to sync).
    """
    def log(msg: str) -> None:
        if log_cb:
            log_cb(msg)

    ok = True
    patch = build_whitson_well_patch_payload(well_id, metadata)
    if len(patch) > 1:
        resp = whitson.edit_well_info([patch])
        if resp.status_code < 200 or resp.status_code >= 300:
            ok = False
            log(
                f"  WM attribute PATCH failed (HTTP {resp.status_code}): "
                f"{(resp.text or '')[:300]}"
            )
        else:
            log("  WM native attributes synced.")

    bulk = build_layer_producer_custom_bulk(well_id, metadata.layer_producer)
    if bulk:
        resp = whitson.edit_custom_attribute_bulk(bulk)
        if resp.status_code < 200 or resp.status_code >= 300:
            ok = False
            log(
                f"  Layer Producer custom attribute failed (HTTP {resp.status_code}): "
                f"{(resp.text or '')[:300]}"
            )
        else:
            log(f"  Layer Producer synced: {metadata.layer_producer!r}")

    return ok
