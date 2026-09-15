"""Shared parsing primitives for administrative source adapters."""

import gzip
import json
import re
from pathlib import Path
from typing import Final

from shapely.geometry import shape

CURRENT_FIELD_MAP: Final[dict[str, str]] = {
    "a02_xa": "current_commune_code",
    "a03_ten": "current_commune_name",
    "a04_tentinh": "province_name",
    "a05_truocsn": "predecessors_text",
    "a06_trungtamhc": "admin_center",
    "a07_dt": "legal_area_km2",
    "a08_ds": "legal_population",
}

INDEX_ASSET_ID: Final = "sonla-admin-2025-index"
RESOLUTION_PAGE_ASSET_ID: Final = "resolution-1681-page"
RESOLUTION_PDF_ASSET_ID: Final = "resolution-1681-pdf"
GADM_ARCHIVE_ASSET_ID: Final = "gadm-vnm-4-1-archive"
PDF_LINK = re.compile(r"href=[\"']([^\"']*1681[^\"']*\.pdf[^\"']*)[\"']", re.IGNORECASE)

def classify_unit(name: str) -> str:
    """Classify a current Vietnamese commune-level unit from its legal name."""
    if name.startswith("Phường "):
        return "ward"
    if name.startswith("Xã "):
        return "commune"
    raise ValueError(f"unexpected current unit type: {name}")

def _read_text(path: Path, *, errors: str = "strict") -> str:
    with path.open("rb") as raw:
        gzip_encoded = raw.read(2) == b"\x1f\x8b"
    if gzip_encoded:
        with gzip.open(path, mode="rt", encoding="utf-8-sig", errors=errors) as stream:
            return stream.read()
    return path.read_text(encoding="utf-8-sig", errors=errors)

def _load_json(path: Path) -> object:
    return json.loads(_read_text(path))

def _index_rows(path: Path) -> list[dict[str, object]]:
    payload = _load_json(path)
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        rows = next(
            (
                value
                for key, value in payload.items()
                if key in {"data", "items", "rows", "result"} and isinstance(value, list)
            ),
            None,
        )
    else:
        rows = None
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise ValueError("admin index must contain a list of unit objects")
    return [dict(row) for row in rows]

def _feature_from_response(path: Path) -> tuple[dict[str, object], object]:
    payload = _load_json(path)
    if not isinstance(payload, dict):
        raise TypeError(f"admin geometry response is not an object: {path.name}")
    features = payload.get("features")
    if not isinstance(features, list) or len(features) != 1 or not isinstance(features[0], dict):
        raise ValueError(f"admin geometry response must contain exactly one feature: {path.name}")
    feature = features[0]
    properties = feature.get("properties")
    geometry = feature.get("geometry")
    if not isinstance(properties, dict) or not isinstance(geometry, dict):
        raise TypeError(f"admin geometry feature is incomplete: {path.name}")
    return dict(properties), shape(geometry)

def _number(value: object, field: str) -> float:
    if isinstance(value, bool):
        raise TypeError(f"{field} is not numeric")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        normalized = value.strip().replace(" ", "")
        if normalized.count(",") == 1 and "." not in normalized:
            normalized = normalized.replace(",", ".")
        else:
            normalized = normalized.replace(",", "")
        try:
            return float(normalized)
        except ValueError as exc:
            raise TypeError(f"{field} is not numeric") from exc
    raise TypeError(f"{field} is not numeric")
