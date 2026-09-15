"""Native-grid exposure raster and valid-time flood-evidence harmonization."""

from __future__ import annotations

import json
import re
import unicodedata
from datetime import date
from pathlib import Path
from typing import Final

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.errors import WindowError
from rasterio.features import geometry_mask, geometry_window
from rasterio.windows import Window
from shapely.geometry import mapping
from shapely.geometry.base import BaseGeometry

from flashflood_data.raster import COG_PROFILE
from flashflood_data.storage.atomic import atomic_target

EVENT_COLUMNS: Final[dict[str, str]] = {
    "STT": "source_row_number",
    "Năm": "event_year",
    "Huyện/Khu vực huyện": "original_district_text",
    "Xã/Địa điểm cụ thể": "original_place_text",
    "Ngày xảy ra": "original_date_text",
    "Thời gian/Giờ xảy ra": "original_time_text",
    "Loại sự kiện": "event_type",
    "Mô tả ngắn": "description",
    "Nguồn tin": "source_name",
    "Link minh chứng": "evidence_url",
    "Ghi chú": "notes",
}
_REFORM_DATE: Final = date(2025, 7, 1)
_DATE_RANGE = re.compile(r"^(\d{1,2})\s*-\s*(\d{1,2})/(\d{1,2})/(\d{4})$")
_DATE_RANGE_CROSS_MONTH = re.compile(
    r"^(\d{1,2})/(\d{1,2})\s*-\s*(\d{1,2})/(\d{1,2})/(\d{4})$"
)
_DATE_SINGLE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")


def harmonize_worldpop(raw_path: Path, core_aoi: BaseGeometry, output: Path) -> Path:
    """Exact-clip a WorldPop raster while retaining its native grid and cell values."""
    if core_aoi.is_empty:
        raise ValueError("Core AOI must not be empty")
    with rasterio.open(raw_path) as source:
        if source.count != 1 or source.crs is None:
            raise ValueError("WorldPop input must be a single-band raster with a CRS")
        dtype = source.dtypes[0]
        if not np.issubdtype(np.dtype(dtype), np.number):
            raise ValueError("WorldPop input must have a numeric cell type")
        try:
            crop = geometry_window(source, [mapping(core_aoi)]).round_offsets().round_lengths()
            crop = crop.intersection(Window(0, 0, source.width, source.height))
        except WindowError as error:
            raise ValueError("Core AOI does not intersect WorldPop input") from error
        if crop.width <= 0 or crop.height <= 0:
            raise ValueError("Core AOI does not intersect WorldPop input")
        profile = source.profile.copy()
        profile.update(
            COG_PROFILE,
            dtype="float32",
            nodata=float(source.nodata) if source.nodata is not None else None,
            width=int(crop.width),
            height=int(crop.height),
            transform=source.window_transform(crop),
            predictor=3,
        )
        output.parent.mkdir(parents=True, exist_ok=True)
        with atomic_target(output) as partial, rasterio.open(partial, "w", **profile) as destination:
            values = source.read(1, window=crop, out_dtype="float32")
            valid = source.read_masks(1, window=crop) > 0
            inside = geometry_mask(
                [mapping(core_aoi)],
                out_shape=(int(crop.height), int(crop.width)),
                transform=source.window_transform(crop),
                invert=True,
            )
            valid &= inside
            if source.nodata is not None:
                values[~valid] = np.float32(source.nodata)
            destination.write(values, 1)
            destination.write_mask(np.where(valid, 255, 0).astype("uint8"))
            factors = [factor for factor in (2, 4, 8, 16) if min(destination.width, destination.height) >= factor]
            if factors:
                destination.build_overviews(factors, rasterio.enums.Resampling.average)
                destination.update_tags(ns="rio_overview", resampling="average")
    return output


def _text(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    return str(value)


def _numeric_row_number(value: object) -> int | None:
    if isinstance(value, bool) or value is None or pd.isna(value):
        return None
    if isinstance(value, (int, float, np.integer, np.floating)) and float(value).is_integer():
        return int(value)
    return None


def _parse_event_dates(value: object) -> tuple[date | None, date | None]:
    text = _text(value)
    if text is None:
        return None, None
    normalized = " ".join(text.split())
    try:
        match = _DATE_SINGLE.fullmatch(normalized)
        if match:
            day, month, year = map(int, match.groups())
            parsed = date(year, month, day)
            return parsed, parsed
        match = _DATE_RANGE.fullmatch(normalized)
        if match:
            start_day, end_day, month, year = map(int, match.groups())
            return date(year, month, start_day), date(year, month, end_day)
        match = _DATE_RANGE_CROSS_MONTH.fullmatch(normalized)
        if match:
            start_day, start_month, end_day, end_month, year = map(int, match.groups())
            return date(year, start_month, start_day), date(year, end_month, end_day)
    except ValueError:
        return None, None
    return None, None


def read_historical_events(path: Path) -> pd.DataFrame:
    """Read numbered evidence rows while preserving source fields and parseable dates."""
    frame = pd.read_excel(path, dtype=object)
    missing = sorted(set(EVENT_COLUMNS).difference(frame.columns))
    if missing:
        raise ValueError(f"historical evidence workbook is missing columns: {', '.join(missing)}")
    rows: list[dict[str, object]] = []
    for _, source in frame.iterrows():
        number = _numeric_row_number(source["STT"])
        if number is None:
            continue
        start, end = _parse_event_dates(source["Ngày xảy ra"])
        year_value = _numeric_row_number(source["Năm"])
        row = {target: _text(source[original]) for original, target in EVENT_COLUMNS.items()}
        row["source_row_number"] = number
        row["event_id"] = f"historical-flood:{number}"
        row["event_year"] = year_value
        row["event_date_start"] = start
        row["event_date_end"] = end
        rows.append(row)
    return pd.DataFrame(rows, columns=(
        "event_id", "source_row_number", "event_year", "original_district_text",
        "original_place_text", "original_date_text", "original_time_text", "event_type",
        "description", "source_name", "evidence_url", "notes", "event_date_start", "event_date_end",
    ))


def _normal_form(value: str) -> str:
    normalized = " ".join(unicodedata.normalize("NFC", value).casefold().split())
    for prefix in ("xã ", "phường ", "thị trấn "):
        if normalized.startswith(prefix):
            return normalized[len(prefix) :]
    return normalized


def _has_name(text: str, name: str) -> bool:
    pattern = rf"(?<![\w]){re.escape(name)}(?![\w])"
    return re.search(pattern, text) is not None


def _explicit_names(
    place: object, known_names: set[str], *, exclude_former_markers: bool
) -> list[str]:
    """Find known commune names in the place field, never in district-only evidence."""
    text = _text(place)
    if text is None:
        return []
    normalized = _normal_form(text)
    matches = [name for name in known_names if _has_name(normalized, name)]
    # A label such as "Nậm Păm (cũ)" is expressly a former area only when matching
    # against post-reform units. It remains valid historical evidence before reform.
    if exclude_former_markers:
        matches = [name for name in matches if f"{name} (cũ)" not in normalized]
    return sorted(matches)


def _json(values: list[str]) -> str:
    return json.dumps(sorted(set(values)), ensure_ascii=False, separators=(",", ":"))


def _current_lookup(current_admin: gpd.GeoDataFrame) -> dict[str, tuple[set[str], bool]]:
    required = {"current_commune_code", "current_commune_name"}
    missing = sorted(required.difference(current_admin.columns))
    if missing:
        raise ValueError(f"current administration is missing columns: {', '.join(missing)}")
    lookup: dict[str, tuple[set[str], bool]] = {}
    for _, row in current_admin.iterrows():
        name = _normal_form(str(row["current_commune_name"]))
        codes, _ = lookup.setdefault(name, (set(), False))
        codes.add(str(row["current_commune_code"]).zfill(5))
    return lookup


def _candidate_codes(row: pd.Series) -> set[str]:
    codes: set[str] = set()
    direct = row.get("current_commune_code")
    if direct is not None and not pd.isna(direct):
        codes.add(str(direct).zfill(5))
    for column in ("candidate_current_commune_codes_json", "current_commune_codes_json"):
        raw = row.get(column)
        if raw is None or (not isinstance(raw, list) and pd.isna(raw)):
            continue
        try:
            values = json.loads(raw) if isinstance(raw, str) else raw
        except json.JSONDecodeError as error:
            raise ValueError(f"crosswalk {column} is not valid JSON") from error
        if not isinstance(values, list) or not all(isinstance(value, str) for value in values):
            raise ValueError(f"crosswalk {column} must contain a JSON string list")
        codes.update(value.zfill(5) for value in values)
    return codes


def _historical_lookup(crosswalk: pd.DataFrame) -> dict[str, tuple[set[str], bool]]:
    required = {"old_admin_name", "current_commune_code", "match_status"}
    missing = sorted(required.difference(crosswalk.columns))
    if missing:
        raise ValueError(f"administrative crosswalk is missing columns: {', '.join(missing)}")
    lookup: dict[str, tuple[set[str], bool]] = {}
    for _, row in crosswalk.iterrows():
        status = str(row["match_status"])
        if status not in {"matched", "ambiguous"}:
            continue
        codes = _candidate_codes(row)
        if not codes and status == "matched":
            continue
        name = _normal_form(str(row["old_admin_name"]))
        stored_codes, was_ambiguous = lookup.setdefault(name, (set(), False))
        stored_codes.update(codes)
        lookup[name] = (stored_codes, was_ambiguous or status == "ambiguous")
    return lookup


def resolve_event_administration(
    events: pd.DataFrame, current_admin: gpd.GeoDataFrame, crosswalk: pd.DataFrame
) -> pd.DataFrame:
    """Resolve only explicit places against the administration valid on the event date."""
    current = _current_lookup(current_admin)
    historical = _historical_lookup(crosswalk)
    records: list[dict[str, object]] = []
    for _, event in events.iterrows():
        start = event.get("event_date_start")
        event_date = start if isinstance(start, date) else None
        lookup = historical if event_date is not None and event_date < _REFORM_DATE else current
        names = (
            _explicit_names(
                event.get("original_place_text"),
                set(lookup),
                exclude_former_markers=event_date >= _REFORM_DATE,
            )
            if event_date
            else []
        )
        resolved = [lookup[name] for name in names]
        codes = sorted(set().union(*(value[0] for value in resolved))) if resolved else []
        one_each = bool(names) and all(len(value[0]) == 1 and not value[1] for value in resolved)
        if one_each:
            status, confidence = "matched", 1.0
        elif codes or any(value[1] for value in resolved):
            status, confidence = "ambiguous", 0.5
        else:
            status, confidence = "unresolved", 0.0
        record = event.to_dict()
        record.update(
            {
                "administration_valid_on": event_date.isoformat() if event_date else None,
                "administration_regime": (
                    "historical" if event_date and event_date < _REFORM_DATE else "current" if event_date else None
                ),
                "admin_candidate_names_json": _json(names),
                "current_commune_codes_json": _json(codes),
                "match_status": status,
                "match_confidence": confidence,
            }
        )
        records.append(record)
    return pd.DataFrame(records)
