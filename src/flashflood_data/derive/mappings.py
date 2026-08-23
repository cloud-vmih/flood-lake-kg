"""Conservative spatial crosswalks between historical and current administration."""

from __future__ import annotations

import json
import unicodedata
from dataclasses import dataclass

import geopandas as gpd
import pandas as pd

from flashflood_data.derive._spatial import checked_basins

ADMIN_PREFIXES = ("xã ", "phường ", "thị trấn ")
_PARTIAL_MERGER = "một phần"


@dataclass(frozen=True)
class ParsedAdminName:
    """One official predecessor name with both written and matching forms."""

    written_name: str
    admin_type: str
    normalized_name: str


def normalize_admin_name(value: str) -> str:
    """Normalize a Vietnamese administrative name without its level prefix."""
    text = unicodedata.normalize("NFC", value).strip()
    folded = text.casefold()
    for prefix in ADMIN_PREFIXES:
        if folded.startswith(prefix):
            text = text[len(prefix) :]
            break
    return " ".join(text.casefold().split())


def _without_admin_prefix(value: str) -> str:
    text = unicodedata.normalize("NFC", value).strip()
    folded = text.casefold()
    for prefix in ADMIN_PREFIXES:
        if folded.startswith(prefix):
            return " ".join(text[len(prefix) :].split())
    return " ".join(text.split())


def parse_predecessors(value: str) -> list[ParsedAdminName]:
    """Parse comma-separated official predecessors and reject partial-merger claims."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("predecessors_text must be a non-empty string")
    if _PARTIAL_MERGER in unicodedata.normalize("NFC", value).casefold():
        raise ValueError("predecessors_text contains unsupported partial merger phrase: một phần")
    parsed: list[ParsedAdminName] = []
    for part in value.split(","):
        written_name = " ".join(unicodedata.normalize("NFC", part).strip().split())
        if not written_name:
            raise ValueError("predecessors_text contains an empty predecessor")
        folded = written_name.casefold()
        admin_type = ""
        for prefix in ADMIN_PREFIXES:
            if folded.startswith(prefix):
                admin_type = written_name[: len(prefix)].strip()
                break
        parsed.append(
            ParsedAdminName(
                written_name=written_name,
                admin_type=admin_type,
                normalized_name=normalize_admin_name(written_name),
            )
        )
    return parsed


def _require_columns(frame: pd.DataFrame, required: set[str], label: str) -> None:
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"{label} is missing required columns: {', '.join(missing)}")


def _overlap_fraction(old_geometry: object, current_geometry: object) -> tuple[float, float]:
    old_area = float(old_geometry.area)
    if old_area <= 0:
        return 0.0, 0.0
    overlap_area = float(old_geometry.intersection(current_geometry).area)
    return overlap_area / old_area, overlap_area


def build_admin_crosswalk(current: gpd.GeoDataFrame, historical: gpd.GeoDataFrame) -> pd.DataFrame:
    """Map official predecessor names only when name and full spatial evidence agree."""
    _require_columns(
        current,
        {"current_commune_code", "current_commune_name", "predecessors_text", "geometry"},
        "current admin",
    )
    _require_columns(historical, {"old_admin_id", "old_admin_name", "valid_to", "geometry"}, "historical admin")
    if current.crs is None or historical.crs is None:
        raise ValueError("current and historical admin layers must have a CRS")
    historical_for_match = historical.to_crs(current.crs) if historical.crs != current.crs else historical
    historical_rows = historical_for_match.copy()
    historical_rows["_normalized_name"] = historical_rows["old_admin_name"].map(
        lambda value: normalize_admin_name(str(value))
    )
    by_name = {
        name: group.sort_values("old_admin_id", kind="stable")
        for name, group in historical_rows.groupby("_normalized_name", sort=True)
    }
    records: list[dict[str, object]] = []
    for _, current_row in current.sort_values("current_commune_code", kind="stable").iterrows():
        current_code = str(current_row["current_commune_code"])
        current_name = str(current_row["current_commune_name"])
        for parsed in parse_predecessors(str(current_row["predecessors_text"])):
            candidates = by_name.get(parsed.normalized_name)
            candidate_rows = [] if candidates is None else list(candidates.iterrows())
            metrics: list[dict[str, object]] = []
            viable_ids: list[str] = []
            for _, old_row in candidate_rows:
                overlap_fraction, overlap_area = _overlap_fraction(old_row.geometry, current_row.geometry)
                old_id = str(old_row["old_admin_id"])
                metrics.append(
                    {
                        "old_admin_id": old_id,
                        "overlap_fraction": overlap_fraction,
                        "overlap_area": overlap_area,
                    }
                )
                if overlap_fraction >= 0.95:
                    viable_ids.append(old_id)
            candidate_ids = [str(old_row["old_admin_id"]) for _, old_row in candidate_rows]
            selected = candidate_rows[0][1] if len(candidate_rows) == 1 else None
            status = "matched" if len(viable_ids) == 1 else "ambiguous" if viable_ids else "unresolved"
            selected_id = viable_ids[0] if status == "matched" else None
            selected_row = next(
                (old_row for _, old_row in candidate_rows if str(old_row["old_admin_id"]) == selected_id), None
            )
            display_name = (
                str(selected_row["old_admin_name"])
                if selected_row is not None
                else _without_admin_prefix(parsed.written_name)
            )
            records.append(
                {
                    "old_admin_id": selected_id if selected_id is not None else (
                        str(selected["old_admin_id"]) if selected is not None else None
                    ),
                    "old_admin_name": display_name,
                    "old_admin_type": parsed.admin_type,
                    "old_admin_normalized_name": parsed.normalized_name,
                    "current_commune_code": current_code if status == "matched" else None,
                    "current_commune_name": current_name if status == "matched" else None,
                    "relationship_type": (
                        "unchanged"
                        if status == "matched" and normalize_admin_name(current_name) == parsed.normalized_name
                        else "merged" if status == "matched" else None
                    ),
                    "match_status": status,
                    "valid_from": None,
                    "valid_to": selected_row["valid_to"] if selected_row is not None else None,
                    "candidate_old_admin_ids": json.dumps(candidate_ids, ensure_ascii=False),
                    "overlap_metrics_json": json.dumps(metrics, ensure_ascii=False, sort_keys=True),
                    "_viable_ids": viable_ids,
                }
            )
    target_codes_by_old_id: dict[str, set[str]] = {}
    for record in records:
        for old_id in record["_viable_ids"]:
            target_codes_by_old_id.setdefault(old_id, set()).add(str(record["current_commune_code"]))
    for record in records:
        if any(len(target_codes_by_old_id[old_id]) > 1 for old_id in record["_viable_ids"]):
            record["match_status"] = "ambiguous"
            record["current_commune_code"] = None
            record["current_commune_name"] = None
            record["relationship_type"] = None
    result = pd.DataFrame(records)
    if result.empty:
        return pd.DataFrame(
            columns=(
                "old_admin_id", "old_admin_name", "old_admin_type", "old_admin_normalized_name",
                "current_commune_code", "current_commune_name", "relationship_type", "match_status",
                "valid_from", "valid_to", "candidate_old_admin_ids", "overlap_metrics_json",
            )
        )
    result = result.drop(columns="_viable_ids")
    return result.sort_values(
        ["old_admin_name", "current_commune_code"], kind="stable", na_position="last"
    ).reset_index(drop=True)


PROCESSING_CRS = "EPSG:32648"


def _metric_layers(
    basins: gpd.GeoDataFrame, entities: gpd.GeoDataFrame, entity_id: str
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Validate one relationship input and put both layers in the metric overlay CRS."""
    if entities.crs is None:
        raise ValueError("entities must have a CRS")
    if entity_id not in entities.columns:
        raise ValueError(f"entities are missing required {entity_id} column")
    if entities[entity_id].isna().any() or entities[entity_id].duplicated().any():
        raise ValueError(f"entities have missing or duplicate {entity_id} values")
    return checked_basins(basins).to_crs(PROCESSING_CRS), entities.to_crs(PROCESSING_CRS)


def _provenance() -> dict[str, object]:
    return {"processing_crs": PROCESSING_CRS, "source_asset_ids_json": "[]"}


def _empty(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def map_subbasin_commune(basins: gpd.GeoDataFrame, communes: gpd.GeoDataFrame) -> pd.DataFrame:
    """Return unsimplified metric basin--current-commune area relationships."""
    metric_basins, metric_communes = _metric_layers(basins, communes, "current_commune_code")
    rows: list[dict[str, object]] = []
    for basin in metric_basins.itertuples(index=False):
        basin_area = float(basin.geometry.area)
        for commune in metric_communes.itertuples(index=False):
            intersection = basin.geometry.intersection(commune.geometry)
            area = float(intersection.area)
            if intersection.is_empty or area <= 0:
                continue
            commune_area = float(commune.geometry.area)
            rows.append(
                {
                    "HYBAS_ID": int(basin.HYBAS_ID),
                    "current_commune_code": str(commune.current_commune_code),
                    "intersection_area_km2": area / 1_000_000,
                    "basin_fraction": area / basin_area if basin_area else 0.0,
                    "commune_fraction": area / commune_area if commune_area else 0.0,
                    "quality_flags_json": "{}",
                    **_provenance(),
                }
            )
    columns = [
        "HYBAS_ID", "current_commune_code", "intersection_area_km2", "basin_fraction",
        "commune_fraction", "quality_flags_json", "processing_crs", "source_asset_ids_json",
    ]
    if not rows:
        return _empty(columns)
    return pd.DataFrame(rows, columns=columns).sort_values(
        ["HYBAS_ID", "current_commune_code"], kind="stable"
    ).reset_index(drop=True)


def map_subbasin_lines(
    basins: gpd.GeoDataFrame,
    lines: gpd.GeoDataFrame,
    entity_id: str,
    *,
    include_relationship_geometry: bool = False,
) -> pd.DataFrame:
    """Return metric line intersections, retaining source identifiers and boundary evidence."""
    metric_basins, metric_lines = _metric_layers(basins, lines, entity_id)
    extra_columns = [column for column in lines.columns if column != "geometry" and column != entity_id]
    rows: list[dict[str, object]] = []
    for basin in metric_basins.itertuples(index=False):
        for _, line in metric_lines.iterrows():
            intersection = basin.geometry.intersection(line.geometry)
            length = float(intersection.length)
            if intersection.is_empty or length <= 0:
                continue
            row = {
                "HYBAS_ID": int(basin.HYBAS_ID),
                entity_id: line[entity_id],
                "intersected_length_km": length / 1_000,
                "boundary_case": bool(line.geometry.touches(basin.geometry)),
                "quality_flags_json": "{}",
                **_provenance(),
            }
            if include_relationship_geometry:
                row["relationship_geometry_wkt"] = intersection.wkt
            row.update({column: line[column] for column in extra_columns})
            rows.append(row)
    columns = [
        "HYBAS_ID", entity_id, *extra_columns, "intersected_length_km", "boundary_case",
        "quality_flags_json", "processing_crs", "source_asset_ids_json",
    ]
    if include_relationship_geometry:
        columns.append("relationship_geometry_wkt")
    if not rows:
        return _empty(columns)
    return pd.DataFrame(rows, columns=columns).sort_values(
        ["HYBAS_ID", entity_id], kind="stable"
    ).reset_index(drop=True)


def map_subbasin_points(
    basins: gpd.GeoDataFrame, points: gpd.GeoDataFrame, entity_id: str
) -> pd.DataFrame:
    """Map points to all containing/touching basins, preserving exact boundary ties."""
    metric_basins, metric_points = _metric_layers(basins, points, entity_id)
    rows: list[dict[str, object]] = []
    tag_columns = [column for column in points.columns if column not in {"geometry", entity_id}]
    for _, point in metric_points.iterrows():
        matches = metric_basins.loc[metric_basins.geometry.covers(point.geometry)]
        tie = len(matches) > 1 and any(geometry.touches(point.geometry) for geometry in matches.geometry)
        for basin in matches.itertuples(index=False):
            touches = bool(basin.geometry.touches(point.geometry))
            relationship = "within" if point.geometry.within(basin.geometry) else "touches"
            if tie and touches:
                relationship = "nearest_boundary_tie"
            tags = {column: point[column] for column in tag_columns if pd.notna(point[column])}
            rows.append(
                {
                    "HYBAS_ID": int(basin.HYBAS_ID),
                    entity_id: point[entity_id],
                    "relationship_type": relationship,
                    "tags_json": json.dumps(tags, ensure_ascii=False, sort_keys=True, default=str),
                    "boundary_case": touches,
                    "quality_flags_json": json.dumps(
                        {"boundary_tie": True} if tie and touches else {}, sort_keys=True
                    ),
                    **_provenance(),
                }
            )
    columns = [
        "HYBAS_ID", entity_id, "relationship_type", "tags_json", "boundary_case",
        "quality_flags_json", "processing_crs", "source_asset_ids_json",
    ]
    if not rows:
        return _empty(columns)
    return pd.DataFrame(rows, columns=columns).sort_values(
        ["HYBAS_ID", entity_id], kind="stable"
    ).reset_index(drop=True)
