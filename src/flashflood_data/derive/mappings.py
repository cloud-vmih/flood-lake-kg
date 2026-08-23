"""Conservative spatial crosswalks between historical and current administration."""

from __future__ import annotations

import json
import unicodedata
from dataclasses import dataclass

import geopandas as gpd
import pandas as pd

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
