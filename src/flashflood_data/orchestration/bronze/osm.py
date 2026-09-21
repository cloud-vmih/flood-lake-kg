"""Bounded OSM PBF parsing for flood-relevant features and critical facilities."""

import json
import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Lock

import pyogrio
import yaml
from pyogrio import _ogr
from shapely import from_wkb

_LAYERS = ("points", "lines", "multipolygons", "multilinestrings", "other_relations")
_NEGATIVE_VALUES = frozenset({"", "0", "false", "no"})
_TAG_NAME = re.compile(r"^[a-z][a-z0-9_:]*$")
_GDAL_CONFIG_LOCK = Lock()
_CLOSED_WAY_POLYGON_KEYS = (
    "aeroway,amenity,boundary,building,craft,geological,historic,landuse,leisure,"
    "military,natural,office,place,shop,sport,tourism,water,wetland,"
    "highway=platform,public_transport=platform"
)


@dataclass(frozen=True)
class OsmSelection:
    """Versioned tag rules; a feature is selected when any group rule matches."""

    version: str
    groups: Mapping[str, Mapping[str, tuple[str, ...]]]

    def __post_init__(self) -> None:
        if not self.version or not self.groups:
            raise ValueError("OSM selection requires a version and at least one group")
        for group, rules in self.groups.items():
            if not group or not rules:
                raise ValueError("OSM selection group cannot be empty")
            for key, values in rules.items():
                if not _TAG_NAME.fullmatch(key) or not values or any(not value for value in values):
                    raise ValueError(f"invalid OSM tag rule in group {group}")

    def matching_groups(self, tags: Mapping[str, str]) -> tuple[str, ...]:
        """Return deterministic group names whose tag rules match the feature."""
        matched = []
        for group, rules in self.groups.items():
            if any(_tag_matches(tags.get(key), values) for key, values in rules.items()):
                matched.append(group)
        return tuple(matched)

    def merged_rules(self) -> dict[str, tuple[str, ...]]:
        """Merge repeated keys across groups for one GDAL attribute filter."""
        merged: dict[str, set[str]] = {}
        for rules in self.groups.values():
            for key, values in rules.items():
                merged.setdefault(key, set()).update(values)
        return {
            key: ("*",) if "*" in values else tuple(sorted(values))
            for key, values in sorted(merged.items())
        }


def _tag_matches(value: str | None, accepted: tuple[str, ...]) -> bool:
    if value is None:
        return False
    parts = {part.strip() for part in str(value).split(";") if part.strip()}
    if "*" in accepted:
        return any(part.lower() not in _NEGATIVE_VALUES for part in parts)
    return bool(parts.intersection(accepted))


def load_osm_selection(path: Path) -> OsmSelection:
    """Load and validate the reviewable flood-relevant OSM tag policy."""
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict) or not isinstance(document.get("groups"), dict):
        raise TypeError("invalid OSM selection document")
    groups: dict[str, dict[str, tuple[str, ...]]] = {}
    for group, rules in document["groups"].items():
        if not isinstance(rules, dict):
            raise TypeError("OSM selection group must contain tag rules")
        if any(not isinstance(values, list) for values in rules.values()):
            raise TypeError("OSM tag rule values must be lists")
        groups[str(group)] = {
            str(key): tuple(str(value) for value in values)
            for key, values in rules.items()
        }
    return OsmSelection(version=str(document.get("version", "")), groups=groups)


def _sql_filter(selection: OsmSelection) -> str:
    clauses = []
    for key, values in selection.merged_rules().items():
        identifier = f'"{key}"'
        if values == ("*",):
            clauses.append(f"({identifier} IS NOT NULL AND {identifier} NOT IN ('no','false','0',''))")
        else:
            encoded = ",".join("'" + value.replace("'", "''") + "'" for value in values)
            clauses.append(f"{identifier} IN ({encoded})")
    return " OR ".join(clauses)


def _ogr_config(selection: OsmSelection) -> str:
    attributes = ",".join(selection.merged_rules())
    sections = []
    for layer in _LAYERS:
        sections.append(
            f"""[{layer}]
osm_id=yes
osm_version=no
osm_timestamp=no
osm_uid=no
osm_user=no
osm_changeset=no
attributes={attributes}
ignore=
all_tags=yes
"""
        )
    return f"""[general]
closed_ways_are_polygons={_CLOSED_WAY_POLYGON_KEYS}
attribute_name_laundering=no
report_all_tags=yes
tags_format=json

{"".join(sections)}"""


def _identity(layer: str, record: Mapping[str, object]) -> tuple[str, str]:
    if layer == "points":
        return "node", str(record["osm_id"])
    if layer == "lines":
        return "way", str(record["osm_id"])
    if layer == "multipolygons" and record.get("osm_way_id") is not None:
        return "way", str(record["osm_way_id"])
    return "relation", str(record["osm_id"])


def _bronze_row(
    layer: str,
    record: Mapping[str, object],
    *,
    geometry_name: str,
    crs: str,
    object_id: str,
    run_id: str,
    parser_version: str,
) -> dict[str, object]:
    tags = json.loads(str(record["all_tags"]))
    osm_type, osm_id = _identity(layer, record)
    raw_geometry = record.get(geometry_name)
    geometry_wkb = bytes(raw_geometry) if raw_geometry is not None else None
    bbox = None
    valid = False
    if geometry_wkb:
        geometry = from_wkb(geometry_wkb)
        if not geometry.is_empty:
            bbox = list(map(float, geometry.bounds))
            valid = bool(geometry.is_valid)
    return {
        "object_id": object_id,
        "osm_type": osm_type,
        "osm_id": osm_id,
        "tags_json": json.dumps(
            tags, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ),
        "geometry_wkb": geometry_wkb,
        "crs": crs if geometry_wkb else None,
        "bbox_wgs84": bbox,
        "ingest_run_id": run_id,
        "parser_version": parser_version,
        "quality_status": "passed" if valid else "flagged",
    }


def iter_osm_batches(
    path: Path,
    *,
    object_id: str,
    run_id: str,
    parser_version: str,
    selection: OsmSelection,
    batch_size: int = 5_000,
) -> Iterator[list[dict[str, object]]]:
    """Stream selected OSM features without loading the country extract into memory."""
    if batch_size < 1:
        raise ValueError("OSM batch_size must be positive")
    if not path.name.lower().endswith(".osm.pbf"):
        raise ValueError("OSM Bronze parser requires an .osm.pbf object")
    sql_filter = _sql_filter(selection)
    output: list[dict[str, object]] = []
    with TemporaryDirectory(prefix="flashflood-osm-", dir=path.parent) as temporary:
        config_path = Path(temporary) / "osmconf.ini"
        config_path.write_text(_ogr_config(selection), encoding="utf-8")
        with _GDAL_CONFIG_LOCK:
            previous_config = _ogr.get_gdal_config_option("OSM_CONFIG_FILE")
            previous_tmp = _ogr.get_gdal_config_option("CPL_TMPDIR")
            pyogrio.set_gdal_config_options(
                {"OSM_CONFIG_FILE": str(config_path), "CPL_TMPDIR": temporary}
            )
            try:
                available = {str(name) for name, _geometry in pyogrio.list_layers(path)}
                for layer in _LAYERS:
                    if layer not in available:
                        continue
                    with pyogrio.open_arrow(
                        path,
                        layer=layer,
                        where=sql_filter,
                        batch_size=batch_size,
                        use_pyarrow=True,
                    ) as (metadata, reader):
                        geometry_name = str(metadata["geometry_name"] or "wkb_geometry")
                        crs = str(metadata["crs"] or "EPSG:4326")
                        for arrow_batch in reader:
                            for record in arrow_batch.to_pylist():
                                tags = json.loads(str(record["all_tags"]))
                                if not selection.matching_groups(tags):
                                    continue
                                output.append(
                                    _bronze_row(
                                        layer,
                                        record,
                                        geometry_name=geometry_name,
                                        crs=crs,
                                        object_id=object_id,
                                        run_id=run_id,
                                        parser_version=parser_version,
                                    )
                                )
                                if len(output) == batch_size:
                                    yield output
                                    output = []
            finally:
                pyogrio.set_gdal_config_options(
                    {"OSM_CONFIG_FILE": previous_config, "CPL_TMPDIR": previous_tmp}
                )
    if output:
        yield output
