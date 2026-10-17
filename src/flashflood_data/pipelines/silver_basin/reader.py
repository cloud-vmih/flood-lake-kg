"""Pinned-snapshot reader for Bronze basin polygons used by Silver topology."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from pyiceberg.catalog import Catalog
from pyiceberg.expressions import EqualTo, Or

from flashflood_data.shared.spark.models import PinnedSnapshot

_TABLE_IDENTIFIER = ("bronze", "basin_polygon_raw")
_SELECTED_FIELDS = (
    "object_id",
    "source_feature_id",
    "source_id",
    "source_fields_json",
    "geometry_wkb",
    "crs",
    "quality_status",
)


@dataclass(frozen=True)
class BasinBronzeRow:
    """One source-faithful basin feature decoded from its Bronze JSON fields."""

    object_id: str
    source_feature_id: str
    source_id: str
    hybas_id: str
    source_fields: Mapping[str, object]
    geometry_wkb: bytes
    crs: str
    quality_status: str


@dataclass(frozen=True)
class BasinBronzeInputs:
    """HydroBASINS and BasinATLAS rows read from the same immutable snapshot."""

    hydro_rows: tuple[BasinBronzeRow, ...]
    atlas_rows: tuple[BasinBronzeRow, ...]
    snapshot_ref: PinnedSnapshot


def _required_text(row: Mapping[str, Any], field: str) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Bronze basin row requires non-empty {field}")
    return value.strip()


def _identifier(value: object, field: str) -> str:
    if isinstance(value, bool) or value is None:
        raise ValueError(f"Bronze basin source fields require {field}")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            raise ValueError(f"Bronze basin {field} must be an integer-like identifier")
        return str(int(value))
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise ValueError(f"Bronze basin source fields require {field}")


def _source_fields(row: Mapping[str, Any]) -> dict[str, object]:
    payload = row.get("source_fields_json")
    if not isinstance(payload, str):
        raise TypeError("Bronze basin source_fields_json must be a JSON string")
    try:
        fields = json.loads(payload)
    except json.JSONDecodeError as error:
        raise ValueError("Bronze basin source_fields_json is invalid JSON") from error
    if not isinstance(fields, dict):
        raise TypeError("Bronze basin source_fields_json must decode to an object")
    _identifier(fields.get("HYBAS_ID"), "HYBAS_ID")
    return {str(name): value for name, value in fields.items()}


def _basin_row(row: Mapping[str, Any]) -> BasinBronzeRow:
    fields = _source_fields(row)
    geometry = row.get("geometry_wkb")
    if not isinstance(geometry, (bytes, bytearray, memoryview)) or not geometry:
        raise ValueError("Bronze basin row requires non-empty geometry_wkb")
    return BasinBronzeRow(
        object_id=_required_text(row, "object_id"),
        source_feature_id=_required_text(row, "source_feature_id"),
        source_id=_required_text(row, "source_id"),
        hybas_id=_identifier(fields["HYBAS_ID"], "HYBAS_ID"),
        source_fields=fields,
        geometry_wkb=bytes(geometry),
        crs=_required_text(row, "crs"),
        quality_status=_required_text(row, "quality_status"),
    )


class BasinBronzeReader:
    """Read only the configured basin sources from one pinned Bronze snapshot."""

    def __init__(
        self,
        catalog: Catalog,
        table_identifier: tuple[str, str] = _TABLE_IDENTIFIER,
    ) -> None:
        if table_identifier != _TABLE_IDENTIFIER:
            raise ValueError("Silver basin reader requires bronze.basin_polygon_raw")
        self.catalog = catalog
        self.table_identifier = table_identifier

    def read(
        self,
        snapshot_id: int,
        hydro_source_id: str,
        atlas_source_id: str,
    ) -> BasinBronzeInputs:
        """Decode both sources from exactly ``snapshot_id`` without reading latest."""
        snapshot_ref = PinnedSnapshot(".".join(self.table_identifier), snapshot_id)
        hydro_id = hydro_source_id.strip()
        atlas_id = atlas_source_id.strip()
        if not hydro_id or not atlas_id or hydro_id == atlas_id:
            raise ValueError("HydroBASINS and BasinATLAS source IDs must be non-empty and distinct")

        table = self.catalog.load_table(self.table_identifier)
        table.refresh()
        rows = table.scan(
            row_filter=Or(EqualTo("source_id", hydro_id), EqualTo("source_id", atlas_id)),
            selected_fields=_SELECTED_FIELDS,
            snapshot_id=snapshot_id,
        ).to_arrow().to_pylist()

        selected: dict[str, list[BasinBronzeRow]] = {hydro_id: [], atlas_id: []}
        for raw in rows:
            source_id = _required_text(raw, "source_id")
            if source_id not in selected:
                continue
            selected[source_id].append(_basin_row(raw))

        order = lambda row: (row.hybas_id, row.object_id, row.source_feature_id)
        return BasinBronzeInputs(
            hydro_rows=tuple(sorted(selected[hydro_id], key=order)),
            atlas_rows=tuple(sorted(selected[atlas_id], key=order)),
            snapshot_ref=snapshot_ref,
        )
