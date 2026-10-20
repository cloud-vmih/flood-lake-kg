"""Spark edge construction and compact graph diagnostics for Silver basins.

The edge DataFrame remains a lazy Spark plan. Only basin and downstream IDs
are collected to the driver for Kahn's algorithm; ranks return to Spark as a
small lookup DataFrame rather than a per-basin SQL CASE expression.
"""

from __future__ import annotations

import heapq
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from flashflood_data.pipelines.silver_basin.models import TopologyReport

_GRAPH_COLUMNS = ("basin_id", "next_down_id")
_UPSTREAM_COLUMNS = (
    *_GRAPH_COLUMNS,
    "basin_version",
    "topology_sort_hint",
    "hydrobasins_object_id",
    "is_coastal",
    "is_endorheic",
)


def _value(row: object, name: str) -> object:
    if isinstance(row, Mapping):
        return row.get(name)
    try:
        return row[name]  # type: ignore[index]
    except (KeyError, TypeError):
        return getattr(row, name)


def _required_identifier(value: object, name: str) -> str:
    normalized = "" if value is None else str(value).strip()
    if not normalized:
        raise ValueError(f"basin topology requires non-empty {name}")
    return normalized


def _optional_downstream(value: object, basin_id: str) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized or normalized == "0" or normalized == basin_id:
        return None
    return normalized


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


@dataclass(frozen=True)
class BasinTopologyPlan:
    """Validated SQL projections for the lazy basin-edge DataFrame."""

    compact_columns: tuple[str, ...]
    upstream_projection: tuple[str, ...]
    downstream_projection: tuple[str, ...]
    edge_projection: tuple[str, ...]
    join_mode: str = "left"


def analyze_basin_topology(rows: Iterable[object]) -> TopologyReport:
    """Classify edges and run a deterministic Kahn sort on compact graph rows."""
    normalized: list[tuple[str, str | None]] = []
    for row in rows:
        basin_id = _required_identifier(_value(row, "basin_id"), "basin_id")
        normalized.append(
            (
                basin_id,
                _optional_downstream(_value(row, "next_down_id"), basin_id),
            )
        )

    counts = Counter(basin_id for basin_id, _ in normalized)
    duplicate_ids = tuple(sorted(key for key, count in counts.items() if count > 1))
    basin_ids = set(counts)

    internal_count = 0
    exit_count = 0
    terminal_count = 0
    for _basin_id, downstream_id in normalized:
        if downstream_id is None:
            terminal_count += 1
        elif downstream_id in basin_ids:
            internal_count += 1
        else:
            exit_count += 1

    # Duplicate basin IDs are already a fatal DQ finding. Pick one deterministic
    # relation per ID so diagnostics remain reproducible until DQ blocks publish.
    relation_by_id: dict[str, str | None] = {}
    for basin_id, downstream_id in sorted(
        normalized,
        key=lambda item: (item[0], item[1] or ""),
    ):
        relation_by_id.setdefault(basin_id, downstream_id)

    indegree = {basin_id: 0 for basin_id in basin_ids}
    internal_relation: dict[str, str] = {}
    for basin_id, downstream_id in relation_by_id.items():
        if downstream_id is None or downstream_id not in basin_ids:
            continue
        internal_relation[basin_id] = downstream_id
        indegree[downstream_id] += 1

    ready = [basin_id for basin_id, degree in indegree.items() if degree == 0]
    heapq.heapify(ready)
    ordered: list[str] = []
    while ready:
        basin_id = heapq.heappop(ready)
        ordered.append(basin_id)
        downstream_id = internal_relation.get(basin_id)
        if downstream_id is None:
            continue
        indegree[downstream_id] -= 1
        if indegree[downstream_id] == 0:
            heapq.heappush(ready, downstream_id)

    ordered_set = set(ordered)
    cycle_nodes = tuple(sorted(basin_ids - ordered_set))
    return TopologyReport(
        basin_count=len(normalized),
        edge_count=len(normalized),
        internal_edge_count=internal_count,
        exit_edge_count=exit_count,
        terminal_edge_count=terminal_count,
        topological_order=tuple(ordered),
        cycle_nodes=cycle_nodes,
        duplicate_basin_ids=duplicate_ids,
        missing_internal_targets=(),
    )


def basin_topology_plan(
    *,
    topology_version: str,
    cycle_nodes: Sequence[str] = (),
) -> BasinTopologyPlan:
    """Compile deterministic Spark SQL expressions for basin-edge construction."""
    version = topology_version.strip()
    if not version:
        raise ValueError("topology_version must not be empty")
    cycles = tuple(sorted(set(cycle_nodes)))
    if any(not str(node).strip() for node in cycles):
        raise ValueError("cycle node identifiers must not be empty")
    terminal = (
        "u.next_down_id IS NULL OR trim(u.next_down_id) IN ('', '0') OR u.next_down_id = u.basin_id"
    )
    internal = f"NOT ({terminal}) AND d.candidate_downstream_id IS NOT NULL"
    # A cycle is a fatal global DQ finding; no per-ID SQL list is necessary.
    cycle_check = "'failed'" if cycles else "'passed'"
    terminal_type = (
        f"CASE WHEN {terminal} THEN CASE "
        "WHEN u.next_down_id = u.basin_id THEN 'self_reference' "
        "WHEN u.is_endorheic = TRUE THEN 'endorheic' "
        "WHEN u.is_coastal = TRUE THEN 'coastal' "
        "ELSE 'sink' END ELSE 'non_terminal' END"
    )

    return BasinTopologyPlan(
        compact_columns=_GRAPH_COLUMNS,
        upstream_projection=_UPSTREAM_COLUMNS,
        downstream_projection=(
            "basin_id AS candidate_downstream_id",
            "topology_sort_hint AS candidate_sort_hint",
        ),
        edge_projection=(
            (
                "sha2(concat_ws('\\u001f', "
                f"{_literal(version)}, u.basin_version, u.basin_id), 256) "
                "AS basin_edge_id"
            ),
            f"{_literal(version)} AS topology_version",
            "u.basin_version AS basin_version",
            "u.basin_id AS upstream_basin_id",
            (
                f"CASE WHEN {internal} THEN u.next_down_id "
                "ELSE CAST(NULL AS STRING) END AS downstream_basin_id"
            ),
            "u.next_down_id AS source_downstream_id",
            (
                f"CASE WHEN {terminal} THEN 'terminal' "
                "WHEN d.candidate_downstream_id IS NOT NULL THEN 'internal' "
                "ELSE 'exits_AOI' END AS edge_status"
            ),
            "r.topological_rank_upstream_first AS topological_rank_upstream_first",
            f"{terminal_type} AS terminal_type",
            (
                f"CASE WHEN {internal} THEN "
                "u.topology_sort_hint IS NOT NULL AND d.candidate_sort_hint IS NOT NULL "
                "AND u.topology_sort_hint > d.candidate_sort_hint "
                "ELSE TRUE END AS _sort_hint_consistent"
            ),
            f"{cycle_check} AS cycle_check",
            (
                f"CASE WHEN {terminal} THEN 'terminal' "
                "WHEN d.candidate_downstream_id IS NULL THEN 'outside_aoi' "
                "ELSE 'passed' END AS outlet_check"
            ),
            "'pending' AS quality_status",
            "u.hydrobasins_object_id AS source_object_id",
        ),
    )


def build_basin_topology(
    basin_frame: Any,
    *,
    topology_version: str,
) -> tuple[Any, TopologyReport]:
    """Return a lazy edge DataFrame and compact deterministic graph diagnostics."""
    compact_rows = basin_frame.select(*_GRAPH_COLUMNS).toLocalIterator()
    report = analyze_basin_topology(compact_rows)
    plan = basin_topology_plan(
        topology_version=topology_version,
        cycle_nodes=report.cycle_nodes,
    )

    upstream = basin_frame.selectExpr(*plan.upstream_projection).alias("u")
    downstream = (
        basin_frame.selectExpr(*plan.downstream_projection)
        .dropDuplicates(["candidate_downstream_id"])
        .alias("d")
    )
    ranks = basin_frame.sparkSession.createDataFrame(
        ((basin_id, rank) for rank, basin_id in enumerate(report.topological_order)),
        schema="basin_id STRING, topological_rank_upstream_first BIGINT",
    ).alias("r")
    joined = upstream.join(
        downstream,
        upstream["next_down_id"] == downstream["candidate_downstream_id"],
        plan.join_mode,
    ).join(
        ranks,
        upstream["basin_id"] == ranks["basin_id"],
        "left",
    )
    return joined.selectExpr(*plan.edge_projection), report
