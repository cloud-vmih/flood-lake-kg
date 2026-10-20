from __future__ import annotations


def _basins():
    return (
        {
            "basin_id": "A",
            "basin_version": "bv1",
            "next_down_id": "B",
            "topology_sort_hint": 3,
            "hydrobasins_object_id": "hydro-1",
            "is_coastal": False,
            "is_endorheic": False,
        },
        {
            "basin_id": "B",
            "basin_version": "bv1",
            "next_down_id": None,
            "topology_sort_hint": 2,
            "hydrobasins_object_id": "hydro-1",
            "is_coastal": True,
            "is_endorheic": False,
        },
        {
            "basin_id": "C",
            "basin_version": "bv1",
            "next_down_id": "OUTSIDE",
            "topology_sort_hint": 1,
            "hydrobasins_object_id": "hydro-1",
            "is_coastal": False,
            "is_endorheic": False,
        },
    )


def test_analyze_topology_classifies_internal_exit_and_terminal() -> None:
    from flashflood_data.pipelines.silver_basin.topology import analyze_basin_topology

    report = analyze_basin_topology(_basins())

    assert report.basin_count == 3
    assert report.edge_count == 3
    assert report.internal_edge_count == 1
    assert report.exit_edge_count == 1
    assert report.terminal_edge_count == 1
    assert report.topological_order == ("A", "B", "C")
    assert report.cycle_nodes == ()
    assert report.duplicate_basin_ids == ()
    assert report.missing_internal_targets == ()


def test_analyze_topology_reports_cycle_nodes_deterministically() -> None:
    from flashflood_data.pipelines.silver_basin.topology import analyze_basin_topology

    rows = (
        {"basin_id": "B", "next_down_id": "A"},
        {"basin_id": "A", "next_down_id": "B"},
        {"basin_id": "C", "next_down_id": "A"},
    )

    report = analyze_basin_topology(rows)

    assert report.topological_order == ("C",)
    assert report.cycle_nodes == ("A", "B")


def test_analyze_topology_reports_duplicate_basin_ids() -> None:
    from flashflood_data.pipelines.silver_basin.topology import analyze_basin_topology

    rows = (
        {"basin_id": "A", "next_down_id": None},
        {"basin_id": "A", "next_down_id": None},
    )

    report = analyze_basin_topology(rows)

    assert report.basin_count == 2
    assert report.edge_count == 2
    assert report.duplicate_basin_ids == ("A",)


def test_terminal_type_only_uses_coast_and_endo_flags_for_terminal_edges() -> None:
    from flashflood_data.pipelines.silver_basin.topology import basin_topology_plan

    plan = basin_topology_plan(topology_version="tv1")
    expression = next(item for item in plan.edge_projection if item.endswith("AS terminal_type"))

    assert expression.startswith("CASE WHEN u.next_down_id IS NULL")
    assert "ELSE 'non_terminal' END AS terminal_type" in expression


def test_large_topology_order_does_not_expand_edge_sql_per_basin() -> None:
    from flashflood_data.pipelines.silver_basin.topology import build_basin_topology

    calls = []
    rows = tuple({"basin_id": f"B{index}", "next_down_id": None} for index in range(10_000))
    build_basin_topology(_Frame(rows, calls), topology_version="tv1")
    projection = next(call[2] for call in calls if call[1] == "selectExpr" and call[0] == "joined")
    expression = next(
        item for item in projection if item.endswith("AS topological_rank_upstream_first")
    )

    assert expression == ("r.topological_rank_upstream_first AS topological_rank_upstream_first")
    assert sum(len(item) for item in projection) < 5_000


def test_cycle_diagnostics_do_not_expand_edge_sql_per_cycle_node() -> None:
    from flashflood_data.pipelines.silver_basin.topology import basin_topology_plan

    plan = basin_topology_plan(
        topology_version="tv1",
        cycle_nodes=tuple(f"B{index}" for index in range(10_000)),
    )
    expression = next(item for item in plan.edge_projection if item.endswith("AS cycle_check"))

    assert expression == "'failed' AS cycle_check"


class _Column:
    def __init__(self, value):
        self.value = value

    def __eq__(self, other):
        return f"{self.value} = {other.value}"


class _CollectedFrame:
    def __init__(self, rows, calls):
        self.rows = rows
        self.calls = calls

    def collect(self):
        self.calls.append(("compact", "collect"))
        return self.rows

    def toLocalIterator(self):
        self.calls.append(("compact", "toLocalIterator"))
        return iter(self.rows)


class _Frame:
    def __init__(self, rows, calls, label="basins"):
        self.rows = rows
        self.calls = calls
        self.label = label

    def select(self, *columns):
        self.calls.append((self.label, "select", columns))
        return _CollectedFrame(self.rows, self.calls)

    @property
    def sparkSession(self):
        return _Spark(self.calls)

    def selectExpr(self, *expressions):
        self.calls.append((self.label, "selectExpr", expressions))
        label = "edges" if self.label == "joined" else self.label
        return _Frame(self.rows, self.calls, label)

    def dropDuplicates(self, columns):
        self.calls.append((self.label, "dropDuplicates", tuple(columns)))
        return self

    def alias(self, name):
        self.calls.append((self.label, "alias", name))
        return _Frame(self.rows, self.calls, name)

    def __getitem__(self, name):
        return _Column(f"{self.label}.{name}")

    def join(self, other, condition, mode):
        self.calls.append((self.label, "join", other.label, condition, mode))
        return _Frame(self.rows, self.calls, "joined")


class _Spark:
    def __init__(self, calls):
        self.calls = calls

    def createDataFrame(self, rows, schema):
        self.calls.append(("rank", "createDataFrame", tuple(rows), schema))
        return _Frame((), self.calls, "rank")


def test_build_basin_topology_creates_lazy_left_joined_edge_frame() -> None:
    from flashflood_data.pipelines.silver_basin.topology import build_basin_topology

    calls = []
    edges, report = build_basin_topology(
        _Frame(_basins(), calls),
        topology_version="tv1",
    )

    joins = [call for call in calls if call[1] == "join"]
    assert len(joins) == 2
    join = joins[0]
    edge_projection = next(
        call[2] for call in calls if call[1] == "selectExpr" and call[0] == "joined"
    )
    assert join[-1] == "left"
    assert join[-2] == "u.next_down_id = d.candidate_downstream_id"
    assert joins[1][-2] == "u.basin_id = r.basin_id"
    assert joins[1][-1] == "left"
    rank_rows = next(call[2] for call in calls if call[1] == "createDataFrame")
    assert rank_rows == (("A", 0), ("B", 1), ("C", 2))
    compact = next(call[2] for call in calls if call[1] == "select")
    assert compact == ("basin_id", "next_down_id")
    assert any("AS edge_status" in expression for expression in edge_projection)
    assert any("AS basin_edge_id" in expression for expression in edge_projection)
    assert any("AS source_downstream_id" in expression for expression in edge_projection)
    assert any("AS topological_rank_upstream_first" in expression for expression in edge_projection)
    assert any("AS terminal_type" in expression for expression in edge_projection)
    assert any("AS quality_status" in expression for expression in edge_projection)
    assert any("AS _sort_hint_consistent" in expression for expression in edge_projection)
    assert any("AS cycle_check" in expression for expression in edge_projection)
    assert not any("river_length_km" in expression for expression in edge_projection)
    assert not any("elevation_drop_m" in expression for expression in edge_projection)
    assert not any("channel_slope_m_m" in expression for expression in edge_projection)
    assert edges.label == "edges"
    assert report.edge_count == 3
    assert sum(call[1] == "toLocalIterator" for call in calls) == 1
    assert not any(call[1] == "collect" for call in calls)
