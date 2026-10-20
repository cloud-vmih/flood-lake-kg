from datetime import UTC, datetime

from flashflood_data.shared.spark.models import PinnedSnapshot
from flashflood_data.shared.spark.snapshots import BuildRecord


def _source(object_id: str, source_id: str, hour: int) -> dict[str, object]:
    return {
        "object_id": object_id,
        "source_id": source_id,
        "status": "available",
        "retrieved_at": datetime(2026, 1, 1, hour, tzinfo=UTC),
    }


def test_select_current_objects_uses_latest_revision_present_in_bronze() -> None:
    from flashflood_data.pipelines.silver_basin.planner import select_current_objects

    selected = select_current_objects(
        bronze_objects=(
            {"object_id": "hydro-old", "source_id": "hydro"},
            {"object_id": "atlas-old", "source_id": "atlas"},
        ),
        source_objects=(
            _source("hydro-old", "hydro", 1),
            _source("hydro-new-not-parsed", "hydro", 2),
            _source("atlas-old", "atlas", 1),
        ),
        hydro_source_id="hydro",
        atlas_source_id="atlas",
    )

    assert selected.hydro_object_id == "hydro-old"
    assert selected.atlas_object_id == "atlas-old"


def test_build_plan_skips_a_completed_identical_signature() -> None:
    from flashflood_data.pipelines.silver_basin.planner import build_basin_execution_plan

    kwargs = {
        "snapshot": PinnedSnapshot("bronze.basin_polygon_raw", 17),
        "base_config_hash": "config-hash",
        "policy_id": "policy-id",
        "algorithm_version": "algorithm-v1",
        "contract_version": "1",
        "hydro_object_id": "hydro-object",
        "atlas_object_id": "atlas-object",
        "basin_ids": ("101", "102"),
    }
    first = build_basin_execution_plan(history=(), **kwargs)
    completed = BuildRecord(
        pipeline_run_id=first.pipeline_run_id,
        status="succeeded",
        build_signature=first.build_signature,
        completed_outputs=("silver.dim_basin", "silver.basin_edge"),
    )
    repeated = build_basin_execution_plan(history=(completed,), **kwargs)

    assert first.action == "build"
    assert repeated.action == "skip"
    assert repeated.missing_outputs == ()
    assert repeated.pipeline_run_id == first.pipeline_run_id


def test_execution_plan_document_round_trip_preserves_repair_scope() -> None:
    from flashflood_data.pipelines.silver_basin.planner import (
        BasinExecutionPlan,
        build_basin_execution_plan,
    )

    first = build_basin_execution_plan(
        snapshot=PinnedSnapshot("bronze.basin_polygon_raw", 17),
        base_config_hash="config-hash",
        policy_id="policy-id",
        algorithm_version="algorithm-v1",
        contract_version="1",
        hydro_object_id="hydro-object",
        atlas_object_id="atlas-object",
        basin_ids=("101", "102"),
        history=(),
    )
    changed_scope = build_basin_execution_plan(
        snapshot=first.snapshot,
        base_config_hash="config-hash",
        policy_id="policy-id",
        algorithm_version="algorithm-v1",
        contract_version="1",
        hydro_object_id=first.hydro_object_id,
        atlas_object_id=first.atlas_object_id,
        basin_ids=("101",),
        history=(),
    )
    assert changed_scope.build_signature != first.build_signature
    repair = build_basin_execution_plan(
        snapshot=first.snapshot,
        base_config_hash="config-hash",
        policy_id="policy-id",
        algorithm_version="algorithm-v1",
        contract_version="1",
        hydro_object_id=first.hydro_object_id,
        atlas_object_id=first.atlas_object_id,
        basin_ids=first.basin_ids,
        history=(
            BuildRecord(
                first.pipeline_run_id,
                "failed",
                first.build_signature,
                ("silver.dim_basin",),
            ),
        ),
    )

    restored = BasinExecutionPlan.from_document(repair.to_document())
    assert restored == repair
    assert restored.action == "repair"
    assert restored.missing_outputs == ("silver.basin_edge",)
