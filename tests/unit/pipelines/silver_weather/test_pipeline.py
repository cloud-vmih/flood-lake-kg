"""Weather Silver build planning and idempotent row selection."""

import pytest

from flashflood_data.pipelines.silver_weather.pipeline import (
    WEATHER_OUTPUTS,
    WeatherBuildPlan,
    missing_weight_rows,
    plan_weather_build,
)
from flashflood_data.shared.spark.models import BuildSignature, PinnedSnapshot
from flashflood_data.shared.spark.snapshots import BuildRecord

SNAPSHOTS = (
    PinnedSnapshot("bronze.weather_raster_slice", 11),
    PinnedSnapshot("silver.source_grid", 12),
    PinnedSnapshot("silver.dim_basin", 13),
)


def test_same_input_snapshots_and_complete_outputs_skip() -> None:
    first = plan_weather_build(SNAPSHOTS, config_hash="a" * 64, history=())
    previous = BuildRecord(
        pipeline_run_id=first.pipeline_run_id,
        status="succeeded",
        build_signature=first.build_signature,
        completed_outputs=WEATHER_OUTPUTS,
    )
    again = plan_weather_build(SNAPSHOTS, config_hash="a" * 64, history=(previous,))

    assert again.action == "skip"
    assert again.missing_outputs == ()
    assert WeatherBuildPlan.from_document(first.to_document()) == first


def test_changed_bronze_snapshot_requires_new_build() -> None:
    first = plan_weather_build(SNAPSHOTS, config_hash="a" * 64, history=())
    changed = (PinnedSnapshot("bronze.weather_raster_slice", 14), *SNAPSHOTS[1:])
    second = plan_weather_build(changed, config_hash="a" * 64, history=())
    assert first.build_signature != second.build_signature
    assert second.missing_outputs == WEATHER_OUTPUTS


@pytest.mark.parametrize("old_version", ["v1", "v2"])
def test_old_manifests_do_not_skip_current_weather_policy(old_version) -> None:
    old_signature = BuildSignature.create(
        snapshots=SNAPSHOTS, config_hash="a" * 64, policy_id="silver-weather-v1",
        algorithm_version=f"weather-normalize-aggregate-{old_version}", contract_version="1.0",
    ).value
    old = BuildRecord("old-run", "succeeded", old_signature, WEATHER_OUTPUTS)

    current = plan_weather_build(SNAPSHOTS, config_hash="a" * 64, history=(old,))

    assert current.action == "build"
    assert current.build_signature != old_signature


def test_failed_prior_run_cannot_make_same_build_skip() -> None:
    first = plan_weather_build(SNAPSHOTS, config_hash="a" * 64, history=())
    failed = BuildRecord(
        pipeline_run_id=first.pipeline_run_id,
        status="failed",
        build_signature=first.build_signature,
        completed_outputs=WEATHER_OUTPUTS,
    )
    again = plan_weather_build(SNAPSHOTS, config_hash="a" * 64, history=(failed,))
    assert again.action == "build"
    assert again.missing_outputs == WEATHER_OUTPUTS


def test_missing_weights_uses_full_versioned_key_for_anti_join() -> None:
    class Frame:
        def __init__(self) -> None:
            self.selected = ()
            self.join_keys = ()
            self.join_mode = ""

        def select(self, *names):
            self.selected = names
            return self

        def distinct(self):
            return self

        def join(self, other, on, how):
            self.join_keys = tuple(on)
            self.join_mode = how
            return self

    candidate, existing = Frame(), Frame()
    missing_weight_rows(candidate, existing)
    assert "geometry_processing_version" in existing.selected
    assert candidate.join_keys == existing.selected
    assert candidate.join_mode == "left_anti"
