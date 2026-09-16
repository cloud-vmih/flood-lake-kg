from pathlib import Path

import pytest

from flashflood_data.core.config import StudyAreaConfig
from flashflood_data.static.sources.budget import StorageBudget


def test_budget_rejects_soft_cap_even_with_free_disk(fake_disk_usage) -> None:
    budget = StorageBudget(
        Path("/data"),
        soft_cap_bytes=8 * 2**30,
        minimum_free_bytes=10 * 2**30,
        existing_new_raw_bytes=7 * 2**30,
    )

    decision = budget.preflight(new_bytes=2 * 2**30, temporary_bytes=0)

    assert not decision.allowed
    assert decision.reason == "new_raw_soft_cap"
    assert decision.projected_new_raw_bytes == 9 * 2**30
    assert decision.projected_free_bytes == 28 * 2**30


def test_budget_rejects_insufficient_post_download_space(fake_disk_usage) -> None:
    budget = StorageBudget(
        Path("/data"),
        soft_cap_bytes=32 * 2**30,
        minimum_free_bytes=10 * 2**30,
        existing_new_raw_bytes=0,
    )

    decision = budget.preflight(new_bytes=21 * 2**30, temporary_bytes=0)

    assert not decision.allowed
    assert decision.reason == "minimum_free_space"
    assert decision.projected_free_bytes == 9 * 2**30


def test_budget_allows_exact_soft_cap_and_free_reserve(fake_disk_usage) -> None:
    budget = StorageBudget(
        Path("/data"),
        soft_cap_bytes=8 * 2**30,
        minimum_free_bytes=10 * 2**30,
        existing_new_raw_bytes=7 * 2**30,
    )

    cap_decision = budget.preflight(new_bytes=1 * 2**30, temporary_bytes=0)
    reserve_decision = StorageBudget(
        Path("/data"), soft_cap_bytes=32 * 2**30, minimum_free_bytes=10 * 2**30
    ).preflight(new_bytes=20 * 2**30, temporary_bytes=0)

    assert cap_decision.allowed
    assert reserve_decision.allowed


def test_budget_uses_validated_study_area_storage_limits() -> None:
    config = StudyAreaConfig(new_raw_soft_cap_gib=1.5, minimum_free_gib=2.5)

    budget = StorageBudget.from_config(Path("/data"), config, existing_new_raw_bytes=10)

    assert budget.soft_cap_bytes == int(1.5 * 2**30)
    assert budget.minimum_free_bytes == int(2.5 * 2**30)


@pytest.mark.parametrize("new_bytes,temporary_bytes", [(-1, 0), (0, -1)])
def test_budget_rejects_negative_requirements(new_bytes: int, temporary_bytes: int) -> None:
    budget = StorageBudget(Path("/data"), soft_cap_bytes=1, minimum_free_bytes=1)

    with pytest.raises(ValueError, match="non-negative"):
        budget.preflight(new_bytes=new_bytes, temporary_bytes=temporary_bytes)
