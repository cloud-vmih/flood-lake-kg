"""Static Bronze work is grouped without losing raw object identities."""

import pytest

from flashflood_data.orchestration.bronze.batching import batch_object_refs


def test_batch_object_refs_groups_source_objects_deterministically() -> None:
    refs = batch_object_refs(
        "soilgrids_2_0",
        tuple(f"soil-{index:02d}" for index in range(35)),
        batch_size=16,
    )

    assert [len(ref["object_ids"]) for ref in refs] == [16, 16, 3]
    assert refs[0] == {
        "source_id": "soilgrids_2_0",
        "object_ids": [f"soil-{index:02d}" for index in range(16)],
    }
    assert [object_id for ref in refs for object_id in ref["object_ids"]] == [
        f"soil-{index:02d}" for index in range(35)
    ]

    admin_refs = batch_object_refs(
        "sonla_admin_2025",
        tuple(f"admin-{index:02d}" for index in range(75)),
        batch_size=25,
    )
    soil_refs = batch_object_refs(
        "soilgrids_2_0",
        tuple(f"soil-{index:02d}" for index in range(96)),
        batch_size=16,
    )
    assert [len(ref["object_ids"]) for ref in admin_refs] == [25, 25, 25]
    assert [len(ref["object_ids"]) for ref in soil_refs] == [16] * 6


def test_batch_object_refs_rejects_duplicate_ids_and_invalid_size() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        batch_object_refs("soilgrids_2_0", ("same", "same"), batch_size=16)
    with pytest.raises(ValueError, match="positive"):
        batch_object_refs("soilgrids_2_0", ("one",), batch_size=0)
