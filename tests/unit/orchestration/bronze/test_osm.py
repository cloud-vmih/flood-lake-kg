"""OSM Bronze parsing keeps only configured flood-relevant features."""

import json
from pathlib import Path

import pytest

from flashflood_data.orchestration.bronze.osm import (
    OsmSelection,
    iter_osm_batches,
    load_osm_selection,
)

FIXTURE = Path(__file__).parents[3] / "fixtures" / "osm" / "sample.osm.pbf"
ROOT = Path(__file__).parents[4]


def test_osm_selection_matches_flood_and_critical_facility_tags(tmp_path: Path) -> None:
    policy = tmp_path / "osm.yaml"
    policy.write_text(
        """
version: v1
groups:
  hydrology:
    waterway: ["*"]
  critical_facility:
    amenity: [hospital, fire_station]
""".strip(),
        encoding="utf-8",
    )

    selection = load_osm_selection(policy)

    assert selection.matching_groups({"waterway": "stream"}) == ("hydrology",)
    assert selection.matching_groups({"amenity": "hospital"}) == ("critical_facility",)
    assert selection.matching_groups({"amenity": "cafe"}) == ()
    assert selection.matching_groups({"waterway": "no"}) == ()


def test_osm_parser_reads_real_pbf_and_keeps_complete_tags() -> None:
    selection = OsmSelection(
        version="test-v1",
        groups={"critical_facility": {"amenity": ("post_box",)}},
    )

    batches = list(
        iter_osm_batches(
            FIXTURE,
            object_id="osm-object",
            run_id="parse-1",
            parser_version="v1",
            selection=selection,
            batch_size=1,
        )
    )

    rows = [row for batch in batches for row in batch]
    assert len(rows) == 1
    assert rows[0]["osm_type"] == "node"
    assert rows[0]["osm_id"] == "818056434"
    assert json.loads(rows[0]["tags_json"])["amenity"] == "post_box"
    assert rows[0]["geometry_wkb"]
    assert rows[0]["crs"] == "EPSG:4326"
    assert len(rows[0]["bbox_wgs84"]) == 4
    assert rows[0]["quality_status"] == "passed"


def test_osm_selection_rejects_invalid_empty_policy() -> None:
    with pytest.raises(ValueError, match="group"):
        OsmSelection(version="v1", groups={})


def test_production_osm_policy_selects_required_facilities_and_flood_features() -> None:
    selection = load_osm_selection(ROOT / "config" / "bronze" / "osm.yaml")

    assert selection.matching_groups({"waterway": "river"}) == ("hydrology",)
    assert selection.matching_groups({"bridge": "yes"}) == ("critical_transport",)
    assert selection.matching_groups({"amenity": "hospital"}) == ("critical_facility",)
    assert selection.matching_groups({"power": "substation"}) == ("critical_facility",)
    assert selection.matching_groups({"amenity": "cafe"}) == ()
