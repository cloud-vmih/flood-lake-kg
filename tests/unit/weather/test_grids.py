from pathlib import Path

from shapely.geometry import box

from flashflood_data.orchestration.weather.grids import (
    GridDefinition,
    WeatherGridRegistry,
    build_grid_rows,
    grid_definition_for_provider,
)


def _definition() -> GridDefinition:
    return GridDefinition.create(
        source_id="test_weather",
        resolution_x=1.0,
        resolution_y=1.0,
        north=90.0,
        west=-180.0,
        width=360,
        height=180,
        crs="EPSG:4326",
    )


def test_cell_index_does_not_change_when_scope_expands() -> None:
    definition = _definition()
    small = build_grid_rows(definition, box(103.2, 20.2, 104.2, 21.2), "sonla")
    large = build_grid_rows(definition, box(102.2, 19.2, 106.2, 23.2), "vietnam")
    small_by_id = {row["source_grid_id"]: row["cell_index"] for row in small}
    large_by_id = {row["source_grid_id"]: row["cell_index"] for row in large}

    assert small_by_id
    assert all(large_by_id[key] == value for key, value in small_by_id.items())
    assert [row["cell_index"] for row in large] == sorted(
        row["cell_index"] for row in large
    )


def test_provider_grids_use_documented_global_alignment() -> None:
    gsmap = grid_definition_for_provider("gsmap", 0.1)
    era = grid_definition_for_provider("era5_land", 0.1)
    ifs = grid_definition_for_provider("ifs_openmeteo", 0.1)

    gsmap_cell = build_grid_rows(gsmap, box(0.051, 59.949, 0.052, 59.951), "scope")[0]
    era_cell = build_grid_rows(era, box(-0.001, -0.001, 0.001, 0.001), "scope")[0]
    ifs_cell = build_grid_rows(ifs, box(0.051, -0.051, 0.052, -0.049), "scope")[0]

    assert gsmap_cell["centroid_lon"] == 0.05
    assert gsmap_cell["centroid_lat"] == 59.95
    assert era_cell["centroid_lon"] == 0.0
    assert era_cell["centroid_lat"] == 0.0
    assert ifs_cell["centroid_lon"] == 0.05
    assert ifs_cell["centroid_lat"] == -0.05


class MemoryGridWriter:
    def __init__(self) -> None:
        self.rows: list[dict[str, object]] = []

    def get_keyed_rows(self, identifier, key):
        assert identifier == ("silver", "source_grid")
        return [
            row for row in self.rows if all(row[name] == value for name, value in key.items())
        ]

    def upsert_keyed_rows(self, identifier, key_fields, rows):
        assert identifier == ("silver", "source_grid")
        for incoming in rows:
            identity = tuple(incoming[name] for name in key_fields)
            self.rows = [
                row
                for row in self.rows
                if tuple(row[name] for name in key_fields) != identity
            ]
            self.rows.append(dict(incoming))
        return 1


def test_grid_registration_merges_scope_ids_without_duplicate_cells(tmp_path: Path) -> None:
    import geopandas as gpd

    national_path = tmp_path / "vietnam.geoparquet"
    local_path = tmp_path / "sonla.geoparquet"
    gpd.GeoDataFrame(
        {"name": ["vietnam"]}, geometry=[box(102.2, 19.2, 106.2, 23.2)], crs=4326
    ).to_parquet(national_path, index=False)
    gpd.GeoDataFrame(
        {"name": ["sonla"]}, geometry=[box(103.2, 20.2, 104.2, 21.2)], crs=4326
    ).to_parquet(local_path, index=False)
    writer = MemoryGridWriter()
    registry = WeatherGridRegistry(writer, tmp_path)

    first = registry.ensure_grid(
        definition=_definition(),
        grid_scope_path=national_path.relative_to(tmp_path),
        aoi_path=local_path.relative_to(tmp_path),
        spatial_scope_name="sonla-l12-h1",
    )
    second = registry.ensure_grid(
        definition=_definition(),
        grid_scope_path=national_path.relative_to(tmp_path),
        aoi_path=local_path.relative_to(tmp_path),
        spatial_scope_name="sonla-l12-h1",
    )

    identities = [
        (row["source_id"], row["source_grid_version"], row["source_grid_id"])
        for row in writer.rows
    ]
    assert len(identities) == len(set(identities))
    assert first.scope_id == second.scope_id
    assert first.cell_index_by_grid_id == second.cell_index_by_grid_id
    assert first.cell_index_by_grid_id
    assert all(
        first.scope_id in row["scope_ids"]
        for row in writer.rows
        if row["source_grid_id"] in first.cell_index_by_grid_id
    )


def test_grid_registration_rejects_changed_existing_cell_definition(tmp_path: Path) -> None:
    import geopandas as gpd
    import pytest

    scope_path = tmp_path / "scope.geoparquet"
    gpd.GeoDataFrame(
        {"name": ["scope"]}, geometry=[box(103.2, 20.2, 104.2, 21.2)], crs=4326
    ).to_parquet(scope_path, index=False)
    writer = MemoryGridWriter()
    registry = WeatherGridRegistry(writer, tmp_path)
    registration = registry.ensure_grid(
        definition=_definition(),
        grid_scope_path=scope_path.relative_to(tmp_path),
        aoi_path=scope_path.relative_to(tmp_path),
        spatial_scope_name="scope",
    )
    target = next(
        row
        for row in writer.rows
        if row["source_grid_id"] in registration.cell_index_by_grid_id
    )
    target["centroid_lon"] = float(target["centroid_lon"]) + 0.5

    with pytest.raises(ValueError, match="changed definition"):
        registry.ensure_grid(
            definition=_definition(),
            grid_scope_path=scope_path.relative_to(tmp_path),
            aoi_path=scope_path.relative_to(tmp_path),
            spatial_scope_name="scope",
        )
