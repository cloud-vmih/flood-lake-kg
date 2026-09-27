import gzip
import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import xarray as xr

from flashflood_data.orchestration.weather.grids import GridDefinition, GridRegistration
from flashflood_data.orchestration.weather.models import (
    FetchedWeatherObject,
    PlannedWeatherObject,
    WeatherWindow,
)
from flashflood_data.orchestration.weather.subset import scope_fetched_object


def _planned(source_id: str, variables: tuple[str, ...]) -> PlannedWeatherObject:
    start = datetime(2026, 9, 27, tzinfo=UTC)
    return PlannedWeatherObject(
        source_id=source_id,
        source_version="v1",
        stream_id="weather",
        product="weather-product",
        asset_id="weather-20260927T0000Z",
        window=WeatherWindow(start=start, end=start.replace(hour=1)),
        variables=variables,
        request_fingerprint="a" * 64,
        source_cycle_id="20260927T0000Z",
    )


def _fetched(path: Path, source_id: str, variables: tuple[str, ...]) -> FetchedWeatherObject:
    now = datetime(2026, 9, 27, 2, tzinfo=UTC)
    return FetchedWeatherObject(
        planned=_planned(source_id, variables),
        path=path,
        filename=path.name,
        media_type="application/octet-stream",
        source_uri="https://provider.example/source",
        retrieved_at=now,
        available_at=now,
        provider_metadata={"provider": "fixture"},
    )


def _registration(definition: GridDefinition, cells: tuple[tuple[int, int], ...]):
    return GridRegistration(
        source_grid_version=definition.source_grid_version,
        scope_id="sonla-scope-v1",
        cell_index_by_grid_id={
            f"row={row},col={column}": row * definition.width + column
            for row, column in cells
        },
        definition=definition,
    )


def test_gsmap_subset_contains_only_scope_cells_and_original_checksum(
    tmp_path: Path,
) -> None:
    definition = GridDefinition.create(
        source_id="gsmap",
        resolution_x=1.0,
        resolution_y=1.0,
        north=2.0,
        west=0.0,
        width=3,
        height=2,
    )
    source = tmp_path / "global.dat.gz"
    provider_bytes = np.arange(6, dtype=">f4").tobytes()
    with source.open("wb") as destination, gzip.GzipFile(
        fileobj=destination, mode="wb", mtime=0
    ) as stream:
        stream.write(provider_bytes)
    original_checksum = sha256(source.read_bytes()).hexdigest()
    original_size = source.stat().st_size
    grid = _registration(definition, ((0, 1), (1, 2)))

    scoped = scope_fetched_object(
        _fetched(source, "gsmap", ("precipitation",)),
        SimpleNamespace(
            provider="gsmap",
            streams=(SimpleNamespace(stream_id="weather", options={"dtype": ">f4"}),),
        ),
        grid,
    )

    with np.load(scoped.path) as payload:
        assert payload["cell_indices"].tolist() == [1, 5]
        assert payload["values"].tolist() == [1.0, 5.0]
    assert scoped.provider_payload_checksum == original_checksum
    assert scoped.provider_payload_size_bytes == original_size
    assert scoped.scoped_payload_size_bytes == scoped.path.stat().st_size
    assert not source.exists()


def test_era_subset_masks_cells_outside_polygon(tmp_path: Path) -> None:
    definition = GridDefinition.create(
        source_id="era5_land",
        resolution_x=1.0,
        resolution_y=1.0,
        north=1.5,
        west=-0.5,
        width=2,
        height=2,
    )
    source = tmp_path / "era.nc"
    dataset = xr.Dataset(
        data_vars={
            "total_precipitation": (
                ("time", "latitude", "longitude"),
                np.arange(4, dtype="float32").reshape(1, 2, 2),
                {"units": "m"},
            ),
            "surface_runoff": (
                ("time", "latitude", "longitude"),
                np.arange(4, 8, dtype="float32").reshape(1, 2, 2),
                {"units": "m"},
            ),
        },
        coords={
            "time": [np.datetime64("2026-09-27T00:00:00")],
            "latitude": [1.0, 0.0],
            "longitude": [0.0, 1.0],
        },
        attrs={"provider": "Copernicus"},
    )
    dataset.to_netcdf(source)
    grid = _registration(definition, ((0, 0), (1, 1)))

    scoped = scope_fetched_object(
        _fetched(source, "era5_land", ("total_precipitation", "surface_runoff")),
        SimpleNamespace(provider="era5_land"),
        grid,
    )

    with xr.open_dataset(scoped.path) as subset:
        assert subset.cell_index.values.tolist() == [0, 3]
        assert set(subset.data_vars) == {"total_precipitation", "surface_runoff"}
        assert subset["total_precipitation"].attrs["units"] == "m"
        assert subset.attrs["provider"] == "Copernicus"
    assert not source.exists()


def test_ifs_subset_rejects_response_outside_requested_scope(tmp_path: Path) -> None:
    import pytest

    definition = GridDefinition.create(
        source_id="ifs_openmeteo",
        resolution_x=1.0,
        resolution_y=1.0,
        north=1.0,
        west=0.0,
        width=2,
        height=2,
    )
    source = tmp_path / "ifs.json"
    source.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "responses": [
                    {"latitude": 0.5, "longitude": 0.5, "hourly": {}},
                    {"latitude": 5.0, "longitude": 5.0, "hourly": {}},
                ],
            }
        ),
        encoding="utf-8",
    )
    grid = _registration(definition, ((0, 0),))

    with pytest.raises(ValueError, match="outside the registered scope"):
        scope_fetched_object(
            _fetched(source, "ifs_openmeteo", ("precipitation",)),
            SimpleNamespace(provider="ifs_openmeteo"),
            grid,
        )
    assert source.exists()
