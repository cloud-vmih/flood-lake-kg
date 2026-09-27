"""Losslessly subset staged provider weather responses before Raw publication."""

import gzip
import io
import json
import zipfile
from hashlib import sha256
from pathlib import Path

import numpy as np
import xarray as xr

from flashflood_data.orchestration.weather.grids import (
    GridRegistration,
    grid_cell_center,
)
from flashflood_data.orchestration.weather.models import (
    FetchedWeatherObject,
    ScopedWeatherObject,
    WeatherPipelineConfig,
)
from flashflood_data.storage.atomic import atomic_target

_ERA5_ALIASES = {
    "total_precipitation": "tp",
    "volumetric_soil_water_layer_1": "swvl1",
    "volumetric_soil_water_layer_2": "swvl2",
    "volumetric_soil_water_layer_3": "swvl3",
    "volumetric_soil_water_layer_4": "swvl4",
    "surface_runoff": "sro",
    "sub_surface_runoff": "ssro",
}


def _checksum(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _ordered_cells(grid: GridRegistration) -> list[tuple[str, int]]:
    return sorted(grid.cell_index_by_grid_id.items(), key=lambda item: item[1])


def _npy_bytes(values: np.ndarray) -> bytes:
    output = io.BytesIO()
    np.lib.format.write_array(output, values, allow_pickle=False)
    return output.getvalue()


def _write_deterministic_npz(
    path: Path, *, cell_indices: np.ndarray, values: np.ndarray
) -> None:
    with atomic_target(path) as temporary, zipfile.ZipFile(
        temporary, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6
    ) as archive:
        for name, array in (("cell_indices", cell_indices), ("values", values)):
            info = zipfile.ZipInfo(f"{name}.npy", date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o600 << 16
            archive.writestr(info, _npy_bytes(array), compress_type=zipfile.ZIP_DEFLATED)


def _scope_gsmap(
    fetched: FetchedWeatherObject,
    grid: GridRegistration,
    output: Path,
    *,
    dtype: str,
) -> None:
    definition = grid.definition
    with gzip.open(fetched.path, "rb") as stream:
        values = np.frombuffer(stream.read(), dtype=np.dtype(dtype))
    expected = definition.height * definition.width
    if values.size != expected:
        raise ValueError(
            f"GSMaP binary has {values.size} values; expected {expected}"
        )
    cells = _ordered_cells(grid)
    indices = np.asarray([index for _, index in cells], dtype="int64")
    local_values = np.asarray(values[indices], dtype="float32")
    _write_deterministic_npz(output, cell_indices=indices, values=local_values)
    with np.load(output) as payload:
        if payload["cell_indices"].tolist() != indices.tolist():
            raise ValueError("GSMaP scoped payload failed cell-index validation")
        if payload["values"].shape != indices.shape:
            raise ValueError("GSMaP scoped payload failed value alignment validation")


def _coordinate_name(dataset: xr.Dataset, choices: tuple[str, ...]) -> str:
    for name in choices:
        if name in dataset.coords or name in dataset.dims:
            return name
    raise ValueError(f"weather dataset lacks coordinate: {choices}")


def _scope_era5(
    fetched: FetchedWeatherObject, grid: GridRegistration, output: Path
) -> None:
    cells = _ordered_cells(grid)
    centers = [grid_cell_center(grid.definition, grid_id) for grid_id, _ in cells]
    indices = np.asarray([index for _, index in cells], dtype="int64")
    with xr.open_dataset(fetched.path) as source:
        lat_name = _coordinate_name(source, ("latitude", "lat"))
        lon_name = _coordinate_name(source, ("longitude", "lon"))
        latitude = xr.DataArray([item[0] for item in centers], dims="cell_index")
        longitude = xr.DataArray([item[1] for item in centers], dims="cell_index")
        selected = source.sel(
            {lat_name: latitude, lon_name: longitude},
            method="nearest",
            tolerance=min(
                grid.definition.resolution_x, grid.definition.resolution_y
            )
            / 4,
        )
        variables: dict[str, xr.DataArray] = {}
        for variable in fetched.planned.variables:
            stored = variable if variable in selected.data_vars else _ERA5_ALIASES.get(variable)
            if stored not in selected.data_vars:
                raise ValueError(f"ERA5 scoped payload is missing variable: {variable}")
            variables[variable] = selected[stored]
        subset = xr.Dataset(variables, attrs=dict(source.attrs)).assign_coords(
            cell_index=("cell_index", indices)
        )
        with atomic_target(output) as temporary:
            subset.to_netcdf(temporary)
    with xr.open_dataset(output) as verified:
        if verified.cell_index.values.tolist() != indices.tolist():
            raise ValueError("ERA5 scoped payload failed cell-index validation")
        if set(verified.data_vars) != set(fetched.planned.variables):
            raise ValueError("ERA5 scoped payload failed variable validation")


def _scope_ifs(
    fetched: FetchedWeatherObject, grid: GridRegistration, output: Path
) -> None:
    document = json.loads(fetched.path.read_text(encoding="utf-8"))
    responses = document.get("responses")
    if not isinstance(responses, list):
        raise TypeError("Open-Meteo provider payload has no responses list")
    approved = {
        tuple(round(value, 6) for value in grid_cell_center(grid.definition, grid_id)): index
        for grid_id, index in _ordered_cells(grid)
    }
    seen: set[int] = set()
    scoped: list[dict[str, object]] = []
    for response in responses:
        coordinate = (
            round(float(response["latitude"]), 6),
            round(float(response["longitude"]), 6),
        )
        if coordinate not in approved:
            raise ValueError(f"Open-Meteo response is outside the registered scope: {coordinate}")
        index = approved[coordinate]
        if index in seen:
            raise ValueError(f"duplicate Open-Meteo response for cell index: {index}")
        seen.add(index)
        scoped.append({**response, "cell_index": index})
    if seen != set(approved.values()):
        raise ValueError("Open-Meteo response does not cover every registered scope cell")
    document["responses"] = sorted(scoped, key=lambda item: int(item["cell_index"]))
    with atomic_target(output) as temporary:
        temporary.write_text(
            json.dumps(document, sort_keys=True, separators=(",", ":")), encoding="utf-8"
        )


def scope_fetched_object(
    fetched: FetchedWeatherObject,
    config: WeatherPipelineConfig,
    grid: GridRegistration,
) -> ScopedWeatherObject:
    """Write and validate the current AOI subset, then discard provider staging."""
    provider_checksum = _checksum(fetched.path)
    provider_size = fetched.path.stat().st_size
    if config.provider == "gsmap":
        stream = next(
            item for item in config.streams if item.stream_id == fetched.planned.stream_id
        )
        output = fetched.path.with_name(f"{fetched.path.stem}.scoped.npz")
        media_type = "application/x-npz"
        _scope_gsmap(
            fetched,
            grid,
            output,
            dtype=str(stream.options.get("dtype", ">f4")),
        )
    elif config.provider == "era5_land":
        output = fetched.path.with_name(f"{fetched.path.stem}.scoped.nc")
        media_type = "application/x-netcdf"
        _scope_era5(fetched, grid, output)
    elif config.provider == "ifs_openmeteo":
        output = fetched.path.with_name(f"{fetched.path.stem}.scoped.json")
        media_type = "application/json"
        _scope_ifs(fetched, grid, output)
    else:
        raise ValueError(f"unsupported weather subset provider: {config.provider}")
    scoped_checksum = _checksum(output)
    scoped_size = output.stat().st_size
    fetched.path.unlink()
    return ScopedWeatherObject(
        planned=fetched.planned,
        path=output,
        filename=output.name,
        media_type=media_type,
        source_uri=fetched.source_uri,
        retrieved_at=fetched.retrieved_at,
        available_at=fetched.available_at,
        provider_issued_at=fetched.provider_issued_at,
        provider_metadata=fetched.provider_metadata,
        spatial_scope_id=grid.scope_id,
        source_grid_version=grid.source_grid_version,
        cell_indices=tuple(index for _, index in _ordered_cells(grid)),
        provider_payload_checksum=provider_checksum,
        provider_payload_size_bytes=provider_size,
        scoped_payload_checksum=scoped_checksum,
        scoped_payload_size_bytes=scoped_size,
    )
