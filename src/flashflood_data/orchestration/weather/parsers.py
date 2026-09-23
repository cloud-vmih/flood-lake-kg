"""Normalize provider-native weather objects into the Bronze grid-value contract."""

import gzip
import json
import math
import re
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import xarray as xr

from flashflood_data.orchestration.landing.models import SourceObjectRow

_SOIL_LEVEL = re.compile(r"(?:layer_)?(\d+_to_\d+cm|layer_\d+)$")
_ERA5_ALIASES = {
    "total_precipitation": "tp",
    "volumetric_soil_water_layer_1": "swvl1",
    "volumetric_soil_water_layer_2": "swvl2",
    "volumetric_soil_water_layer_3": "swvl3",
    "volumetric_soil_water_layer_4": "swvl4",
    "surface_runoff": "sro",
}


def _time(value: object) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, np.datetime64):
        text = np.datetime_as_string(value, unit="us")
        parsed = datetime.fromisoformat(text)
    else:
        parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _selection(row: SourceObjectRow) -> dict[str, object]:
    document = json.loads(row.selection_json)
    if not isinstance(document, dict):
        raise TypeError("weather source selection must be a mapping")
    return document


def _vertical_level(variable: str) -> str:
    match = _SOIL_LEVEL.search(variable)
    if match:
        return match.group(1)
    return "surface"


def _value_kind(source_id: str, variable: str) -> str:
    if source_id == "gsmap":
        return "rate"
    if variable.startswith(("soil_moisture", "volumetric_soil_water")):
        return "instantaneous"
    if source_id == "ifs_openmeteo" and variable in {"precipitation", "runoff"}:
        return "preceding_hour_sum"
    if source_id == "era5_land" and variable in {"total_precipitation", "surface_runoff"}:
        return "accumulation_since_00_utc"
    return "instantaneous"


def _number(value: object) -> float | None:
    result = float(value)
    return result if math.isfinite(result) else None


def _interval(source_id: str, variable: str, valid_time: datetime) -> tuple[datetime, datetime]:
    kind = _value_kind(source_id, variable)
    if kind == "preceding_hour_sum":
        return valid_time - timedelta(hours=1), valid_time
    if kind == "accumulation_since_00_utc":
        if valid_time.hour == 0:
            return valid_time - timedelta(hours=24), valid_time
        return valid_time.replace(hour=0, minute=0, second=0, microsecond=0), valid_time
    return valid_time, valid_time


def _base(
    row: SourceObjectRow,
    selection: Mapping[str, object],
    *,
    run_id: str,
    parser_version: str,
) -> dict[str, object]:
    available = row.available_at or row.retrieved_at.isoformat()
    return {
        "object_id": row.object_id,
        "source_id": row.source_id,
        "source_grid_version": row.source_version,
        "window_start": _time(selection["window_start"]),
        "window_end": _time(selection["window_end"]),
        "source_revision": int(selection.get("source_revision", 0)),
        "source_cycle_id": str(selection["source_cycle_id"]),
        "model_run_time": None if row.model_run_time is None else _time(row.model_run_time),
        "available_at": _time(available),
        "ingest_run_id": run_id,
        "parser_version": parser_version,
        "quality_status": "passed",
    }


def _openmeteo_rows(
    row: SourceObjectRow,
    path: Path,
    selection: Mapping[str, object],
    base: Mapping[str, object],
) -> Iterator[dict[str, object]]:
    document = json.loads(path.read_text(encoding="utf-8"))
    responses = document.get("responses")
    if not isinstance(responses, list):
        raise TypeError("Open-Meteo Raw object has no responses list")
    variables = tuple(map(str, selection.get("variables", ())))
    for response in responses:
        latitude = float(response["latitude"])
        longitude = float(response["longitude"])
        hourly = response.get("hourly", {})
        units = response.get("hourly_units", {})
        times = hourly.get("time", [])
        grid_id = f"lat={latitude:.6f},lon={longitude:.6f}"
        for variable in variables:
            values = hourly.get(variable)
            if values is None:
                continue
            if len(values) != len(times):
                raise ValueError(f"Open-Meteo length mismatch for {variable}")
            for valid_time, value in zip(times, values, strict=True):
                valid = _time(valid_time)
                window_start, window_end = _interval(row.source_id, variable, valid)
                yield {
                    **base,
                    "source_grid_id": grid_id,
                    "variable": variable,
                    "vertical_level": _vertical_level(variable),
                    "valid_time": valid,
                    "window_start": window_start,
                    "window_end": window_end,
                    "value": None if value is None else _number(value),
                    "unit": str(units.get(variable, "unknown")),
                    "value_kind": _value_kind(row.source_id, variable),
                }


def _coordinate_name(dataset: xr.Dataset, choices: tuple[str, ...]) -> str:
    for name in choices:
        if name in dataset.coords or name in dataset.dims:
            return name
    raise ValueError(f"weather dataset lacks coordinate: {choices}")


def _era5_rows(
    row: SourceObjectRow,
    path: Path,
    selection: Mapping[str, object],
    base: Mapping[str, object],
) -> Iterator[dict[str, object]]:
    with xr.open_dataset(path) as dataset:
        time_name = _coordinate_name(dataset, ("valid_time", "time"))
        lat_name = _coordinate_name(dataset, ("latitude", "lat"))
        lon_name = _coordinate_name(dataset, ("longitude", "lon"))
        variables = tuple(map(str, selection.get("variables", ())))
        for variable in variables:
            stored_variable = variable if variable in dataset.data_vars else _ERA5_ALIASES.get(variable)
            if stored_variable not in dataset.data_vars:
                continue
            array = dataset[stored_variable]
            unit = str(array.attrs.get("units", "unknown"))
            for time_value in dataset[time_name].values.reshape(-1):
                time_slice = array.sel({time_name: time_value}) if time_name in array.dims else array
                for latitude in dataset[lat_name].values.reshape(-1):
                    lat_slice = time_slice.sel({lat_name: latitude}) if lat_name in time_slice.dims else time_slice
                    for longitude in dataset[lon_name].values.reshape(-1):
                        value = lat_slice.sel({lon_name: longitude}) if lon_name in lat_slice.dims else lat_slice
                        scalar = np.asarray(value.values).squeeze()
                        if scalar.size != 1:
                            raise ValueError(f"ERA5 variable has unsupported dimensions: {variable}")
                        valid = _time(time_value)
                        window_start, window_end = _interval(row.source_id, variable, valid)
                        yield {
                            **base,
                            "source_grid_id": f"lat={float(latitude):.6f},lon={float(longitude):.6f}",
                            "variable": variable,
                            "vertical_level": _vertical_level(variable),
                            "valid_time": valid,
                            "window_start": window_start,
                            "window_end": window_end,
                            "value": _number(scalar.item()),
                            "unit": unit,
                            "value_kind": _value_kind(row.source_id, variable),
                        }


def _gsmap_rows(
    row: SourceObjectRow,
    path: Path,
    selection: Mapping[str, object],
    base: Mapping[str, object],
) -> Iterator[dict[str, object]]:
    options = selection.get("options", {})
    if not isinstance(options, Mapping):
        raise TypeError("GSMaP options must be a mapping")
    height = int(options.get("grid_height", 1200))
    width = int(options.get("grid_width", 3600))
    north = float(options.get("grid_north", 60.0))
    west = float(options.get("grid_west", 0.0))
    resolution = float(options.get("grid_resolution_degrees", 0.1))
    dtype = np.dtype(str(options.get("dtype", ">f4")))
    with gzip.open(path, "rb") as stream:
        values = np.frombuffer(stream.read(), dtype=dtype)
    if values.size != height * width:
        raise ValueError("GSMaP binary size does not match configured grid")
    grid = values.reshape(height, width)
    bounds = options.get("aoi_bounds", [-180.0, -90.0, 180.0, 90.0])
    if not isinstance(bounds, (list, tuple)) or len(bounds) != 4:
        raise ValueError("GSMaP AOI bounds must contain west, south, east, north")
    aoi_west, aoi_south, aoi_east, aoi_north = map(float, bounds)
    missing_values = {float(value) for value in options.get("missing_values", [-99.0])}
    valid_time = _time(selection["window_start"])
    latitudes = north - (np.arange(height) + 0.5) * resolution
    longitudes = west + (np.arange(width) + 0.5) * resolution
    normalized_longitudes = np.where(longitudes > 180, longitudes - 360, longitudes)
    row_indices = np.flatnonzero((latitudes >= aoi_south) & (latitudes <= aoi_north))
    longitude_mask = (
        (normalized_longitudes >= aoi_west) & (normalized_longitudes <= aoi_east)
        if aoi_west <= aoi_east
        else (normalized_longitudes >= aoi_west) | (normalized_longitudes <= aoi_east)
    )
    column_indices = np.flatnonzero(longitude_mask)
    for row_index in row_indices:
        latitude = float(latitudes[row_index])
        for column_index in column_indices:
            normalized_longitude = float(normalized_longitudes[column_index])
            yield {
                **base,
                "source_grid_id": f"lat={latitude:.6f},lon={normalized_longitude:.6f}",
                "variable": "precipitation",
                "vertical_level": "surface",
                "valid_time": valid_time,
                "value": (
                    None
                    if float(grid[row_index, column_index]) in missing_values
                    else _number(grid[row_index, column_index])
                ),
                "unit": "mm/h",
                "value_kind": "rate",
            }


def parse_weather_object(
    row: SourceObjectRow,
    path: Path,
    *,
    run_id: str,
    parser_version: str,
) -> Iterator[dict[str, object]]:
    """Dispatch one registered Raw object to its provider parser."""
    selection = _selection(row)
    base = _base(row, selection, run_id=run_id, parser_version=parser_version)
    if row.source_id == "ifs_openmeteo":
        yield from _openmeteo_rows(row, path, selection, base)
    elif row.source_id == "era5_land":
        yield from _era5_rows(row, path, selection, base)
    elif row.source_id == "gsmap":
        yield from _gsmap_rows(row, path, selection, base)
    else:
        raise ValueError(f"unsupported dynamic weather source: {row.source_id}")
