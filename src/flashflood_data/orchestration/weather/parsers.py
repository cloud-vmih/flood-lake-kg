"""Normalize scoped provider payloads into aligned Bronze raster slices."""

import json
import math
import re
from collections import defaultdict
from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path

import numpy as np
import xarray as xr

from flashflood_data.orchestration.landing.models import SourceObjectRow
from flashflood_data.orchestration.weather.models import WeatherRasterSlice

_SOIL_LEVEL = re.compile(r"(?:layer_)?(\d+_to_\d+cm|layer_\d+)$")
_ERA5_ALIASES = {
    "total_precipitation": "tp",
    "volumetric_soil_water_layer_1": "swvl1",
    "volumetric_soil_water_layer_2": "swvl2",
    "volumetric_soil_water_layer_3": "swvl3",
    "volumetric_soil_water_layer_4": "swvl4",
    "surface_runoff": "sro",
    "sub_surface_runoff": "ssro",
}


def _time(value: object) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, np.datetime64):
        parsed = datetime.fromisoformat(np.datetime_as_string(value, unit="us"))
    else:
        parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _selection(row: SourceObjectRow) -> dict[str, object]:
    document = json.loads(row.selection_json)
    if not isinstance(document, dict):
        raise TypeError("weather source selection must be a mapping")
    required = (
        "window_start", "window_end", "source_cycle_id", "source_grid_version",
        "spatial_scope_id", "cell_count",
    )
    missing = [name for name in required if name not in document]
    if missing:
        raise ValueError(f"weather source selection is missing fields: {missing}")
    return document


def _vertical_level(variable: str) -> str:
    match = _SOIL_LEVEL.search(variable)
    return match.group(1) if match else "surface"


def _value_kind(source_id: str, variable: str) -> str:
    if source_id == "gsmap":
        return "rate"
    if variable.startswith(("soil_moisture", "volumetric_soil_water")):
        return "instantaneous"
    if source_id == "ifs_openmeteo" and variable in {"precipitation", "runoff"}:
        return "preceding_hour_sum"
    if source_id == "era5_land" and variable in {
        "total_precipitation", "surface_runoff", "sub_surface_runoff",
    }:
        return "accumulation_since_00_utc"
    return "instantaneous"


def _number(value: object) -> float | None:
    if value is None:
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def _interval(
    source_id: str, variable: str, valid_time: datetime
) -> tuple[datetime, datetime]:
    kind = _value_kind(source_id, variable)
    if kind == "preceding_hour_sum":
        return valid_time - timedelta(hours=1), valid_time
    if kind == "accumulation_since_00_utc":
        if valid_time.hour == 0:
            return valid_time - timedelta(hours=24), valid_time
        return valid_time.replace(hour=0, minute=0, second=0, microsecond=0), valid_time
    return valid_time, valid_time


def _canonical_key(values: Mapping[str, object]) -> str:
    document = {
        name: value.astimezone(UTC).isoformat().replace("+00:00", "Z")
        if isinstance(value, datetime)
        else value
        for name, value in values.items()
    }
    return json.dumps(document, sort_keys=True, separators=(",", ":"))


def _slice(
    row: SourceObjectRow,
    selection: Mapping[str, object],
    *,
    run_id: str,
    parser_version: str,
    variable: str,
    valid_time: datetime,
    window_start: datetime,
    window_end: datetime,
    cell_indices: Sequence[int],
    values: Sequence[float | None],
    unit: str,
    value_kind: str,
) -> dict[str, object]:
    key = {
        "object_id": row.object_id,
        "source_grid_version": str(selection["source_grid_version"]),
        "spatial_scope_id": str(selection["spatial_scope_id"]),
        "variable": variable,
        "vertical_level": _vertical_level(variable),
        "source_cycle_id": str(selection["source_cycle_id"]),
        "valid_time": valid_time,
        "window_start": window_start,
        "window_end": window_end,
        "source_revision": int(selection.get("source_revision", 0)),
    }
    document: dict[str, object] = {
        "slice_id": sha256(_canonical_key(key).encode()).hexdigest(),
        **key,
        "source_id": row.source_id,
        "source_product": row.product,
        "model_run_time": None if row.model_run_time is None else _time(row.model_run_time),
        "cell_indices": list(map(int, cell_indices)),
        "values": list(values),
        "unit": unit,
        "value_kind": value_kind,
        "available_at": _time(row.available_at or row.retrieved_at),
        "ingest_run_id": run_id,
        "parser_version": parser_version,
        "quality_status": "passed",
    }
    WeatherRasterSlice.model_validate(document)
    expected_count = int(selection["cell_count"])
    if len(cell_indices) != expected_count:
        raise ValueError(
            f"weather slice cell count {len(cell_indices)} does not match manifest {expected_count}"
        )
    return document


def _openmeteo_slices(
    row: SourceObjectRow,
    path: Path,
    selection: Mapping[str, object],
    *,
    run_id: str,
    parser_version: str,
) -> Iterator[dict[str, object]]:
    document = json.loads(path.read_text(encoding="utf-8"))
    responses = document.get("responses")
    if not isinstance(responses, list):
        raise TypeError("Open-Meteo Raw object has no responses list")
    variables = tuple(map(str, selection.get("variables", ())))
    grouped: dict[
        tuple[str, datetime, datetime, datetime, str, str], list[tuple[int, float | None]]
    ] = defaultdict(list)
    for response in responses:
        if "cell_index" not in response:
            raise ValueError("Open-Meteo scoped response has no cell_index")
        cell_index = int(response["cell_index"])
        hourly = response.get("hourly", {})
        units = response.get("hourly_units", {})
        times = hourly.get("time", [])
        for variable in variables:
            source_values = hourly.get(variable)
            if source_values is None:
                raise ValueError(f"Open-Meteo scoped payload is missing variable: {variable}")
            if len(source_values) != len(times):
                raise ValueError(f"Open-Meteo length mismatch for {variable}")
            kind = _value_kind(row.source_id, variable)
            unit = str(units.get(variable, "unknown"))
            for time_value, value in zip(times, source_values, strict=True):
                valid = _time(time_value)
                start, end = _interval(row.source_id, variable, valid)
                grouped[(variable, valid, start, end, unit, kind)].append(
                    (cell_index, _number(value))
                )
    for (variable, valid, start, end, unit, kind), pairs in sorted(
        grouped.items(), key=lambda item: (item[0][1], item[0][0])
    ):
        pairs.sort(key=lambda item: item[0])
        yield _slice(
            row, selection, run_id=run_id, parser_version=parser_version,
            variable=variable, valid_time=valid, window_start=start, window_end=end,
            cell_indices=[item[0] for item in pairs],
            values=[item[1] for item in pairs], unit=unit, value_kind=kind,
        )


def _coordinate_name(dataset: xr.Dataset, choices: tuple[str, ...]) -> str:
    for name in choices:
        if name in dataset.coords or name in dataset.dims:
            return name
    raise ValueError(f"weather dataset lacks coordinate: {choices}")


def _era5_slices(
    row: SourceObjectRow,
    path: Path,
    selection: Mapping[str, object],
    *,
    run_id: str,
    parser_version: str,
) -> Iterator[dict[str, object]]:
    with xr.open_dataset(path) as dataset:
        time_name = _coordinate_name(dataset, ("valid_time", "time"))
        if "cell_index" not in dataset.coords and "cell_index" not in dataset.dims:
            raise ValueError("ERA5 scoped payload has no cell_index coordinate")
        indices = [int(value) for value in dataset["cell_index"].values.reshape(-1)]
        variables = tuple(map(str, selection.get("variables", ())))
        for variable in variables:
            stored = variable if variable in dataset.data_vars else _ERA5_ALIASES.get(variable)
            if stored not in dataset.data_vars:
                raise ValueError(f"ERA5 scoped payload is missing variable: {variable}")
            array = dataset[stored]
            unit = str(array.attrs.get("units", "unknown"))
            time_values = (
                dataset[time_name].values.reshape(-1)
                if time_name in array.dims
                else np.asarray([selection["window_end"]])
            )
            for time_value in time_values:
                time_slice = array.sel({time_name: time_value}) if time_name in array.dims else array
                values = np.asarray(time_slice.values).reshape(-1)
                if len(values) != len(indices):
                    raise ValueError(f"ERA5 scoped value alignment failed: {variable}")
                valid = _time(time_value)
                start, end = _interval(row.source_id, variable, valid)
                yield _slice(
                    row, selection, run_id=run_id, parser_version=parser_version,
                    variable=variable, valid_time=valid, window_start=start, window_end=end,
                    cell_indices=indices, values=[_number(value) for value in values],
                    unit=unit, value_kind=_value_kind(row.source_id, variable),
                )


def _gsmap_slices(
    row: SourceObjectRow,
    path: Path,
    selection: Mapping[str, object],
    *,
    run_id: str,
    parser_version: str,
) -> Iterator[dict[str, object]]:
    with np.load(path) as payload:
        if set(payload.files) != {"cell_indices", "values"}:
            raise ValueError("GSMaP scoped payload must contain cell_indices and values")
        indices = [int(value) for value in payload["cell_indices"].reshape(-1)]
        values = [_number(value) for value in payload["values"].reshape(-1)]
    start = _time(selection["window_start"])
    end = _time(selection["window_end"])
    yield _slice(
        row, selection, run_id=run_id, parser_version=parser_version,
        variable="precipitation", valid_time=end, window_start=start, window_end=end,
        cell_indices=indices, values=values, unit="mm/h", value_kind="rate",
    )


def parse_weather_object(
    row: SourceObjectRow,
    path: Path,
    *,
    run_id: str,
    parser_version: str,
) -> Iterator[dict[str, object]]:
    """Dispatch one registered, AOI-scoped Raw object to its slice parser."""
    selection = _selection(row)
    if row.source_id == "ifs_openmeteo":
        yield from _openmeteo_slices(
            row, path, selection, run_id=run_id, parser_version=parser_version
        )
    elif row.source_id == "era5_land":
        yield from _era5_slices(
            row, path, selection, run_id=run_id, parser_version=parser_version
        )
    elif row.source_id == "gsmap":
        yield from _gsmap_slices(
            row, path, selection, run_id=run_id, parser_version=parser_version
        )
    else:
        raise ValueError(f"unsupported dynamic weather source: {row.source_id}")
