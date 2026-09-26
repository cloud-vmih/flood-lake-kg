"""Pure planning functions for restart-safe weather ingestion."""

import json
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from hashlib import sha256

from flashflood_data.orchestration.weather.models import (
    IngestWatermark,
    PlannedWeatherObject,
    WeatherStreamConfig,
    WeatherWindow,
)


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def operational_start(
    stream: WeatherStreamConfig, watermark: IngestWatermark | None
) -> datetime:
    """Return stream start or replay a bounded overlap before the committed cursor."""
    if watermark is None:
        return stream.start_at
    replay = watermark.cursor_time - timedelta(
        minutes=stream.step_minutes * stream.overlap_steps
    )
    if stream.chunking == "calendar_month":
        replay = replay.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return max(stream.start_at, replay)


def provider_safe_end(now: datetime, stream: WeatherStreamConfig) -> datetime:
    """Floor current time minus provider lag to a complete source step."""
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    candidate = now.astimezone(UTC) - timedelta(minutes=stream.availability_lag_minutes)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    steps = int((candidate - epoch).total_seconds() // (stream.step_minutes * 60))
    safe_end = epoch + timedelta(minutes=steps * stream.step_minutes)
    if stream.chunking == "calendar_month":
        return safe_end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return safe_end


def plan_expected_objects(
    *,
    source_id: str,
    source_version: str,
    stream: WeatherStreamConfig,
    start: datetime,
    end: datetime,
    request_options: Mapping[str, object] | None = None,
) -> tuple[PlannedWeatherObject, ...]:
    """Enumerate every expected provider object in a half-open interval."""
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("planning bounds must be timezone-aware")
    start = start.astimezone(UTC)
    end = end.astimezone(UTC)
    if end <= start:
        return ()
    chunk = timedelta(minutes=stream.chunk_minutes)
    options = dict(stream.options if request_options is None else request_options)
    objects: list[PlannedWeatherObject] = []
    cursor = start
    while cursor < end:
        if stream.chunking == "calendar_month":
            window_end = cursor.replace(
                year=cursor.year + (1 if cursor.month == 12 else 0),
                month=1 if cursor.month == 12 else cursor.month + 1,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )
            window_end = min(window_end, end)
        else:
            window_end = cursor + chunk
            if window_end > end:
                break
        window = WeatherWindow(start=cursor, end=window_end)
        identity = {
            "source_id": source_id,
            "source_version": source_version,
            "stream_id": stream.stream_id,
            "product": stream.product,
            "window_start": _utc_text(cursor),
            "window_end": _utc_text(window_end),
            "variables": list(stream.variables),
            "options": options,
        }
        canonical = json.dumps(identity, sort_keys=True, separators=(",", ":"))
        fingerprint = sha256(canonical.encode("utf-8")).hexdigest()
        cycle = cursor.strftime("%Y%m%dT%H%MZ")
        objects.append(
            PlannedWeatherObject(
                source_id=source_id,
                source_version=source_version,
                stream_id=stream.stream_id,
                product=stream.product,
                asset_id=f"{stream.stream_id}-{cycle}",
                window=window,
                variables=stream.variables,
                request_fingerprint=fingerprint,
                source_cycle_id=cycle,
                model_run_time=cursor if source_id == "ifs_openmeteo" else None,
                options=options,
            )
        )
        cursor = (
            window_end
            if stream.chunking == "calendar_month"
            else cursor + timedelta(minutes=stream.step_minutes)
        )
    return tuple(objects)


def select_missing_or_overlap(
    expected: Sequence[PlannedWeatherObject],
    *,
    existing_asset_ids: set[str],
    overlap_start: datetime,
    recently_committed_asset_ids: set[str] | None = None,
) -> tuple[PlannedWeatherObject, ...]:
    """Skip immutable objects outside the revision lookback and replay the overlap."""
    overlap_start = overlap_start.astimezone(UTC)
    recent = recently_committed_asset_ids or set()
    return tuple(
        item
        for item in expected
        if item.asset_id not in recent
        and (item.asset_id not in existing_asset_ids or item.window.start >= overlap_start)
    )


def advance_contiguous_cursor(
    expected: Sequence[PlannedWeatherObject],
    statuses: Mapping[str, str],
) -> datetime | None:
    """Advance only over consecutive available or explicitly unavailable objects."""
    cursor: datetime | None = None
    for item in sorted(expected, key=lambda value: value.window.start):
        if cursor is not None and item.window.start > cursor:
            break
        if statuses.get(item.asset_id) not in {"available", "no_data"}:
            break
        cursor = item.window.end if cursor is None else max(cursor, item.window.end)
    return cursor


def verify_weather_outcomes(
    plan: Mapping[str, object], outcomes: Iterable[Mapping[str, object]]
) -> list[dict[str, object]]:
    """Validate mapped task coverage and return a JSON-serializable concrete list."""
    missing = plan.get("missing")
    if not isinstance(missing, list) or not all(isinstance(item, Mapping) for item in missing):
        raise ValueError("weather plan missing objects must be a list of mappings")
    materialized = [dict(item) for item in outcomes]
    expected_missing = {str(item["asset_id"]) for item in missing}
    actual = {str(item["asset_id"]) for item in materialized}
    if expected_missing != actual:
        raise ValueError("weather fetch outcomes do not cover every planned missing object")
    if any(item.get("status") not in {"available", "no_data"} for item in materialized):
        raise ValueError("weather coverage contains an unresolved outcome")
    return materialized
