"""Flood-event ingestion and Silver harmonization pipeline namespace."""

from flashflood_data.pipelines.flood_events.legacy_input import (
    read_historical_events,
    resolve_event_administration,
)

__all__ = ["read_historical_events", "resolve_event_administration"]
