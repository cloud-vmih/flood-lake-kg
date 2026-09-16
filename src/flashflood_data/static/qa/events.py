"""Static quality checks split by subject."""

from __future__ import annotations

import pandas as pd

from flashflood_data.core.config import StudyAreaConfig
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.static.qa.models import CheckResult
from flashflood_data.static.qa.shared import (
    _check,
)

_EVENT_STATUSES = frozenset({"matched", "ambiguous", "unresolved"})


def _event_checks(paths: ProjectPaths, config: StudyAreaConfig) -> list[CheckResult]:
    expected_count = config.historical_event_expected_count
    path = paths.harmonized / "events" / "historical_flood_event_2020_2026.parquet"
    if not path.is_file():
        return [
            _check(
                "events.historical.matching",
                False,
                "fatal",
                f"{expected_count} rows with legal match status and confidence",
                "missing",
                "historical event evidence is missing",
            ),
            _check(
                "events.unresolved_names",
                False,
                "warning",
                "0 unresolved event names",
                "missing",
                "historical event evidence is missing",
            ),
        ]
    try:
        events = pd.read_parquet(path)
        statuses = events.get("match_status", pd.Series(dtype="object")).astype(str)
        confidence = pd.to_numeric(
            events.get("match_confidence", pd.Series(dtype="float64")), errors="coerce"
        )
        valid = (
            len(events) == expected_count
            and statuses.isin(_EVENT_STATUSES).all()
            and confidence.between(0, 1).all()
        )
        unresolved = int((statuses == "unresolved").sum())
        return [
            _check(
                "events.historical.matching",
                valid,
                "fatal",
                f"{expected_count} rows with legal match status and confidence",
                f"{len(events)} rows / {int(statuses.isin(_EVENT_STATUSES).sum())} legal statuses",
                "historical evidence legal-administration replay",
            ),
            _check(
                "events.unresolved_names",
                unresolved == 0,
                "warning",
                "0 unresolved event names",
                unresolved,
                "unresolved event names remain explicit evidence",
            ),
        ]
    except Exception as error:  # noqa: BLE001
        return [
            _check(
                "events.historical.matching",
                False,
                "fatal",
                f"{expected_count} rows with legal match status and confidence",
                type(error).__name__,
                "historical event evidence is unreadable",
            ),
            _check(
                "events.unresolved_names",
                False,
                "warning",
                "0 unresolved event names",
                "unreadable",
                "historical event evidence is unreadable",
            ),
        ]




def checks(paths: ProjectPaths, config: StudyAreaConfig) -> list[CheckResult]:
    return _event_checks(paths, config)
