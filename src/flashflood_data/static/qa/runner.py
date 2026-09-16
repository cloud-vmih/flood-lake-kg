"""Ordered, fail-safe static quality-gate runner."""

import hashlib
import json
from collections.abc import Callable
from dataclasses import asdict

from flashflood_data.core.config import StudyAreaConfig
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.static.qa import admin, events, hydro, mappings, population, provenance, raster
from flashflood_data.static.qa.models import CheckResult, QAReport
from flashflood_data.static.qa.shared import _check, _config_fingerprint

_admin_checks = admin._admin_checks
_hydro_checks = hydro._hydro_checks
_mapping_checks = mappings._mapping_checks
_raster_checks = raster._raster_checks
_population_checks = population._population_checks
_event_checks = events._event_checks
_provenance_checks = provenance._provenance_checks

CHECK_GROUPS = (_admin_checks, _hydro_checks, _mapping_checks, _raster_checks, _population_checks, _event_checks, _provenance_checks)
_PREFIXES = ("admin", "hydro", "mapping", "raster", "population", "events", "raw")


def run_quality_gates(paths: ProjectPaths, config: StudyAreaConfig) -> QAReport:
    """Evaluate every gate without stopping at the first fatal outcome."""
    def contained(prefix: str, gate: Callable[[], list[CheckResult]]) -> list[CheckResult]:
        try:
            return gate()
        except Exception as error:  # noqa: BLE001
            return [_check(f"{prefix}.gate_execution", False, "fatal", "malformed artifacts produce a published fatal result", type(error).__name__, "quality gate could not evaluate its malformed input")]

    checks = [
        *contained("admin", lambda: _admin_checks(paths, config)),
        *contained("hydro", lambda: _hydro_checks(paths)),
        *contained("mapping", lambda: _mapping_checks(paths)),
        *contained("raster", lambda: _raster_checks(paths, config)),
        *contained("population", lambda: _population_checks(paths)),
        *contained("events", lambda: _event_checks(paths, config)),
        *contained("raw", lambda: _provenance_checks(paths, config)),
    ]
    ordered = tuple(sorted(checks, key=lambda check: check.check_id))
    config_fingerprint = _config_fingerprint(config)
    canonical = json.dumps([asdict(check) for check in ordered], sort_keys=True, separators=(",", ":"))
    run_id = "qa-" + hashlib.sha256((config_fingerprint + canonical).encode("utf-8")).hexdigest()[:16]
    return QAReport(run_id=run_id, config_fingerprint=config_fingerprint, checks=ordered)
