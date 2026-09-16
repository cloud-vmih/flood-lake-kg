"""Shared helpers for static quality checks."""

import hashlib
import json
from collections.abc import Iterable
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from flashflood_data.catalog import AssetCatalog
from flashflood_data.catalog.models import AssetRecord
from flashflood_data.core.config import StudyAreaConfig
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.static.qa.models import CheckResult, Severity


def _check(
    check_id: str,
    passed: bool,
    severity: Severity,
    expected: str,
    actual: object,
    message: str,
    asset_ids: Iterable[str] = (),
) -> CheckResult:
    return CheckResult(
        check_id=check_id,
        passed=bool(passed),
        severity=severity,
        expected=expected,
        actual=str(actual),
        message=message,
        asset_ids=tuple(sorted(str(asset_id) for asset_id in asset_ids)),
    )


def _config_fingerprint(config: StudyAreaConfig) -> str:
    body = json.dumps(config.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _first(paths: Iterable[Path]) -> Path | None:
    return next((path for path in paths if path.is_file()), None)


def _geometry(paths: ProjectPaths, name: str) -> tuple[gpd.GeoDataFrame | None, Path]:
    path = paths.harmonized / "aoi" / f"{name}_aoi.geoparquet"
    if not path.is_file():
        return None, path
    try:
        return gpd.read_parquet(path), path
    except Exception:  # noqa: BLE001 - gates must report a broken artifact.
        return None, path


def _asset_records(paths: ProjectPaths) -> list[AssetRecord]:
    catalog_path = paths.catalog / "assets.parquet"
    if not catalog_path.is_file():
        return []
    try:
        return AssetCatalog(paths)._read_assets()
    except Exception:  # noqa: BLE001
        return []


def _strict_integral_ids(values: pd.Series) -> set[int] | None:
    """Return IDs only when every value is finite and exactly integral."""
    numeric = pd.to_numeric(values, errors="coerce")
    array = numeric.to_numpy(dtype="float64", na_value=np.nan)
    valid = np.isfinite(array) & np.equal(array, np.floor(array))
    if not valid.all():
        return None
    try:
        return set(numeric.astype("int64"))
    except (TypeError, ValueError, OverflowError):
        return None


def _valid_entity_ids(values: pd.Series) -> set[str] | None:
    """Normalize finite, nonblank relationship keys without accepting null sentinels."""

    def normalized(value: object) -> str | None:
        if value is None or isinstance(value, (bool, np.bool_)):
            return None
        try:
            if bool(pd.isna(value)):
                return None
        except (TypeError, ValueError):
            return None
        if isinstance(value, (float, np.floating)):
            if not np.isfinite(value):
                return None
            return format(float(value), ".17g")
        if isinstance(value, (int, np.integer)):
            return str(int(value))
        text = str(value).strip()
        if not text or text.casefold() in {"<na>", "nan", "none", "null"}:
            return None
        return text

    result = [normalized(value) for value in values]
    if any(value is None for value in result):
        return None
    return {value for value in result if value is not None}


