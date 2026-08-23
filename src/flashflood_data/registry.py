"""Validated source manifests and a fixed lazy adapter registry."""

from importlib import import_module
from pathlib import Path
from typing import Final

import yaml

from flashflood_data.models import SourceFile, SourceSpec
from flashflood_data.sources.base import SourceAdapter

_ADAPTER_TARGETS: Final[dict[str, tuple[str, str]]] = {
    "existing": ("flashflood_data.sources.existing", "ExistingAdapter"),
    "admin_current": ("flashflood_data.sources.admin", "CurrentAdminAdapter"),
    "gadm_admin": ("flashflood_data.sources.admin", "GadmAdminAdapter"),
    "soilgrids": ("flashflood_data.sources.soilgrids", "SoilGridsAdapter"),
    "cop_dem": ("flashflood_data.sources.cop_dem", "CopDemAdapter"),
    "worldcover": ("flashflood_data.sources.worldcover", "WorldCoverAdapter"),
    "geofabrik_osm": ("flashflood_data.sources.osm", "GeofabrikOsmAdapter"),
}


class UnsupportedAdapter(ValueError):
    """Raised when a manifest requests an adapter outside the fixed registry."""


def load_source_specs(config_dir: Path, *, include_disabled: bool = False) -> dict[str, SourceSpec]:
    """Load sorted YAML manifests and return source specifications keyed by ID."""
    specifications: dict[str, SourceSpec] = {}
    for path in sorted(config_dir.glob("*.yaml")):
        source_file = SourceFile.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
        for spec in source_file.sources:
            if spec.source_id in specifications:
                raise ValueError(f"duplicate source_id: {spec.source_id}")
            specifications[spec.source_id] = spec
    if include_disabled:
        return specifications
    return {source_id: spec for source_id, spec in specifications.items() if spec.enabled}


def build_adapter(spec: SourceSpec) -> SourceAdapter:
    """Instantiate an adapter from the fixed allowlist without interpreting YAML as code."""
    try:
        module_name, class_name = _ADAPTER_TARGETS[spec.adapter]
    except KeyError as exc:
        raise UnsupportedAdapter(f"unsupported adapter: {spec.adapter}") from exc
    module = import_module(module_name)
    adapter_class = getattr(module, class_name)
    if not isinstance(adapter_class, type) or not issubclass(adapter_class, SourceAdapter):
        raise UnsupportedAdapter(
            f"registered adapter does not implement SourceAdapter: {spec.adapter}"
        )
    return adapter_class(spec)  # type: ignore[no-any-return]
