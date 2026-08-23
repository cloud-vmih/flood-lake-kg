import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from flashflood_data import registry
from flashflood_data.models import SourceSpec
from flashflood_data.registry import UnsupportedAdapter, build_adapter, load_source_specs
from flashflood_data.sources.base import SourceAdapter


class NotAnAdapter:
    def __init__(self, spec: SourceSpec) -> None:
        self.spec = spec


def _write_source(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")


def test_registry_rejects_duplicate_source_id_across_files(tmp_path: Path) -> None:
    _write_source(
        tmp_path / "a.yaml",
        "sources:\n  - source_id: x\n    adapter: existing\n    version: '1'\n    license_id: x\n",
    )
    _write_source(
        tmp_path / "b.yaml",
        "sources:\n  - source_id: x\n    adapter: existing\n    version: '2'\n    license_id: x\n",
    )

    with pytest.raises(ValueError, match="duplicate source_id: x"):
        load_source_specs(tmp_path)


def test_registry_filters_disabled_sources_unless_requested(tmp_path: Path) -> None:
    _write_source(
        tmp_path / "sources.yaml",
        """sources:
  - source_id: enabled
    adapter: existing
    version: "1"
    license_id: fixture
  - source_id: disabled
    adapter: existing
    version: "1"
    license_id: fixture
    enabled: false
""",
    )

    assert tuple(load_source_specs(tmp_path)) == ("enabled",)
    assert tuple(load_source_specs(tmp_path, include_disabled=True)) == ("enabled", "disabled")


def test_registry_preserves_deeply_frozen_public_settings(tmp_path: Path) -> None:
    _write_source(
        tmp_path / "sources.yaml",
        """sources:
  - source_id: nested
    adapter: existing
    version: "1"
    license_id: fixture
    settings:
      selection:
        levels: [8, 9, 10]
""",
    )

    settings = load_source_specs(tmp_path)["nested"].settings

    assert settings["selection"]["levels"] == (8, 9, 10)
    with pytest.raises(TypeError):
        settings["selection"]["levels"] = ()  # type: ignore[index]


def test_registry_never_imports_arbitrary_yaml_module(monkeypatch: pytest.MonkeyPatch) -> None:
    imported = False
    module = ModuleType("fixture_evil_adapter")

    class ExplodingAdapter:
        def __init__(self, spec: SourceSpec) -> None:
            nonlocal imported
            imported = True

    module.ExplodingAdapter = ExplodingAdapter
    monkeypatch.setitem(sys.modules, module.__name__, module)
    spec = SourceSpec(
        source_id="unsafe",
        adapter="fixture_evil_adapter.ExplodingAdapter",
        version="1",
        license_id="fixture",
    )

    with pytest.raises(UnsupportedAdapter, match="fixture_evil_adapter"):
        build_adapter(spec)

    assert not imported


def test_registry_resolves_allowed_adapter_through_fixed_lazy_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = SourceSpec(
        source_id="existing-source",
        adapter="existing",
        version="1",
        license_id="fixture",
    )

    class FixtureAdapter(SourceAdapter):
        def __init__(self, received: SourceSpec) -> None:
            super().__init__(received)

        def resolve(self, context, available):
            return []

        def validate_raw(self, path):
            raise NotImplementedError

        def harmonize(self, context, assets):
            return []

    requested_modules: list[str] = []

    def import_module(name: str) -> object:
        requested_modules.append(name)
        return SimpleNamespace(ExistingAdapter=FixtureAdapter)

    monkeypatch.setattr("flashflood_data.registry.import_module", import_module)

    adapter = build_adapter(spec)

    assert isinstance(adapter, FixtureAdapter)
    assert adapter.spec is spec
    assert requested_modules == ["flashflood_data.sources.existing"]


def test_registry_rejects_imported_class_outside_source_adapter_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = SourceSpec(source_id="fixture", adapter="fixture", version="1", license_id="fixture")
    monkeypatch.setattr(registry, "NotAnAdapter", NotAnAdapter, raising=False)
    monkeypatch.setitem(
        registry._ADAPTER_TARGETS,
        "fixture",
        ("flashflood_data.registry", "NotAnAdapter"),
    )

    with pytest.raises(UnsupportedAdapter, match="SourceAdapter"):
        build_adapter(spec)


def test_all_registered_adapter_classes_implement_source_adapter() -> None:
    specs = load_source_specs(Path("config/sources"))

    assert all(issubclass(type(build_adapter(spec)), SourceAdapter) for spec in specs.values())
