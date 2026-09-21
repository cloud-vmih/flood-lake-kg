"""Command-line entry points for independently runnable static-pipeline stages."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Annotated, NoReturn
from uuid import uuid4

import typer

from flashflood_data.catalog import AssetCatalog
from flashflood_data.catalog.models import AssetStatus, RunRecord
from flashflood_data.cli.commands.bronze import bronze_app
from flashflood_data.core.config import EnvironmentSettings, StudyAreaConfig, load_study_area
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.static.sources.base import SourceConfigurationError, SourceContext
from flashflood_data.static.sources.cop_dem import MissingCredentials
from flashflood_data.static.sources.existing import inventory_existing
from flashflood_data.static.workflow import STATIC_ORDER, RunSummary, Stage
from flashflood_data.storage.http import BudgetRejected

app = typer.Typer(no_args_is_help=True)
app.add_typer(bronze_app, name="bronze")

RootOption = Annotated[Path | None, typer.Option("--root")]
SourcesOption = Annotated[list[str] | None, typer.Option("--source")]
ResolveOnlyOption = Annotated[bool, typer.Option("--resolve-only")]
JsonSummaryOption = Annotated[bool, typer.Option("--json-summary")]
ProfileOption = Annotated[str, typer.Option("--profile")]
RunIdOption = Annotated[str | None, typer.Option("--run-id")]

STATIC_LANDING_SOURCE_IDS = (
    "sonla_admin_2025",
    "gadm_vnm_4_1",
    "hydrobasins_v1c",
    "basinatlas_v10",
    "hydrorivers_v10",
    "worldpop_vnm_2025",
    "historical_flood_evidence_2020_2026",
    "geofabrik_vietnam_snapshot",
    "cop_dem_glo30_2024_1",
    "soilgrids_2_0",
    "esa_worldcover_2021_v200",
)


def build_static_landing_service(root: Path | None = None):
    """Compose the production landing service from validated project configuration."""
    from flashflood_data.core.lakehouse import LakehouseSettings
    from flashflood_data.orchestration.landing.config import load_static_landing_config
    from flashflood_data.orchestration.landing.service import StaticSourceLandingService
    from flashflood_data.static.sources.budget import StorageBudget
    from flashflood_data.static.sources.registry import load_source_specs
    from flashflood_data.storage.http import HttpFetcher
    from flashflood_data.storage.iceberg import SourceObjectInventory, load_polaris_catalog
    from flashflood_data.storage.object_store import ObjectPublisher, PyArrowS3ObjectStore

    paths = ProjectPaths.discover(root)
    paths.ensure_output_dirs()
    study_area = load_study_area(paths.root / "config" / "study_area.yaml")
    environment = EnvironmentSettings(_env_file=paths.root / ".env")
    settings_values: dict[str, object] = {"project_root": paths.root}
    if "FLASHFLOOD_STAGING_ROOT" not in os.environ:
        settings_values["staging_root"] = paths.dataset / "lakehouse" / "staging"
    settings = LakehouseSettings(_env_file=paths.root / ".env", **settings_values)
    config = load_static_landing_config(paths.root / "config" / "landing" / "static.yaml")
    source_specs = load_source_specs(paths.root / "config" / "sources")
    catalog = AssetCatalog(paths)
    fetcher = HttpFetcher(
        paths,
        catalog,
        StorageBudget.from_config(paths.dataset, study_area),
        environment=environment,
    )
    object_store = PyArrowS3ObjectStore.from_settings(settings)
    iceberg = SourceObjectInventory(load_polaris_catalog(settings))
    return StaticSourceLandingService(
        config=config,
        publisher=ObjectPublisher(object_store, settings.raw_bucket),
        inventory=iceberg,
        staging_root=settings.staging_root,
        source_specs=source_specs,
        paths=paths,
        catalog=catalog,
        fetcher=fetcher,
        study_area=study_area,
        environment=environment,
    )


def stage_unavailable(stage: str) -> NoReturn:
    """Report a command that intentionally remains outside this milestone."""
    typer.echo(json.dumps({"stage": stage, "status": "unavailable"}), err=True)
    raise typer.Exit(code=2)


def _run_stage(
    stages: list[Stage],
    *,
    root: Path | None,
    source: list[str] | None,
    resolve_only: bool,
    json_summary: bool,
    profile: str,
    command: str,
) -> None:
    if profile not in {"smoke", "live"}:
        raise typer.BadParameter("profile must be 'smoke' or 'live'", param_hint="--profile")
    try:
        from flashflood_data import cli as cli_package

        summary = cli_package.StaticPipeline(ProjectPaths.discover(root), profile=profile).run(
            stages, source, resolve_only=resolve_only, command=command
        )
    except (BudgetRejected, SourceConfigurationError, TypeError, ValueError, MissingCredentials) as exc:
        typer.echo(json.dumps({"error_code": "configuration_error", "status": "failed"}), err=True)
        raise typer.Exit(code=2) from exc
    except Exception as exc:
        typer.echo(json.dumps({"error_code": "stage_error", "status": "failed"}), err=True)
        raise typer.Exit(code=1) from exc
    _emit_summary(summary, json_summary)
    if summary.status != "completed":
        raise typer.Exit(code=1)


def _emit_summary(summary: RunSummary, json_summary: bool) -> None:
    """Emit a sanitized summary; the flag makes this stable for automation."""
    del json_summary
    typer.echo(json.dumps(summary.model_dump(mode="json"), sort_keys=True))


@app.command()
def inventory(
    root: RootOption = None,
    source: SourcesOption = None,
    resolve_only: ResolveOnlyOption = False,
    json_summary: JsonSummaryOption = False,
    profile: ProfileOption = "smoke",
    rehash: Annotated[bool, typer.Option("--rehash")] = False,
) -> None:
    """Inventory existing static source assets without moving them."""
    del source, resolve_only, json_summary, profile
    paths = ProjectPaths.discover(root)
    paths.ensure_output_dirs()
    catalog = AssetCatalog(paths)
    study_path = paths.root / "config" / "study_area.yaml"
    study_area = load_study_area(study_path) if study_path.is_file() else StudyAreaConfig()
    config_body = json.dumps(study_area.model_dump(mode="json"), sort_keys=True)
    run_id = f"inventory-{uuid4().hex}"
    catalog.begin_run(
        RunRecord(
            run_id=run_id,
            command="inventory",
            started_at=datetime.now(UTC),
            config_fingerprint=sha256(config_body.encode("utf-8")).hexdigest(),
        )
    )
    try:
        context = SourceContext(
            paths=paths,
            catalog=catalog,
            study_area=study_area,
            environment=EnvironmentSettings(_env_file=paths.root / ".env"),
            run_id=run_id,
        )
        records = inventory_existing(context, rehash=rehash)
        report = json.loads((paths.catalog / "inventory.json").read_text(encoding="utf-8"))
        if not isinstance(report, dict) or type(report.get("asset_count")) is not int:
            raise ValueError("inventory report has no integer asset_count")
        if report["asset_count"] != len(records):
            raise ValueError("inventory report count does not match registered records")
        if any(record.status is AssetStatus.FAILED for record in records):
            raise ValueError("one or more inventory assets failed validation")
        typer.echo(json.dumps(report, sort_keys=True))
    except Exception as exc:
        catalog.end_run(run_id, "failed", datetime.now(UTC))
        typer.echo(
            json.dumps(
                {"error_code": "inventory_failed", "stage": "inventory", "status": "failed"}
            ),
            err=True,
        )
        raise typer.Exit(code=1) from exc
    catalog.end_run(run_id, "succeeded", datetime.now(UTC))


@app.command()
def fetch(
    root: RootOption = None,
    source: SourcesOption = None,
    resolve_only: ResolveOnlyOption = False,
    json_summary: JsonSummaryOption = False,
    profile: ProfileOption = "smoke",
) -> None:
    """Resolve and fetch missing immutable static source assets."""
    _run_stage(
        [Stage.FETCH],
        root=root,
        source=source,
        resolve_only=resolve_only,
        json_summary=json_summary,
        profile=profile,
        command="fetch",
    )

@app.command()
def validate(
    root: RootOption = None,
    source: SourcesOption = None,
    resolve_only: ResolveOnlyOption = False,
    json_summary: JsonSummaryOption = False,
    profile: ProfileOption = "smoke",
) -> None:
    """Validate registered raw source assets."""
    _run_stage(
        [Stage.VALIDATE],
        root=root,
        source=source,
        resolve_only=resolve_only,
        json_summary=json_summary,
        profile=profile,
        command="validate",
    )


@app.command()
def harmonize(
    root: RootOption = None,
    source: SourcesOption = None,
    resolve_only: ResolveOnlyOption = False,
    json_summary: JsonSummaryOption = False,
    profile: ProfileOption = "smoke",
) -> None:
    """Harmonize sources through their registered adapters and dependency graph."""
    _run_stage(
        [Stage.HARMONIZE],
        root=root,
        source=source,
        resolve_only=resolve_only,
        json_summary=json_summary,
        profile=profile,
        command="harmonize",
    )


@app.command()
def derive(
    root: RootOption = None,
    source: SourcesOption = None,
    resolve_only: ResolveOnlyOption = False,
    json_summary: JsonSummaryOption = False,
    profile: ProfileOption = "smoke",
) -> None:
    """Run registered static-feature derivation handlers."""
    _run_stage(
        [Stage.DERIVE],
        root=root,
        source=source,
        resolve_only=resolve_only,
        json_summary=json_summary,
        profile=profile,
        command="derive",
    )


@app.command(name="map")
def map_stage(
    root: RootOption = None,
    source: SourcesOption = None,
    resolve_only: ResolveOnlyOption = False,
    json_summary: JsonSummaryOption = False,
    profile: ProfileOption = "smoke",
) -> None:
    """Run registered mapping and profile handlers through the MAP seam."""
    _run_stage(
        [Stage.MAP],
        root=root,
        source=source,
        resolve_only=resolve_only,
        json_summary=json_summary,
        profile=profile,
        command="map",
    )


@app.command(name="run-static")
def run_static(
    root: RootOption = None,
    source: SourcesOption = None,
    resolve_only: ResolveOnlyOption = False,
    json_summary: JsonSummaryOption = False,
    profile: ProfileOption = "smoke",
    stop_after: Annotated[str | None, typer.Option("--stop-after")] = None,
) -> None:
    """Run a static-stage prefix; live payload work needs ``--profile live``."""
    try:
        final_stage = Stage(stop_after) if stop_after is not None else STATIC_ORDER[-1]
    except ValueError as exc:
        raise typer.BadParameter("unknown static stage", param_hint="--stop-after") from exc
    stages = list(STATIC_ORDER[: STATIC_ORDER.index(final_stage) + 1])
    _run_stage(
        stages,
        root=root,
        source=source,
        resolve_only=resolve_only or profile != "live",
        json_summary=json_summary,
        profile=profile,
        command="run-static",
    )


@app.command(name="land-static")
def land_static(
    root: RootOption = None,
    source: SourcesOption = None,
    run_id: RunIdOption = None,
    json_summary: JsonSummaryOption = False,
) -> None:
    """Land validated static source objects in MinIO and register their Iceberg inventory."""
    del json_summary
    requested = source or list(STATIC_LANDING_SOURCE_IDS)
    unknown = sorted(set(requested) - set(STATIC_LANDING_SOURCE_IDS))
    if unknown:
        raise typer.BadParameter(
            f"unknown static landing source: {', '.join(unknown)}", param_hint="--source"
        )
    try:
        service = build_static_landing_service(root)
    except (OSError, TypeError, ValueError) as exc:
        typer.echo(json.dumps({"error_code": "configuration_error", "status": "failed"}), err=True)
        raise typer.Exit(code=2) from exc
    summary = service.run(requested, run_id)
    typer.echo(json.dumps(summary.model_dump(mode="json"), sort_keys=True))
    if summary.status != "completed":
        raise typer.Exit(code=1)


@app.command()
def cleanup(root: RootOption = None) -> NoReturn:
    """Report potential cleanup actions."""
    del root
    stage_unavailable("cleanup")
