"""Command-line entry points for supported local pipeline operations."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from flashflood_data.cli.commands.bronze import bronze_app
from flashflood_data.pipelines.static_landing.runtime import build_static_landing_service
from flashflood_data.pipelines.study_area.pipeline import build_study_area_pipeline

app = typer.Typer(no_args_is_help=True)
app.add_typer(bronze_app, name="bronze")

RootOption = Annotated[Path | None, typer.Option("--root")]
SourcesOption = Annotated[list[str] | None, typer.Option("--source")]
JsonSummaryOption = Annotated[bool, typer.Option("--json-summary")]
RunIdOption = Annotated[str | None, typer.Option("--run-id")]

STATIC_LANDING_SOURCE_IDS = (
    "sonla_admin_2025",
    "gadm_vnm_4_1",
    "hydrobasins_v1c",
    "basinatlas_v10",
    "hydrorivers_v10",
    "worldpop_vnm_2025",
    "geofabrik_vietnam_snapshot",
    "cop_dem_glo30_2024_1",
    "soilgrids_2_0",
    "esa_worldcover_2021_v200",
)


@app.command(name="build-aoi")
def build_aoi(
    root: RootOption = None,
    run_id: RunIdOption = None,
    json_summary: JsonSummaryOption = False,
) -> None:
    """Bootstrap administrative inputs and build the five canonical L12 AOIs."""
    del json_summary
    try:
        result = build_study_area_pipeline(root).run(run_id)
    except (OSError, TypeError, ValueError) as exc:
        typer.echo(
            json.dumps({"error_code": "aoi_build_failed", "status": "failed"}),
            err=True,
        )
        raise typer.Exit(code=1) from exc
    typer.echo(json.dumps(result.to_document(), sort_keys=True))


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
        typer.echo(
            json.dumps({"error_code": "configuration_error", "status": "failed"}),
            err=True,
        )
        raise typer.Exit(code=2) from exc
    summary = service.run(requested, run_id)
    typer.echo(json.dumps(summary.model_dump(mode="json"), sort_keys=True))
    if summary.status != "completed":
        raise typer.Exit(code=1)
