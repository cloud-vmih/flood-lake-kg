"""Command-line shell for independently runnable static pipeline stages."""

import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Annotated, NoReturn
from uuid import uuid4

import typer

from flashflood_data.catalog import AssetCatalog
from flashflood_data.config import EnvironmentSettings, StudyAreaConfig, load_study_area
from flashflood_data.models import AssetStatus, RunRecord
from flashflood_data.paths import ProjectPaths
from flashflood_data.sources.base import SourceContext
from flashflood_data.sources.existing import inventory_existing

app = typer.Typer(no_args_is_help=True)


def stage_unavailable(stage: str) -> NoReturn:
    """Report that a scaffolded stage has not yet been implemented."""
    typer.echo(json.dumps({"stage": stage, "status": "unavailable"}), err=True)
    raise typer.Exit(code=2)


@app.command()
def inventory(
    root: Annotated[Path | None, typer.Option("--root")] = None,
    rehash: Annotated[bool, typer.Option("--rehash")] = False,
) -> None:
    """Inventory existing static source assets."""
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
def fetch(root: Annotated[Path | None, typer.Option("--root")] = None) -> NoReturn:
    """Fetch missing immutable static source assets."""
    stage_unavailable("fetch")


@app.command()
def validate(root: Annotated[Path | None, typer.Option("--root")] = None) -> NoReturn:
    """Validate registered source assets."""
    stage_unavailable("validate")


@app.command()
def harmonize(root: Annotated[Path | None, typer.Option("--root")] = None) -> NoReturn:
    """Harmonize raw source assets."""
    stage_unavailable("harmonize")


@app.command()
def derive(root: Annotated[Path | None, typer.Option("--root")] = None) -> NoReturn:
    """Derive static basin features."""
    stage_unavailable("derive")


@app.command(name="map")
def map_stage(root: Annotated[Path | None, typer.Option("--root")] = None) -> NoReturn:
    """Build static spatial mapping tables."""
    stage_unavailable("map")


@app.command(name="run-static")
def run_static(root: Annotated[Path | None, typer.Option("--root")] = None) -> NoReturn:
    """Run the non-destructive static pipeline."""
    stage_unavailable("run-static")


@app.command()
def cleanup(root: Annotated[Path | None, typer.Option("--root")] = None) -> NoReturn:
    """Report potential cleanup actions."""
    stage_unavailable("cleanup")
