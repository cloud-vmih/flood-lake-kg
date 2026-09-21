"""Manual and read-only entry points for the Airflow-owned Bronze pipeline."""

import json
from typing import Annotated
from uuid import uuid4

import typer

from flashflood_data.core.paths import ProjectPaths
from flashflood_data.orchestration.bronze.config import load_bronze_config
from flashflood_data.orchestration.bronze.factory import build_bronze_service

bronze_app = typer.Typer(no_args_is_help=True)


@bronze_app.command("backfill")
def backfill(
    source_id: Annotated[str, typer.Option("--source-id")],
    object_id: Annotated[str | None, typer.Option("--object-id")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
    force_reprocess: Annotated[bool, typer.Option("--force-reprocess")] = False,
    run_id: Annotated[str | None, typer.Option("--run-id")] = None,
) -> None:
    """Parse existing registered raw objects; never call acquisition adapters."""
    config = load_bronze_config(ProjectPaths.discover().root / "config" / "bronze" / "static.yaml")
    if source_id not in config.sources:
        raise typer.BadParameter("unknown Bronze source", param_hint="--source-id")
    service = build_bronze_service()
    version = config.parser_version(source_id)
    selected = list(service.discover(
        source_id, parser_version=version, force_reprocess=force_reprocess
    ))
    if object_id is not None:
        if object_id not in selected:
            raise typer.BadParameter("object is not available for this source", param_hint="--object-id")
        selected = [object_id]
    if dry_run:
        typer.echo(json.dumps({"source_id": source_id, "object_ids": selected}, sort_keys=True))
        return
    identifier = run_id or f"manual-bronze-{uuid4().hex}"
    results = [
        service.process_object(source_id, raw_id, run_id=identifier, parser_version=version)
        for raw_id in selected
    ]
    typer.echo(json.dumps({"run_id": identifier, "results": results}, sort_keys=True))


@bronze_app.command("reconcile")
def reconcile(
    source_id: Annotated[str | None, typer.Option("--source-id")] = None,
) -> None:
    """Read-only reconciliation report between raw objects, Bronze tables, and Meta."""
    service = build_bronze_service()
    report = service.reconcile(source_id=source_id)
    typer.echo(json.dumps(report, indent=2, sort_keys=True))
