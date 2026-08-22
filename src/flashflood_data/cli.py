"""Command-line shell for independently runnable static pipeline stages."""

import json
from pathlib import Path
from typing import Annotated, NoReturn

import typer

app = typer.Typer(no_args_is_help=True)


def stage_unavailable(stage: str) -> NoReturn:
    """Report that a scaffolded stage has not yet been implemented."""
    typer.echo(json.dumps({"stage": stage, "status": "unavailable"}), err=True)
    raise typer.Exit(code=2)


@app.command()
def inventory(root: Annotated[Path | None, typer.Option("--root")] = None) -> NoReturn:
    """Inventory existing static source assets."""
    stage_unavailable("inventory")


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
