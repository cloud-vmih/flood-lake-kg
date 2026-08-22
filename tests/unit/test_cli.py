import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from flashflood_data.cli import app


def test_cli_lists_static_stages() -> None:
    result = CliRunner().invoke(app, ["--help"])
    assert result.exit_code == 0
    for command in (
        "inventory",
        "fetch",
        "validate",
        "harmonize",
        "derive",
        "map",
        "run-static",
        "cleanup",
    ):
        assert command in result.stdout


@pytest.mark.parametrize(
    "command",
    ("fetch", "validate", "harmonize", "derive", "map", "run-static", "cleanup"),
)
def test_static_stage_reports_unavailable_for_explicit_root(command: str, tmp_path: Path) -> None:
    result = CliRunner().invoke(app, [command, "--root", str(tmp_path)])

    assert result.exit_code == 2
    assert json.loads(result.stderr) == {"stage": command, "status": "unavailable"}
