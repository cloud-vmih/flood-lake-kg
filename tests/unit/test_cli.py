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
