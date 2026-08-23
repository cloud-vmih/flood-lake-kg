from pathlib import Path


def test_makefile_exposes_reproducible_pipeline_commands() -> None:
    makefile = Path("Makefile").read_text(encoding="utf-8")

    assert "/home/cloud/.pyenv/shims/python3.11 -m venv .venv" in makefile
    assert ".venv/bin/pip install -r requirements.lock" in makefile
    assert ".venv/bin/pytest -q" in makefile
    assert ".venv/bin/ruff check src tests" in makefile
    assert ".venv/bin/flashflood-data run-static --profile smoke --root ." in makefile
    assert "run-static --profile live --root . --stop-after aoi" in makefile
    assert "fetch --profile live --root . --resolve-only" in makefile
