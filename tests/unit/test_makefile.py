from pathlib import Path


def test_makefile_exposes_reproducible_pipeline_commands() -> None:
    makefile = Path("Makefile").read_text(encoding="utf-8")

    assert "$(PYTHON) -m venv .venv" in makefile
    assert "$(VENV_PYTHON) -m pip install -r requirements.lock" in makefile
    assert ".venv/bin/pytest -q" in makefile
    assert ".venv/bin/ruff check src tests" in makefile
    assert ".venv/bin/flashflood-data run-static --profile smoke --root ." in makefile
    assert "run-static --profile live --root . --stop-after aoi" in makefile
    assert "fetch --profile live --root . --resolve-only" in makefile


def test_makefile_bootstrap_is_machine_independent() -> None:
    makefile = Path("Makefile").read_text(encoding="utf-8")

    assert "PYTHON ?= python3.11" in makefile
    assert "VENV_PYTHON := .venv/bin/python" in makefile
    assert "/home/cloud/" not in makefile
    assert "doctor:" in makefile
    assert "bootstrap:" in makefile
    assert "tools/bootstrap/check_prerequisites.sh" in makefile
    assert "$(MAKE) setup" in makefile
    assert "$(MAKE) lakehouse-python-setup" in makefile
    assert "$(MAKE) lakehouse-build" in makefile
    assert "$(MAKE) lakehouse-up" in makefile
    assert "$(MAKE) lakehouse-smoke" in makefile
    assert "$(MAKE) lakehouse-python-smoke" in makefile


def test_readme_documents_linux_and_wsl_bootstrap() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "make bootstrap" in readme
    assert "Windows + WSL2" in readme
    assert "make setup PYTHON=" in readme
    assert "không đặt repository dưới `/mnt/c/`" in readme
