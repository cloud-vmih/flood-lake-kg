from __future__ import annotations

import os
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).parents[2]
MANIFEST = ROOT / "requirements/lakehouse.txt"
SETUP_SCRIPT = ROOT / "infra/scripts/setup-lakehouse-python.sh"
SMOKE_SCRIPT = ROOT / "infra/scripts/smoke-lakehouse-python.sh"

EXPECTED_REQUIREMENTS = {
    "xarray==2026.7.0",
    "pyiceberg[pyarrow]==0.11.1",
    "cfgrib==0.9.15.1",
    "eccodes==2.48.0",
}


def test_lakehouse_manifest_pins_the_approved_runtime() -> None:
    assert MANIFEST.is_file()
    requirements = {
        line.strip()
        for line in MANIFEST.read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    assert requirements == EXPECTED_REQUIREMENTS


def test_host_setup_rejects_a_missing_virtual_environment(tmp_path: Path) -> None:
    assert SETUP_SCRIPT.is_file()
    result = subprocess.run(
        [SETUP_SCRIPT],
        cwd=ROOT,
        env=os.environ | {"VENV_PYTHON": str(tmp_path / "missing-python")},
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert result.stderr == "Run 'make setup' first.\n"
    assert result.stdout == ""


def test_host_setup_installs_and_checks_in_fail_fast_order(tmp_path: Path) -> None:
    assert SETUP_SCRIPT.is_file()
    invocation_log = tmp_path / "python-invocations"
    fake_python = tmp_path / "python"
    fake_python.write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' \"$*\" >>\"$INVOCATION_LOG\"\n"
        "if [ \"${1:-}\" = - ]; then cat >/dev/null; fi\n"
    )
    fake_python.chmod(0o755)

    result = subprocess.run(
        [SETUP_SCRIPT],
        cwd=ROOT,
        env=os.environ
        | {
            "VENV_PYTHON": str(fake_python),
            "INVOCATION_LOG": str(invocation_log),
        },
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert invocation_log.read_text().splitlines() == [
        f"-m pip install --disable-pip-version-check --requirement {MANIFEST}",
        "-m pip check",
        "-m cfgrib selfcheck",
        "-",
    ]


def test_make_exposes_the_host_setup_entrypoint() -> None:
    result = subprocess.run(
        ["make", "--dry-run", "lakehouse-python-setup"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == ["infra/scripts/setup-lakehouse-python.sh"]


def test_compose_builds_every_airflow_service_from_one_local_image() -> None:
    data = yaml.safe_load((ROOT / "compose.yaml").read_text())
    expected_build = {
        "context": ".",
        "dockerfile": "infra/airflow/Dockerfile",
        "args": {"AIRFLOW_VERSION": "3.3.1"},
    }

    for name in (
        "airflow-init",
        "airflow-api-server",
        "airflow-scheduler",
        "airflow-dag-processor",
    ):
        service = data["services"][name]
        assert service["image"] == "flood-lakehouse-airflow:3.3.1-python3.11"
        assert service["build"] == expected_build
        dockerfile = ROOT / service["build"]["dockerfile"]
        assert dockerfile.is_file()


def test_make_exposes_the_airflow_image_build() -> None:
    result = subprocess.run(
        ["make", "--dry-run", "lakehouse-airflow-build"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines()[-1] == "docker compose build airflow-api-server"


def test_python_runtime_smoke_runs_read_only_checks_in_both_environments(
    tmp_path: Path,
) -> None:
    assert SMOKE_SCRIPT.is_file()
    invocation_log = tmp_path / "invocations"
    container_program = tmp_path / "container-program.py"

    fake_python = tmp_path / "python"
    fake_python.write_text(
        "#!/bin/sh\n"
        "printf 'python:%s\\n' \"$*\" >>\"$INVOCATION_LOG\"\n"
        "if [ \"${1:-}\" = - ]; then cat >/dev/null; fi\n"
    )
    fake_python.chmod(0o755)

    fake_docker = tmp_path / "docker"
    fake_docker.write_text(
        "#!/bin/sh\n"
        "printf 'docker:%s\\n' \"$*\" >>\"$INVOCATION_LOG\"\n"
        "case \"$*\" in *' python -') cat >\"$CONTAINER_PROGRAM\" ;; esac\n"
    )
    fake_docker.chmod(0o755)

    result = subprocess.run(
        [SMOKE_SCRIPT],
        cwd=ROOT,
        env=os.environ
        | {
            "VENV_PYTHON": str(fake_python),
            "DOCKER_BIN": str(fake_docker),
            "INVOCATION_LOG": str(invocation_log),
            "CONTAINER_PROGRAM": str(container_program),
        },
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert invocation_log.read_text().splitlines() == [
        "python:-m pip check",
        "python:-m cfgrib selfcheck",
        "python:-",
        "docker:compose exec -T airflow-api-server python -m pip check",
        "docker:compose exec -T airflow-api-server python -m cfgrib selfcheck",
        "docker:compose exec -T airflow-api-server python -",
    ]
    program = container_program.read_text()
    assert "load_catalog" in program
    assert "list_namespaces()" in program
    for mutation in ("create_namespace", "create_table", "drop_namespace", "drop_table"):
        assert mutation not in program


def test_make_exposes_the_python_runtime_smoke() -> None:
    result = subprocess.run(
        ["make", "--dry-run", "lakehouse-python-smoke"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "infra/scripts/check-docker-access.sh",
        "infra/scripts/smoke-lakehouse-python.sh",
    ]
