from __future__ import annotations

import os
import subprocess
from pathlib import Path


ROOT = Path(__file__).parents[2]
MANIFEST = ROOT / "requirements/lakehouse.txt"
SETUP_SCRIPT = ROOT / "infra/scripts/setup-lakehouse-python.sh"

EXPECTED_REQUIREMENTS = {
    "xarray==2026.7.0",
    "pyiceberg[pyarrow]==0.12.0",
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
