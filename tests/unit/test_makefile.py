import os
import shutil
import subprocess
from pathlib import Path


def test_setup_installs_locked_dependencies_and_local_package(tmp_path: Path) -> None:
    project = tmp_path / "project"
    python = project / ".venv" / "bin" / "python"
    log = tmp_path / "python-invocations.txt"
    project.mkdir()
    python.parent.mkdir(parents=True)
    shutil.copy(Path("Makefile"), project / "Makefile")
    (project / "requirements.lock").write_text("", encoding="utf-8")
    python.write_text("#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$FAKE_PYTHON_LOG\"\n", encoding="utf-8")
    python.chmod(0o755)

    result = subprocess.run(
        ["make", "setup"],
        cwd=project,
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "FAKE_PYTHON_LOG": str(log)},
    )

    assert result.returncode == 0, result.stderr
    assert log.read_text(encoding="utf-8").splitlines() == [
        "-m pip install -r requirements.lock",
        "-m pip install --no-deps -e .",
    ]
