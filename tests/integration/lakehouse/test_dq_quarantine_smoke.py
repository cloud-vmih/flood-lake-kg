import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[3]
SCRIPT = ROOT / "tools/smoke/dq_quarantine.sh"


def test_dq_smoke_make_target_resolves_to_isolated_script() -> None:
    result = subprocess.run(
        ["make", "--dry-run", "lakehouse-dq-smoke"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    assert "tools/smoke/dq_quarantine.sh" in result.stdout


def test_dq_smoke_uses_isolated_namespace_and_cleans_it() -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    assert 'namespace = f"smoke_dq_{token}"' in source
    assert "catalog.purge_table(identifier)" in source
    assert "catalog.drop_namespace((namespace,))" in source
    assert "DqPublishCoordinator" in source
    assert "succeeded_with_warning" in source
    assert "failed_data_quality" in source
    assert "set -x" not in source
    assert "print(os.environ" not in source


def test_dq_quarantine_against_local_lakehouse_when_enabled() -> None:
    if os.environ.get("FLASHFLOOD_RUN_LAKEHOUSE_INTEGRATION") != "1":
        pytest.skip("set FLASHFLOOD_RUN_LAKEHOUSE_INTEGRATION=1 to run local services")
    subprocess.run(["make", "lakehouse-dq-smoke"], cwd=ROOT, check=True)
