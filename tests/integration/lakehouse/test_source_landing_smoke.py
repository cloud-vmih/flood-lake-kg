import subprocess
from pathlib import Path

ROOT = Path(__file__).parents[3]
SCRIPT = ROOT / "tools/smoke/source_landing.sh"


def test_make_target_resolves_to_source_landing_smoke_script() -> None:
    result = subprocess.run(
        ["make", "--dry-run", "lakehouse-source-landing-smoke"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )

    assert "tools/smoke/source_landing.sh" in result.stdout


def test_smoke_uses_isolated_prefix_namespace_and_exact_cleanup() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert 'f"smoke_{run_id}"' in text
    assert 'f"_smoke/{run_id}/fixture.bin"' in text
    assert "catalog.purge_table(table_identifier)" in text
    assert "catalog.drop_namespace((namespace,))" in text
    assert "store.delete(published.object_key)" in text
    assert "store.delete(published_manifest.object_key)" in text
    assert "finally:" in text


def test_smoke_script_does_not_print_credentials_or_environment() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "print(os.environ" not in text
    assert "set -x" not in text
    assert "POLARIS_CLIENT_SECRET=" not in text
    assert "MINIO_ROOT_PASSWORD=" not in text
