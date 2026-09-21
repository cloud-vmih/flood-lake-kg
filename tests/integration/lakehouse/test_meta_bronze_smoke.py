import subprocess
from pathlib import Path

ROOT = Path(__file__).parents[3]
SCRIPT = ROOT / "tools/smoke/meta_bronze.sh"


def test_make_target_resolves_to_meta_bronze_smoke_script() -> None:
    result = subprocess.run(
        ["make", "--dry-run", "lakehouse-meta-bronze-smoke"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )

    assert "tools/smoke/meta_bronze.sh" in result.stdout


def test_smoke_uses_isolated_prefix_namespace_and_exact_cleanup() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert 'f"smoke_meta_bronze_{run_id}"' in text
    assert 'f"_smoke/{run_id}/hydrobasins.zip"' in text
    assert "catalog.purge_table(target_id)" in text
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
