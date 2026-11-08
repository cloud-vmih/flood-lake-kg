import ast
from pathlib import Path

ROOT = Path(__file__).parents[3]


def test_gateway_is_internal_and_part_of_spark_lifecycle() -> None:
    compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    source = (ROOT / "spark/jobs/submit_gateway.py").read_text(encoding="utf-8")

    ast.parse(source)
    assert "spark-submit-gateway:" in compose
    assert "8090:8090" not in compose
    assert "spark-submit-gateway" in makefile
    assert "ThreadingHTTPServer" in source
    assert "SparkSubmitter" in source
    assert "TemporaryFile" in source
    assert "runner=_bounded_run" in source


def test_spark_driver_and_worker_share_writable_checkpoint_staging() -> None:
    import yaml

    compose = yaml.safe_load((ROOT / "compose.yaml").read_text(encoding="utf-8"))
    expected = "/opt/flashflood/spark-checkpoints:z"
    for service in ("spark-worker", "spark-submit-gateway"):
        volumes = compose["services"][service].get("volumes", [])
        assert any(item.endswith(expected) for item in volumes)
    bootstrap = (ROOT / "tools/bootstrap/init_lakehouse_env.sh").read_text(encoding="utf-8")
    assert '"$LAKEHOUSE_DATA_ROOT/staging/spark-checkpoints"' in bootstrap
    assert 'chmod 1777 "$LAKEHOUSE_DATA_ROOT/staging/spark-checkpoints"' in bootstrap
