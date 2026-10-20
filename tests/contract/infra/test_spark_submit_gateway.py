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
