from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
JOBS = ROOT / "spark/jobs"


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


runtime = load_module("runtime", JOBS / "runtime.py")
smoke = load_module("smoke_iceberg", JOBS / "smoke_iceberg.py")


class FakeResult:
    def __init__(self, rows: list[tuple[int, str]]) -> None:
        self.rows = rows

    def collect(self) -> list[tuple[int, str]]:
        return self.rows


class FakeSpark:
    def __init__(
        self,
        rows: list[tuple[int, str]] | None = None,
        read_error: Exception | None = None,
    ) -> None:
        self.rows = rows or []
        self.read_error = read_error
        self.statements: list[str] = []

    def sql(self, statement: str) -> FakeResult:
        self.statements.append(statement)
        if statement.startswith("SELECT"):
            if self.read_error:
                raise self.read_error
            return FakeResult(self.rows)
        return FakeResult([])


def test_catalog_options_require_secrets_and_redact_credentials() -> None:
    env = {
        "POLARIS_URI": "http://polaris:8181/api/catalog",
        "POLARIS_CATALOG": "flood_lakehouse",
        "POLARIS_CLIENT_ID": "client",
        "POLARIS_CLIENT_SECRET": "secret",
        "MINIO_ENDPOINT": "http://minio:9000",
    }

    options = runtime.catalog_options(env)
    prefix = "spark.sql.catalog.flood_lakehouse"
    assert options[f"{prefix}.type"] == "rest"
    assert options[f"{prefix}.warehouse"] == "flood_lakehouse"
    assert options[f"{prefix}.credential"] == "client:secret"
    assert options[f"{prefix}.io-impl"] == "org.apache.iceberg.aws.s3.S3FileIO"
    assert options[f"{prefix}.s3.endpoint"] == "http://minio:9000"
    assert "credential" in options["spark.redaction.regex"].lower()

    for missing in env:
        incomplete = env.copy()
        incomplete.pop(missing)
        with pytest.raises(KeyError, match=missing):
            runtime.catalog_options(incomplete)


def test_smoke_namespace_is_generated_and_not_user_controlled() -> None:
    first = smoke.make_smoke_namespace()
    second = smoke.make_smoke_namespace()

    assert re.fullmatch(r"smoke_[0-9a-f]{32}", first)
    assert first != second


def test_roundtrip_cleans_up_only_its_table_after_success() -> None:
    namespace = "smoke_0123456789abcdef0123456789abcdef"
    spark = FakeSpark(rows=[(1, "basin"), (2, "rain"), (3, "risk")])

    smoke.run_roundtrip(spark, namespace)

    assert spark.statements[-2:] == [
        f"DROP TABLE IF EXISTS flood_lakehouse.{namespace}.roundtrip",
        f"DROP NAMESPACE IF EXISTS flood_lakehouse.{namespace}",
    ]


def test_roundtrip_cleans_up_after_read_failure() -> None:
    namespace = "smoke_0123456789abcdef0123456789abcdef"
    spark = FakeSpark(read_error=RuntimeError("read failed"))

    with pytest.raises(RuntimeError, match="read failed"):
        smoke.run_roundtrip(spark, namespace)

    assert spark.statements[-2:] == [
        f"DROP TABLE IF EXISTS flood_lakehouse.{namespace}.roundtrip",
        f"DROP NAMESPACE IF EXISTS flood_lakehouse.{namespace}",
    ]


@pytest.mark.parametrize(
    "namespace",
    ["public", "smoke_short", "smoke_0123456789ABCDEF0123456789ABCDEF", "smoke_../raw"],
)
def test_roundtrip_rejects_unowned_namespace(namespace: str) -> None:
    spark = FakeSpark()

    with pytest.raises(ValueError, match="unsafe smoke namespace"):
        smoke.run_roundtrip(spark, namespace)

    assert spark.statements == []
