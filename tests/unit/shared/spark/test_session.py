from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from flashflood_data.shared.spark.session import catalog_options, session_options

ROOT = Path(__file__).parents[4]


ENV = {
    "POLARIS_URI": "http://polaris:8181/api/catalog",
    "POLARIS_CATALOG": "flood_lakehouse",
    "POLARIS_CLIENT_ID": "client",
    "POLARIS_CLIENT_SECRET": "secret",
    "MINIO_ENDPOINT": "http://minio:9000",
}


def test_catalog_options_enable_iceberg_sedona_utc_ansi_and_aqe() -> None:
    options = catalog_options(ENV)

    assert options["spark.sql.extensions"].split(",") == [
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "org.apache.sedona.sql.SedonaSqlExtensions",
    ]
    assert options["spark.sql.session.timeZone"] == "UTC"
    assert options["spark.sql.ansi.enabled"] == "true"
    assert options["spark.sql.adaptive.enabled"] == "true"


def test_session_options_add_small_local_profile_and_stable_application_tags() -> None:
    options = session_options(
        ENV,
        shuffle_partitions=8,
        pipeline="basin_topology",
        pipeline_run_id="run-1",
        build_signature="abc123",
    )

    assert options["spark.sql.shuffle.partitions"] == "8"
    assert options["spark.app.tags"] == "basin_topology,run-1,abc123"


def test_session_module_import_does_not_require_yaml_in_spark_image() -> None:
    script = """
import builtins
original_import = builtins.__import__
def guarded_import(name, *args, **kwargs):
    if name == 'yaml':
        raise ModuleNotFoundError('yaml intentionally absent')
    return original_import(name, *args, **kwargs)
builtins.__import__ = guarded_import
from flashflood_data.shared.spark.session import catalog_options
assert callable(catalog_options)
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        env=os.environ | {"PYTHONPATH": str(ROOT / "src")},
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
