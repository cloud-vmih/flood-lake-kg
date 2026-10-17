"""Single Spark/Sedona/Iceberg session builder for Silver applications."""

from __future__ import annotations

import os
from collections.abc import Mapping

from flashflood_data.shared.spark.models import SparkRuntimeConfig

REQUIRED_ENV = (
    "POLARIS_URI",
    "POLARIS_CATALOG",
    "POLARIS_CLIENT_ID",
    "POLARIS_CLIENT_SECRET",
    "MINIO_ENDPOINT",
)


def catalog_options(environ: Mapping[str, str]) -> dict[str, str]:
    values = {key: environ[key] for key in REQUIRED_ENV}
    catalog = values["POLARIS_CATALOG"]
    prefix = f"spark.sql.catalog.{catalog}"
    return {
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.apache.sedona.sql.SedonaSqlExtensions"
        ),
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryo.registrator": "org.apache.sedona.core.serde.SedonaKryoRegistrator",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.ansi.enabled": "true",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.defaultCatalog": catalog,
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.type": "rest",
        f"{prefix}.uri": values["POLARIS_URI"],
        f"{prefix}.warehouse": catalog,
        f"{prefix}.credential": (
            f'{values["POLARIS_CLIENT_ID"]}:{values["POLARIS_CLIENT_SECRET"]}'
        ),
        f"{prefix}.scope": "PRINCIPAL_ROLE:ALL",
        f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{prefix}.s3.endpoint": values["MINIO_ENDPOINT"],
        f"{prefix}.s3.path-style-access": "true",
        f"{prefix}.s3.region": "us-east-1",
        "spark.redaction.regex": "(?i)secret|password|token|access[.]?key|credential",
    }


def session_options(
    environ: Mapping[str, str],
    *,
    shuffle_partitions: int,
    pipeline: str,
    pipeline_run_id: str,
    build_signature: str,
) -> dict[str, str]:
    if shuffle_partitions < 1:
        raise ValueError("shuffle_partitions must be positive")
    tags = (pipeline, pipeline_run_id, build_signature)
    if any(not tag or "," in tag for tag in tags):
        raise ValueError("Spark application tags must be non-empty and cannot contain commas")
    return {
        **catalog_options(environ),
        "spark.sql.shuffle.partitions": str(shuffle_partitions),
        "spark.app.tags": ",".join(tags),
    }


def build_spark_session(
    app_name: str,
    *,
    config: SparkRuntimeConfig | None = None,
    environ: Mapping[str, str] | None = None,
    pipeline: str | None = None,
    pipeline_run_id: str | None = None,
    build_signature: str | None = None,
):
    from pyspark.sql import SparkSession

    environment = os.environ if environ is None else environ
    if config is None:
        options = catalog_options(environment)
        driver_memory = "1g"
    else:
        if not all((pipeline, pipeline_run_id, build_signature)):
            raise ValueError("tagged Spark sessions require pipeline, run ID, and build signature")
        options = session_options(
            environment,
            shuffle_partitions=config.shuffle_partitions,
            pipeline=str(pipeline),
            pipeline_run_id=str(pipeline_run_id),
            build_signature=str(build_signature),
        )
        driver_memory = config.driver_memory
    builder = SparkSession.builder.appName(app_name).config(
        "spark.driver.memory", driver_memory
    )
    for key, value in options.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
