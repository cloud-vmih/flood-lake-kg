from __future__ import annotations

import os
from collections.abc import Mapping

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
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
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


def build_spark_session(app_name: str):
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name).config("spark.driver.memory", "1g")
    for key, value in catalog_options(os.environ).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
