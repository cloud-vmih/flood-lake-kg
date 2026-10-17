"""Compatibility import for existing standalone Spark jobs."""

from flashflood_data.shared.spark.session import build_spark_session, catalog_options

__all__ = ["build_spark_session", "catalog_options"]
