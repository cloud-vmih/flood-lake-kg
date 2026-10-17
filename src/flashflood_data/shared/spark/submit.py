"""Deterministic subprocess lifecycle for standalone Spark applications."""

from __future__ import annotations

import subprocess
from collections.abc import Callable, Sequence
from typing import Any

from flashflood_data.shared.spark.models import (
    SparkApplicationRequest,
    SparkApplicationResult,
)

RESULT_PREFIX = "FLASHFLOOD_SPARK_RESULT="


class SparkSubmitter:
    """Submit one Spark application and reduce process output to a small result DTO."""

    def __init__(
        self,
        *,
        command_prefix: Sequence[str] = ("/opt/spark/bin/spark-submit",),
        master: str = "spark://spark-master:7077",
        cli_path: str = "/opt/flashflood/src/flashflood_data/shared/spark/cli.py",
        timeout_seconds: int = 3600,
        runner: Callable[..., Any] = subprocess.run,
    ) -> None:
        if not command_prefix or any(not item for item in command_prefix):
            raise ValueError("spark-submit command prefix cannot be empty")
        if not master.startswith("spark://"):
            raise ValueError("Spark master must use a spark:// URL")
        if timeout_seconds < 1:
            raise ValueError("Spark submit timeout must be positive")
        self.command_prefix = tuple(command_prefix)
        self.master = master
        self.cli_path = cli_path
        self.timeout_seconds = timeout_seconds
        self.runner = runner

    def command(self, request: SparkApplicationRequest) -> tuple[str, ...]:
        return (
            *self.command_prefix,
            "--master",
            self.master,
            "--deploy-mode",
            "client",
            "--name",
            request.application_id,
            self.cli_path,
            "--request-json",
            request.to_json(),
        )

    def submit(self, request: SparkApplicationRequest) -> SparkApplicationResult:
        command = self.command(request)
        try:
            completed = self.runner(
                command,
                text=True,
                capture_output=True,
                check=False,
                timeout=self.timeout_seconds,
            )
        except subprocess.TimeoutExpired:
            return SparkApplicationResult(
                application_id=request.application_id,
                status="timed_out",
                output_manifest_uri=None,
                error_code="spark_submit_timeout",
            )
        if completed.returncode != 0:
            return SparkApplicationResult(
                application_id=request.application_id,
                status="failed",
                output_manifest_uri=None,
                error_code=f"spark_submit_exit_{completed.returncode}",
            )
        marker = next(
            (
                line.removeprefix(RESULT_PREFIX)
                for line in reversed(completed.stdout.splitlines())
                if line.startswith(RESULT_PREFIX)
            ),
            None,
        )
        if marker is None:
            return SparkApplicationResult(
                application_id=request.application_id,
                status="failed",
                output_manifest_uri=None,
                error_code="missing_result_marker",
            )
        result = SparkApplicationResult.from_json(marker)
        if result.application_id != request.application_id:
            return SparkApplicationResult(
                application_id=request.application_id,
                status="failed",
                output_manifest_uri=None,
                error_code="application_identity_mismatch",
            )
        return result
