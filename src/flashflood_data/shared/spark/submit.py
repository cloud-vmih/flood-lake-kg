"""Deterministic subprocess lifecycle for standalone Spark applications."""

from __future__ import annotations

import subprocess
import urllib.error
import urllib.request
from collections.abc import Callable, Sequence
from typing import Any
from urllib.parse import urlsplit

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
        marker = next(
            (
                line.removeprefix(RESULT_PREFIX)
                for line in reversed(completed.stdout.splitlines())
                if line.startswith(RESULT_PREFIX)
            ),
            None,
        )
        if marker is not None:
            result = SparkApplicationResult.from_json(marker)
            if result.application_id != request.application_id:
                return SparkApplicationResult(
                    application_id=request.application_id,
                    status="failed",
                    output_manifest_uri=None,
                    error_code="application_identity_mismatch",
                )
            return result
        if completed.returncode != 0:
            return SparkApplicationResult(
                application_id=request.application_id,
                status="failed",
                output_manifest_uri=None,
                error_code=f"spark_submit_exit_{completed.returncode}",
            )
        if marker is None:
            return SparkApplicationResult(
                application_id=request.application_id,
                status="failed",
                output_manifest_uri=None,
                error_code="missing_result_marker",
            )
        raise AssertionError("unreachable Spark result parsing branch")


class SparkGatewaySubmitter:
    """Submit a validated request to the isolated local Spark driver gateway."""

    def __init__(
        self,
        endpoint: str,
        *,
        timeout_seconds: int = 3600,
        opener: Callable[..., Any] = urllib.request.urlopen,
    ) -> None:
        parsed = urlsplit(endpoint)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("Spark gateway endpoint must be absolute HTTP(S)")
        if timeout_seconds < 1:
            raise ValueError("Spark gateway timeout must be positive")
        self.endpoint = endpoint.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.opener = opener

    def submit(self, request: SparkApplicationRequest) -> SparkApplicationResult:
        http_request = urllib.request.Request(
            f"{self.endpoint}/submit",
            data=request.to_json().encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with self.opener(http_request, timeout=self.timeout_seconds) as response:
                payload = response.read().decode("utf-8")
        except urllib.error.HTTPError as error:
            return SparkApplicationResult(
                application_id=request.application_id,
                status="failed",
                output_manifest_uri=None,
                error_code=f"spark_gateway_http_{error.code}",
            )
        except (urllib.error.URLError, TimeoutError):
            return SparkApplicationResult(
                application_id=request.application_id,
                status="failed",
                output_manifest_uri=None,
                error_code="spark_gateway_unreachable",
            )
        result = SparkApplicationResult.from_json(payload)
        if result.application_id != request.application_id:
            return SparkApplicationResult(
                application_id=request.application_id,
                status="failed",
                output_manifest_uri=None,
                error_code="application_identity_mismatch",
            )
        return result
