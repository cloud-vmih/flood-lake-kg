"""Generic spark-submit entrypoint that dispatches a validated domain module."""

from __future__ import annotations

import argparse
import importlib

from flashflood_data.shared.spark.models import (
    SparkApplicationRequest,
    SparkApplicationResult,
)
from flashflood_data.shared.spark.submit import RESULT_PREFIX


def run_request(request: SparkApplicationRequest) -> SparkApplicationResult:
    module = importlib.import_module(request.entrypoint)
    runner = getattr(module, "run", None)
    if not callable(runner):
        raise TypeError(f"Spark entrypoint has no callable run(): {request.entrypoint}")
    result = runner(request)
    if not isinstance(result, SparkApplicationResult):
        raise TypeError("Spark domain run() must return SparkApplicationResult")
    if result.application_id != request.application_id:
        raise ValueError("Spark domain result application identity does not match request")
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--request-json", required=True)
    args = parser.parse_args(argv)
    result = run_request(SparkApplicationRequest.from_json(args.request_json))
    print(RESULT_PREFIX + result.to_json(), flush=True)
    return 0 if result.status == "succeeded" else 1


if __name__ == "__main__":
    raise SystemExit(main())
