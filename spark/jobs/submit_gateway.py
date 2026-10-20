"""Internal HTTP gateway that isolates spark-submit drivers from Airflow workers."""

from __future__ import annotations

import json
import os
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from tempfile import TemporaryFile

from flashflood_data.shared.spark.models import SparkApplicationRequest
from flashflood_data.shared.spark.submit import SparkSubmitter

_MAX_REQUEST_BYTES = 1024 * 1024
_MAX_PROCESS_TAIL_BYTES = 2 * 1024 * 1024
_SUBMIT_LOCK = threading.Lock()


def _tail(stream, limit: int = _MAX_PROCESS_TAIL_BYTES) -> str:
    stream.seek(0, os.SEEK_END)
    size = stream.tell()
    stream.seek(max(0, size - limit))
    return stream.read().decode("utf-8", errors="replace")


def _bounded_run(command, *, text, capture_output, check, timeout):
    """Keep long Spark logs on disk and return only a bounded diagnostic tail."""
    if not text or not capture_output:
        raise ValueError("Spark gateway runner requires captured text output")
    with TemporaryFile() as stdout, TemporaryFile() as stderr:
        completed = subprocess.run(
            command,
            stdout=stdout,
            stderr=stderr,
            check=check,
            timeout=timeout,
        )
        return subprocess.CompletedProcess(
            command,
            completed.returncode,
            stdout=_tail(stdout),
            stderr=_tail(stderr),
        )


def _submitter() -> SparkSubmitter:
    return SparkSubmitter(
        command_prefix=("/opt/spark/bin/spark-submit",),
        master=os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"),
        cli_path="/opt/flashflood/src/flashflood_data/shared/spark/cli.py",
        timeout_seconds=int(os.environ.get("SPARK_APPLICATION_TIMEOUT_SECONDS", "3600")),
        runner=_bounded_run,
    )


class Handler(BaseHTTPRequestHandler):
    server_version = "FlashFloodSparkGateway/1"

    def _json(self, status: int, payload: object) -> None:
        data = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        if self.path != "/health":
            self._json(404, {"error": "not_found"})
            return
        self._json(200, {"status": "ok"})

    def do_POST(self) -> None:
        if self.path != "/submit":
            self._json(404, {"error": "not_found"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._json(400, {"error": "invalid_content_length"})
            return
        if length < 1 or length > _MAX_REQUEST_BYTES:
            self._json(413, {"error": "invalid_request_size"})
            return
        try:
            request = SparkApplicationRequest.from_json(
                self.rfile.read(length).decode("utf-8")
            )
        except (TypeError, ValueError, json.JSONDecodeError):
            self._json(400, {"error": "invalid_request"})
            return
        with _SUBMIT_LOCK:
            result = _submitter().submit(request)
        self._json(200, json.loads(result.to_json()))

    def log_message(self, format: str, *args: object) -> None:
        # Do not print request bodies because future argument lists may contain
        # provider-specific values. Standard access metadata is sufficient.
        super().log_message(format, *args)


def main() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", 8090), Handler)
    server.serve_forever()


if __name__ == "__main__":
    main()
