"""Failed Spark applications keep a bounded traceback in gateway logs."""

import importlib.util
import subprocess
from pathlib import Path


def test_gateway_logs_failed_spark_stderr_even_when_cli_returns_zero(monkeypatch, capsys) -> None:
    path = Path(__file__).resolve().parents[4] / "spark/jobs/submit_gateway.py"
    spec = importlib.util.spec_from_file_location("diagnostic_submit_gateway", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    def fake_run(command, *, stdout, stderr, check, timeout):
        stdout.write(b'FLASHFLOOD_SPARK_RESULT={"status":"failed"}\n')
        stderr.write(b"Traceback (most recent call last): real Spark failure\n")
        stderr.write(b"INFO Spark shutdown\n" * 150000)
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    result = module._bounded_run(("spark-submit",), text=True, capture_output=True, check=False, timeout=5)

    assert result.returncode == 0
    assert "Traceback (most recent call last): real Spark failure" in capsys.readouterr().err


def test_gateway_logs_failed_python_driver_stdout(monkeypatch, capsys) -> None:
    path = Path(__file__).resolve().parents[4] / "spark/jobs/submit_gateway.py"
    spec = importlib.util.spec_from_file_location("diagnostic_submit_gateway_stdout", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    def fake_run(command, *, stdout, stderr, check, timeout):
        stdout.write(b"Traceback (most recent call last):\n")
        stdout.write(b"Py4JJavaError: real driver failure\n")
        stdout.write(b"INFO Spark shutdown\n" * 150000)
        stdout.write(b'FLASHFLOOD_SPARK_RESULT={"status":"failed"}\n')
        stderr.write(b"INFO JVM shutdown\n" * 150000)
        return subprocess.CompletedProcess(command, 1)

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    module._bounded_run(("spark-submit",), text=True, capture_output=True, check=False, timeout=5)

    assert "Py4JJavaError: real driver failure" in capsys.readouterr().err
