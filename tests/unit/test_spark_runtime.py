from __future__ import annotations

import os
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).parents[2]
DOCKERFILE = ROOT / "infra/spark/Dockerfile"
SMOKE_SCRIPT = ROOT / "infra/scripts/smoke-spark.sh"


def dockerfile_args() -> dict[str, str]:
    assert DOCKERFILE.is_file()
    return dict(
        line.removeprefix("ARG ").split("=", 1)
        for line in DOCKERFILE.read_text().splitlines()
        if line.startswith("ARG ") and "=" in line
    )


def test_spark_image_pins_compatible_runtime_and_checksums() -> None:
    assert dockerfile_args() == {
        "SPARK_VERSION": "4.1.3",
        "ICEBERG_VERSION": "1.11.0",
        "ICEBERG_SPARK_SHA512": (
            "f4620bb2d20777146a769a8a939a5bed658e1d5708b6a5155e1f6e831c0bfd29"
            "bd1f82618dc59bc0f30399ffcb03add3e66656286692dc6c2be94e4e1f4e7479"
        ),
        "ICEBERG_AWS_SHA512": (
            "bb50f7dc5f36a001efecf15e4d03eff955466f06557261fbacdd7fe0d88113c82"
            "e0b9b2af96cd554cef4a1d4c2348fc950270bc3f51cf6bf1e4526f46ce336a0"
        ),
    }


def test_make_exposes_the_spark_image_build() -> None:
    result = subprocess.run(
        ["make", "--dry-run", "spark-build"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines()[-1] == "docker compose --profile spark build spark-master"


def spark_services() -> dict:
    return yaml.safe_load((ROOT / "compose.yaml").read_text())["services"]


def test_spark_master_and_worker_are_optional_private_and_bounded() -> None:
    services = spark_services()
    master = services["spark-master"]
    worker = services["spark-worker"]
    expected_image = "flood-lakehouse-spark:4.1.3-iceberg1.11.0"

    assert master["image"] == worker["image"] == expected_image
    assert master["profiles"] == worker["profiles"] == ["spark"]
    assert master["ports"] == ["127.0.0.1:7077:7077", "127.0.0.1:8081:8080"]
    assert worker["ports"] == ["127.0.0.1:8082:8081"]
    assert master["mem_limit"] == "768m"
    assert worker["mem_limit"] == "2560m"
    assert worker["depends_on"]["spark-master"]["condition"] == "service_healthy"
    assert master["healthcheck"]
    assert worker["healthcheck"]
    assert "spark-master" in master["healthcheck"]["test"][-1]


def test_spark_submit_is_one_shot_and_receives_runtime_secrets() -> None:
    submit = spark_services()["spark-submit"]

    assert submit["profiles"] == ["spark"]
    assert submit["mem_limit"] == "1536m"
    assert submit["entrypoint"] == ["/opt/spark/bin/spark-submit"]
    assert submit["restart"] == "no"
    assert submit.get("ports") is None
    assert "${POLARIS_CLIENT_SECRET:?run make lakehouse-init}" == submit["environment"][
        "POLARIS_CLIENT_SECRET"
    ]
    assert "${MINIO_ROOT_PASSWORD:?run make lakehouse-init}" == submit["environment"][
        "AWS_SECRET_ACCESS_KEY"
    ]


def test_base_lakehouse_up_does_not_start_spark() -> None:
    result = subprocess.run(
        ["make", "--dry-run", "lakehouse-up"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "spark-master" not in result.stdout
    assert "spark-worker" not in result.stdout


def test_make_exposes_isolated_spark_lifecycle() -> None:
    expected = {
        "spark-up": "docker compose --profile spark up -d --wait spark-master spark-worker",
        "spark-status": "docker compose --profile spark ps spark-master spark-worker",
        "spark-down": "docker compose --profile spark stop spark-worker spark-master",
    }

    for target, command in expected.items():
        result = subprocess.run(
            ["make", "--dry-run", target],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        assert command in result.stdout

    down = subprocess.run(
        ["make", "--dry-run", "spark-down"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    ).stdout
    assert "docker compose down" not in down
    assert "--volumes" not in down
    assert "docker compose --profile spark rm -f spark-worker spark-master" in down


def test_spark_smoke_checks_one_worker_then_submits_one_shot(tmp_path: Path) -> None:
    assert SMOKE_SCRIPT.is_file()
    invocation_log = tmp_path / "invocations"
    fake_docker = tmp_path / "docker"
    fake_docker.write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' \"$*\" >>\"$INVOCATION_LOG\"\n"
        "if [ \"${2:-}\" = exec ]; then printf '1\\n'; fi\n"
    )
    fake_docker.chmod(0o755)

    result = subprocess.run(
        [SMOKE_SCRIPT],
        cwd=ROOT,
        env=os.environ
        | {"DOCKER_BIN": str(fake_docker), "INVOCATION_LOG": str(invocation_log)},
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    invocations = invocation_log.read_text().splitlines()
    assert len(invocations) == 2
    assert invocations[0].startswith("compose exec -T spark-master python3 -c ")
    assert "http://localhost:8080/json/" in invocations[0]
    assert "aliveworkers" in invocations[0]
    assert invocations[1] == (
        "compose --profile spark run --no-deps --rm spark-submit "
        "--master spark://spark-master:7077 --deploy-mode client "
        "/opt/spark/jobs/smoke_iceberg.py"
    )


def test_make_exposes_spark_smoke() -> None:
    result = subprocess.run(
        ["make", "--dry-run", "spark-smoke"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "infra/scripts/smoke-spark.sh" in result.stdout.splitlines()
