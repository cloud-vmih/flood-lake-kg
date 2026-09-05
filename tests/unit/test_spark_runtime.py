from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).parents[2]
DOCKERFILE = ROOT / "infra/spark/Dockerfile"


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
