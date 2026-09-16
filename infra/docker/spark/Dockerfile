# syntax=docker/dockerfile:1
ARG SPARK_VERSION=4.1.3
FROM apache/spark:${SPARK_VERSION}-scala2.13-java17-python3-ubuntu

ARG SPARK_VERSION=4.1.3
ARG ICEBERG_VERSION=1.11.0
ARG ICEBERG_SPARK_SHA512=f4620bb2d20777146a769a8a939a5bed658e1d5708b6a5155e1f6e831c0bfd29bd1f82618dc59bc0f30399ffcb03add3e66656286692dc6c2be94e4e1f4e7479
ARG ICEBERG_AWS_SHA512=bb50f7dc5f36a001efecf15e4d03eff955466f06557261fbacdd7fe0d88113c82e0b9b2af96cd554cef4a1d4c2348fc950270bc3f51cf6bf1e4526f46ce336a0

USER root

RUN ICEBERG_VERSION="${ICEBERG_VERSION}" \
    ICEBERG_SPARK_SHA512="${ICEBERG_SPARK_SHA512}" \
    ICEBERG_AWS_SHA512="${ICEBERG_AWS_SHA512}" \
    python3 - <<'PY'
import hashlib
import os
import urllib.request
from pathlib import Path

version = os.environ["ICEBERG_VERSION"]
artifacts = (
    (
        f"iceberg-spark-runtime-4.1_2.13-{version}.jar",
        f"iceberg-spark-runtime-4.1_2.13/{version}",
        os.environ["ICEBERG_SPARK_SHA512"],
    ),
    (
        f"iceberg-aws-bundle-{version}.jar",
        f"iceberg-aws-bundle/{version}",
        os.environ["ICEBERG_AWS_SHA512"],
    ),
)
destination = Path("/opt/spark/jars")
base_url = "https://repo.maven.apache.org/maven2/org/apache/iceberg"

for filename, repository_path, expected_hash in artifacts:
    url = f"{base_url}/{repository_path}/{filename}"
    with urllib.request.urlopen(url, timeout=60) as response:
        data = response.read()
    actual_hash = hashlib.sha512(data).hexdigest()
    if actual_hash != expected_hash:
        raise RuntimeError(f"checksum mismatch: {filename}")
    (destination / filename).write_bytes(data)
PY

RUN chown spark:spark \
      "/opt/spark/jars/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar" \
      "/opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

USER spark

RUN /opt/spark/bin/spark-submit --version 2>&1 | grep "version ${SPARK_VERSION}"
