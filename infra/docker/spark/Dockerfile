# syntax=docker/dockerfile:1
ARG SPARK_VERSION=4.1.3
FROM apache/spark:${SPARK_VERSION}-scala2.13-java17-python3-ubuntu

ARG SPARK_VERSION=4.1.3
ARG ICEBERG_VERSION=1.11.0
ARG ICEBERG_SPARK_SHA512=f4620bb2d20777146a769a8a939a5bed658e1d5708b6a5155e1f6e831c0bfd29bd1f82618dc59bc0f30399ffcb03add3e66656286692dc6c2be94e4e1f4e7479
ARG ICEBERG_AWS_SHA512=bb50f7dc5f36a001efecf15e4d03eff955466f06557261fbacdd7fe0d88113c82e0b9b2af96cd554cef4a1d4c2348fc950270bc3f51cf6bf1e4526f46ce336a0
ARG SEDONA_VERSION=1.9.1
ARG SEDONA_SHA512=893eb5802244a6eb734849bad1279feda276261e1ee6d9ce04a935415886c55552a416250eb2f598f8e4fb15309239ec74d39247892fd993641cd5f921586478
ARG GEOTOOLS_VERSION=1.9.1-33.5
ARG GEOTOOLS_SHA512=64d3c4b73e2ddf700efec6d11f08a94fa546c5f88556d7bb76f92f1c5ea15b359035aa7d97c81de8e1c3c853f38ab825e79228210ddbe44e9b3761e39776f7dc

USER root

RUN ICEBERG_VERSION="${ICEBERG_VERSION}" \
    ICEBERG_SPARK_SHA512="${ICEBERG_SPARK_SHA512}" \
    ICEBERG_AWS_SHA512="${ICEBERG_AWS_SHA512}" \
    SEDONA_VERSION="${SEDONA_VERSION}" \
    SEDONA_SHA512="${SEDONA_SHA512}" \
    GEOTOOLS_VERSION="${GEOTOOLS_VERSION}" \
    GEOTOOLS_SHA512="${GEOTOOLS_SHA512}" \
    python3 - <<'PY'
import hashlib
import os
import urllib.request
from pathlib import Path

version = os.environ["ICEBERG_VERSION"]
sedona_version = os.environ["SEDONA_VERSION"]
geotools_version = os.environ["GEOTOOLS_VERSION"]
artifacts = (
    (
        f"iceberg-spark-runtime-4.1_2.13-{version}.jar",
        f"https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/{version}/iceberg-spark-runtime-4.1_2.13-{version}.jar",
        os.environ["ICEBERG_SPARK_SHA512"],
    ),
    (
        f"iceberg-aws-bundle-{version}.jar",
        f"https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/{version}/iceberg-aws-bundle-{version}.jar",
        os.environ["ICEBERG_AWS_SHA512"],
    ),
    (
        f"sedona-spark-shaded-4.1_2.13-{sedona_version}.jar",
        f"https://repo.maven.apache.org/maven2/org/apache/sedona/sedona-spark-shaded-4.1_2.13/{sedona_version}/sedona-spark-shaded-4.1_2.13-{sedona_version}.jar",
        os.environ["SEDONA_SHA512"],
    ),
    (
        f"geotools-wrapper-{geotools_version}.jar",
        f"https://repo.maven.apache.org/maven2/org/datasyslab/geotools-wrapper/{geotools_version}/geotools-wrapper-{geotools_version}.jar",
        os.environ["GEOTOOLS_SHA512"],
    ),
)
destination = Path("/opt/spark/jars")

for filename, url, expected_hash in artifacts:
    with urllib.request.urlopen(url, timeout=60) as response:
        data = response.read()
    actual_hash = hashlib.sha512(data).hexdigest()
    if actual_hash != expected_hash:
        raise RuntimeError(f"checksum mismatch: {filename}")
    (destination / filename).write_bytes(data)
PY

RUN chown spark:spark \
      "/opt/spark/jars/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar" \
      "/opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "/opt/spark/jars/sedona-spark-shaded-4.1_2.13-${SEDONA_VERSION}.jar" \
      "/opt/spark/jars/geotools-wrapper-${GEOTOOLS_VERSION}.jar"

USER spark

RUN /opt/spark/bin/spark-submit --version 2>&1 | grep "version ${SPARK_VERSION}"
