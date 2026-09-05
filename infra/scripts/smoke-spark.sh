#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
DOCKER_BIN=${DOCKER_BIN:-docker}

cd "$PROJECT_ROOT"

alive_workers=$(
    "$DOCKER_BIN" compose exec -T spark-master python3 -c \
        'import json, urllib.request; data=json.load(urllib.request.urlopen("http://localhost:8080/json/", timeout=5)); print(data["aliveworkers"])'
)

if [ "$alive_workers" != "1" ]; then
    printf 'Expected exactly one alive Spark worker, found: %s\n' "$alive_workers" >&2
    exit 1
fi

"$DOCKER_BIN" compose --profile spark run --no-deps --rm spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /opt/spark/jobs/smoke_iceberg.py
