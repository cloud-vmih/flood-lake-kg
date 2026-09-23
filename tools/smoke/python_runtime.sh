#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
VENV_PYTHON=${VENV_PYTHON:-$PROJECT_ROOT/.venv/bin/python}
DOCKER_BIN=${DOCKER_BIN:-docker}

if [ ! -x "$VENV_PYTHON" ]; then
    printf '%s\n' "Run 'make setup' first." >&2
    exit 1
fi

cd "$PROJECT_ROOT"

"$VENV_PYTHON" -m pip check
"$VENV_PYTHON" -m cfgrib selfcheck
"$VENV_PYTHON" - <<'PY'
from importlib.metadata import version

import cfgrib
import cdsapi
import eccodes
import netCDF4
import pyiceberg
import xarray

expected = {
    "xarray": "2026.7.0",
    "pyiceberg": "0.11.1",
    "cfgrib": "0.9.15.1",
    "eccodes": "2.48.0",
    "cdsapi": "0.7.7",
    "netCDF4": "1.7.4",
}
actual = {name: version(name) for name in expected}
if actual != expected:
    raise SystemExit(f"Unexpected host lakehouse versions: {actual!r}")
print("Host Python lakehouse runtime is ready.")
PY

"$DOCKER_BIN" compose exec -T airflow-api-server python -m pip check
"$DOCKER_BIN" compose exec -T airflow-api-server python -m cfgrib selfcheck
"$DOCKER_BIN" compose exec -T airflow-api-server python - <<'PY'
import os
from importlib.metadata import version

import cfgrib
import cdsapi
import eccodes
import netCDF4
import pyiceberg
import xarray
from pyiceberg.catalog import load_catalog

expected = {
    "apache-airflow": "3.3.1",
    "xarray": "2026.7.0",
    "pyiceberg": "0.11.1",
    "cfgrib": "0.9.15.1",
    "eccodes": "2.48.0",
    "cdsapi": "0.7.7",
    "netCDF4": "1.7.4",
}
actual = {name: version(name) for name in expected}
if actual != expected:
    raise SystemExit(f"Unexpected Airflow lakehouse versions: {actual!r}")

catalog = load_catalog(
    os.environ["POLARIS_CATALOG"],
    type="rest",
    uri=os.environ["POLARIS_URI"],
    warehouse=os.environ["POLARIS_CATALOG"],
    credential=(
        f'{os.environ["POLARIS_CLIENT_ID"]}:'
        f'{os.environ["POLARIS_CLIENT_SECRET"]}'
    ),
    scope="PRINCIPAL_ROLE:ALL",
)
catalog.list_namespaces()
print("Airflow Python runtime and Polaris REST catalog are ready.")
PY
