#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
VENV_PYTHON=${VENV_PYTHON:-$PROJECT_ROOT/.venv/bin/python}
LAKEHOUSE_REQUIREMENTS=${LAKEHOUSE_REQUIREMENTS:-$PROJECT_ROOT/requirements/lakehouse.txt}

if [ ! -x "$VENV_PYTHON" ]; then
    printf '%s\n' "Run 'make setup' first." >&2
    exit 1
fi

"$VENV_PYTHON" -m pip install \
    --disable-pip-version-check \
    --requirement "$LAKEHOUSE_REQUIREMENTS"
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
    raise SystemExit(f"Unexpected lakehouse versions: {actual!r}")
print("Host Python lakehouse runtime is ready.")
PY
