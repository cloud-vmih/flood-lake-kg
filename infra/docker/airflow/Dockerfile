ARG AIRFLOW_VERSION=3.3.1
FROM apache/airflow:${AIRFLOW_VERSION}-python3.11

ARG AIRFLOW_VERSION


COPY --chown=airflow:root requirements/lakehouse.txt /tmp/requirements-lakehouse.txt
COPY --chown=airflow:root pyproject.toml /opt/flashflood-build/pyproject.toml

RUN python -m pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      --constraint "${HOME}/constraints.txt" \
      --requirement /tmp/requirements-lakehouse.txt \
    && python -m pip check \
    && python -m cfgrib selfcheck \
    && python -c 'from importlib.metadata import version; expected={"xarray":"2026.7.0","pyiceberg":"0.11.1","cfgrib":"0.9.15.1","eccodes":"2.48.0","cdsapi":"0.7.7","netCDF4":"1.7.4","apache-airflow":"3.3.1"}; actual={name:version(name) for name in expected}; assert actual == expected, actual'

RUN --mount=type=cache,target=/home/airflow/.cache/pip \
    mkdir -p /opt/flashflood-build/src/flashflood_data \
    && touch /opt/flashflood-build/src/flashflood_data/__init__.py \
    && python -m pip install /opt/flashflood-build \
    && rm -rf /opt/flashflood-build/src \
    && python -m pip check

COPY --chown=airflow:root src/ /opt/flashflood-build/src/

RUN python -m pip install --no-deps \
      --force-reinstall /opt/flashflood-build \
    && python -m pip check \
    && python -c 'import flashflood_data, geopandas, pyarrow, pyiceberg, rasterio'
