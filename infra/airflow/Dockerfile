ARG AIRFLOW_VERSION=3.3.1
FROM apache/airflow:${AIRFLOW_VERSION}-python3.11

ARG AIRFLOW_VERSION

COPY --chown=airflow:root requirements/lakehouse.txt /tmp/requirements-lakehouse.txt

RUN python -m pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      --constraint "${HOME}/constraints.txt" \
      --requirement /tmp/requirements-lakehouse.txt \
    && python -m pip check \
    && python -m cfgrib selfcheck \
    && python -c 'from importlib.metadata import version; expected={"xarray":"2026.7.0","pyiceberg":"0.11.1","cfgrib":"0.9.15.1","eccodes":"2.48.0","apache-airflow":"3.3.1"}; actual={name:version(name) for name in expected}; assert actual == expected, actual'
