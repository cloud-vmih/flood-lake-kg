.PHONY: doctor bootstrap setup test lint smoke inventory preflight lakehouse-aoi resolve-live live qa-map lakehouse-python-setup lakehouse-build lakehouse-airflow-build lakehouse-python-smoke lakehouse-source-landing-smoke lakehouse-meta-bronze-smoke lakehouse-init lakehouse-up lakehouse-status lakehouse-smoke lakehouse-down query-up query-status query-smoke query-down spark-build spark-up spark-status spark-down spark-smoke
MAKEFLAGS += --no-print-directory
PYTHON ?= python3.11
VENV_PYTHON := .venv/bin/python


doctor:
	PYTHON="$(PYTHON)" tools/bootstrap/check_prerequisites.sh


bootstrap:
	$(MAKE) doctor
	$(MAKE) setup
	$(MAKE) lakehouse-python-setup
	$(MAKE) lakehouse-build
	$(MAKE) lakehouse-up
	$(MAKE) lakehouse-smoke
	$(MAKE) lakehouse-python-smoke


setup:
	$(PYTHON) -c 'import sys; assert sys.version_info[:2] == (3, 11), f"Python 3.11 required, got {sys.version.split()[0]}"'
	$(PYTHON) -m venv .venv
	$(VENV_PYTHON) -m pip install -r requirements.lock

test:
	env -u MAKEFLAGS -u MAKELEVEL .venv/bin/pytest -q


lint:
	.venv/bin/ruff check src tests

smoke:
	.venv/bin/flashflood-data run-static --profile smoke --root .

inventory:
	.venv/bin/flashflood-data inventory --root .

lakehouse-aoi:
	.venv/bin/flashflood-data run-static --profile live --root . --stop-after aoi --json-summary

resolve-live:
	.venv/bin/flashflood-data fetch --profile live --root . --resolve-only --json-summary

preflight: inventory lakehouse-aoi resolve-live

live:
	.venv/bin/flashflood-data run-static --profile live --root . --json-summary

qa-map:
	.venv/bin/python -m http.server 8000 --directory dataset/qa/map

lakehouse-python-setup:
	tools/bootstrap/setup_python_runtime.sh

lakehouse-build: lakehouse-init
	docker compose build airflow-api-server polaris-bootstrap


lakehouse-airflow-build: lakehouse-init
	docker compose build airflow-api-server

lakehouse-init:
	tools/bootstrap/init_lakehouse_env.sh
	docker compose config --quiet

lakehouse-up: lakehouse-init
	tools/bootstrap/check_docker_access.sh
	docker compose up -d --wait postgres minio polaris airflow-api-server airflow-scheduler airflow-dag-processor
	docker compose run --no-deps --rm polaris-bootstrap

lakehouse-status:
	tools/bootstrap/check_docker_access.sh
	docker compose ps

lakehouse-smoke:
	tools/bootstrap/check_docker_access.sh
	tools/smoke/lakehouse.sh

lakehouse-python-smoke:
	tools/bootstrap/check_docker_access.sh
	tools/smoke/python_runtime.sh

lakehouse-source-landing-smoke:
	tools/bootstrap/check_docker_access.sh
	tools/smoke/source_landing.sh

lakehouse-meta-bronze-smoke:
	tools/bootstrap/check_docker_access.sh
	tools/smoke/meta_bronze.sh


lakehouse-down:
	tools/bootstrap/check_docker_access.sh
	docker compose down

query-up: lakehouse-up
	docker compose --profile query up -d --wait trino

query-status:
	tools/bootstrap/check_docker_access.sh
	docker compose --profile query ps trino

query-smoke:
	tools/bootstrap/check_docker_access.sh
	docker compose --profile query exec -T trino trino --catalog lakehouse --execute 'SHOW TABLES FROM lakehouse.meta'
	docker compose --profile query exec -T trino trino --catalog lakehouse --execute 'SELECT object_id FROM lakehouse.meta.source_objects LIMIT 1'

query-down:
	tools/bootstrap/check_docker_access.sh
	docker compose --profile query stop trino
	docker compose --profile query rm -f trino

spark-build: lakehouse-init
	docker compose --profile spark build spark-master

spark-up: lakehouse-up
	tools/bootstrap/check_docker_access.sh
	docker compose --profile spark up -d --wait spark-master spark-worker

spark-status:
	tools/bootstrap/check_docker_access.sh
	docker compose --profile spark ps spark-master spark-worker

spark-smoke: spark-up
	tools/bootstrap/check_docker_access.sh
	tools/smoke/spark_iceberg.sh

spark-down:
	tools/bootstrap/check_docker_access.sh
	docker compose --profile spark stop spark-worker spark-master
	docker compose --profile spark rm -f spark-worker spark-master
