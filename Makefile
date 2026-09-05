.PHONY: setup test lint smoke inventory preflight preflight-aoi resolve-live live qa-map lakehouse-python-setup lakehouse-airflow-build lakehouse-python-smoke lakehouse-init lakehouse-up lakehouse-status lakehouse-smoke lakehouse-down spark-build spark-up spark-status spark-down

setup:
	/home/cloud/.pyenv/shims/python3.11 -m venv .venv
	.venv/bin/pip install -r requirements.lock

test:
	.venv/bin/pytest -q

lint:
	.venv/bin/ruff check src tests

smoke:
	.venv/bin/flashflood-data run-static --profile smoke --root .

inventory:
	.venv/bin/flashflood-data inventory --root .

preflight-aoi:
	.venv/bin/flashflood-data run-static --profile live --root . --stop-after aoi --json-summary

resolve-live:
	.venv/bin/flashflood-data fetch --profile live --root . --resolve-only --json-summary

preflight: inventory preflight-aoi resolve-live

live:
	.venv/bin/flashflood-data run-static --profile live --root . --json-summary

qa-map:
	.venv/bin/python -m http.server 8000 --directory dataset/qa/map

lakehouse-python-setup:
	infra/scripts/setup-lakehouse-python.sh

lakehouse-airflow-build: lakehouse-init
	docker compose build airflow-api-server

lakehouse-init:
	infra/scripts/init-lakehouse-env.sh
	docker compose config --quiet

lakehouse-up: lakehouse-init
	infra/scripts/check-docker-access.sh
	docker compose up -d --wait postgres minio polaris airflow-api-server airflow-scheduler airflow-dag-processor
	docker compose run --no-deps --rm polaris-bootstrap

lakehouse-status:
	infra/scripts/check-docker-access.sh
	docker compose ps

lakehouse-smoke:
	infra/scripts/check-docker-access.sh
	infra/scripts/smoke-lakehouse.sh

lakehouse-python-smoke:
	infra/scripts/check-docker-access.sh
	infra/scripts/smoke-lakehouse-python.sh

lakehouse-down:
	infra/scripts/check-docker-access.sh
	docker compose down

spark-build: lakehouse-init
	docker compose --profile spark build spark-master

spark-up: lakehouse-up
	infra/scripts/check-docker-access.sh
	docker compose --profile spark up -d --wait spark-master spark-worker

spark-status:
	infra/scripts/check-docker-access.sh
	docker compose --profile spark ps spark-master spark-worker

spark-down:
	infra/scripts/check-docker-access.sh
	docker compose --profile spark stop spark-worker spark-master
	docker compose --profile spark rm -f spark-worker spark-master
