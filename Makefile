.PHONY: setup test lint smoke inventory preflight preflight-aoi resolve-live live qa-map lakehouse-init lakehouse-up lakehouse-status lakehouse-smoke lakehouse-down

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

lakehouse-init:
	infra/scripts/init-lakehouse-env.sh
	docker compose config --quiet

lakehouse-up: lakehouse-init
	infra/scripts/check-docker-access.sh
	docker compose up -d --wait postgres minio polaris airflow-api-server airflow-scheduler airflow-dag-processor

lakehouse-status:
	infra/scripts/check-docker-access.sh
	docker compose ps

lakehouse-smoke:
	infra/scripts/check-docker-access.sh
	infra/scripts/smoke-lakehouse.sh

lakehouse-down:
	infra/scripts/check-docker-access.sh
	docker compose down
