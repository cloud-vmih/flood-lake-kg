.PHONY: setup test lint smoke inventory preflight preflight-aoi resolve-live live qa-map

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
