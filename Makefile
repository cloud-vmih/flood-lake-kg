.PHONY: setup test lint smoke preflight

setup:
	/home/cloud/.pyenv/shims/python3.11 -m venv .venv
	.venv/bin/pip install -r requirements.lock

test:
	.venv/bin/pytest -q

lint:
	.venv/bin/ruff check src tests

smoke:
	.venv/bin/flashflood-data run-static --profile smoke --root .

preflight:
	.venv/bin/flashflood-data run-static --profile live --root . --stop-after aoi
	.venv/bin/flashflood-data fetch --profile live --root . --resolve-only
