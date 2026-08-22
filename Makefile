.PHONY: setup test lint

setup:
	.venv/bin/pip install -r requirements.lock

test:
	.venv/bin/pytest

lint:
	.venv/bin/ruff check src tests
