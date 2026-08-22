.PHONY: setup test lint

setup:
	.venv/bin/python -m pip install -r requirements.lock
	.venv/bin/python -m pip install --no-deps -e .

test:
	.venv/bin/pytest

lint:
	.venv/bin/ruff check src tests
