.PHONY: install lint format typecheck test check docker-build

install:
	uv sync

lint:
	uv run ruff check .

format:
	uv run ruff format .

typecheck:
	uv run mypy src

test:
	uv run pytest -q

check: lint typecheck test

docker-build:
	docker build -t ratchet:dev .
