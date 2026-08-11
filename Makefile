.DEFAULT_GOAL := all

.PHONY: install
install:
	uv sync

.PHONY: format
format:
	uv run --frozen ruff check --fix .
	uv run --frozen ruff format .

.PHONY: lint
lint:
	uv run --frozen ruff check .
	uv run --frozen ruff format --check --diff .

.PHONY: typecheck
typecheck:
	uv run --frozen ty check aws_lambda_opentelemetry tests

.PHONY: test
test:
	uv run --frozen pytest --cov-report term --cov-report xml:coverage.xml --cov=aws_lambda_opentelemetry tests -vvv

.PHONY: build
build:
	uv run --frozen uv build

.PHONY: publish
publish:
	uv run --frozen uv publish

.PHONY: docs-build
docs-build:
	uv run --frozen --group docs mkdocs build

.PHONY: docs-serve
docs-serve:
	uv run --frozen --group docs mkdocs serve --dev-addr localhost:8080

.PHONY: docs-deploy
docs-deploy:
	uv run --frozen --group docs mkdocs gh-deploy --force

.PHONY: check
check:
	uv sync --frozen
	uv run --frozen ruff check .
	uv run --frozen ruff format --check --diff .
	uv run --frozen ty check aws_lambda_opentelemetry tests
	uv run --frozen pytest --cov-report term --cov=aws_lambda_opentelemetry tests -vvv

.PHONY: all
all: format lint typecheck test build
