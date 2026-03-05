.DEFAULT_GOAL := all

.PHONY: install
install:
	uv sync

.PHONY: format
format:
	ruff check --fix .
	ruff format .

.PHONY: lint
lint:
	ruff check .
	ruff format --check --diff .

.PHONY: test
test:
	pytest --cov-report term --cov-report xml:coverage.xml --cov=aws_lambda_opentelemetry tests -vvv

.PHONY: build
build:
	uv build


.PHONY: publish
publish:
	uv publish

.PHONY: docs-build
docs-build:
	uv run --group docs mkdocs build

.PHONY: docs-serve
docs-serve:
	uv run --group docs mkdocs serve --dev-addr localhost:8080

.PHONY: docs-deploy
docs-deploy:
	uv run --group docs mkdocs gh-deploy --force

.PHONY: all
all: format lint test build