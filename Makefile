.DEFAULT_GOAL := all

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
	pytest --cov=aws_lambda_opentelemetry tests -vvv

.PHONY: all
all: format lint test