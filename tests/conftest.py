import json
from pathlib import Path

import pytest

from aws_lambda_opentelemetry.typing.context import LambdaContext


class MockLambdaContext(LambdaContext):
    def __init__(self):
        self._function_name = "test_function"
        self._function_version = "$LATEST"
        self._invoked_function_arn = "arn:aws:function:test_function"
        self._memory_limit_in_mb = 128
        self._aws_request_id = "test-request-id"
        self._log_group_name = "/aws/lambda/test_function"
        self._log_stream_name = "2021/01/01/[$LATEST]abcdef123456abcdef123456abcdef12"


def get_fixture(name: str) -> dict:
    path = Path(__file__).parent / "fixtures" / name
    with open(path.resolve()) as f:
        return json.load(f)


@pytest.fixture
def lambda_context() -> LambdaContext:
    return MockLambdaContext()


@pytest.fixture
def sqs_event() -> dict:
    return get_fixture("sqs.json")


@pytest.fixture
def apigateway_event() -> dict:
    return get_fixture("apigateway.json")
