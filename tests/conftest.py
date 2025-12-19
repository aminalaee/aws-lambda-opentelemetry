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


@pytest.fixture
def lambda_context() -> LambdaContext:
    return MockLambdaContext()
