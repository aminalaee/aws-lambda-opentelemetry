import importlib

import opentelemetry.trace
import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_lambda_opentelemetry.trace import instrument_handler
from aws_lambda_opentelemetry.typing.context import LambdaContext

exporter = InMemorySpanExporter()
provider = TracerProvider()
processor = BatchSpanProcessor(exporter)
provider.add_span_processor(processor)


@pytest.fixture(autouse=True)
def configure_provider():
    importlib.reload(opentelemetry.trace)
    opentelemetry.trace.set_tracer_provider(provider)
    yield


@pytest.fixture(autouse=True)
def clear_exporter():
    exporter.clear()
    yield
    exporter.clear()


@instrument_handler(name=__name__)
def handler(event, context: LambdaContext):
    return {"statusCode": 200, "body": event["body"]}


class TestInstrumentHandler:
    def test_handler_attributes_are_set(self, lambda_context: LambdaContext):
        handler({"body": "Hello, World!"}, lambda_context)

        spans = exporter.get_finished_spans()
        assert len(spans) == 1

        span = spans[0]
        assert span.name == "tests.test_trace.test_helpers"

        assert span.status.status_code.name == "OK"

        attributes = dict(span.attributes) if span.attributes else {}
        assert attributes["faas.invocation_id"] == lambda_context.aws_request_id
        assert attributes["faas.invoked_name"] == lambda_context.function_name
        assert attributes["faas.coldstart"] is True
        assert attributes["cloud.resource_id"] == lambda_context.invoked_function_arn

    def test_handler_return_value(self, lambda_context: LambdaContext):
        response = handler({"body": "Hello, World!"}, lambda_context)

        assert response["statusCode"] == 200
        assert response["body"] == "Hello, World!"

    def test_handler_cold_start_is_false_after_setup(
        self, lambda_context: LambdaContext
    ):
        handler({"body": "Hello, World!"}, lambda_context)

        spans = exporter.get_finished_spans()
        assert len(spans) == 1

        span = spans[0]
        attributes = dict(span.attributes) if span.attributes else {}
        assert attributes["faas.coldstart"] is False

    def test_handler_exception_is_recorded(self, lambda_context: LambdaContext):
        with pytest.raises(KeyError):
            handler({"invalid_key": ""}, lambda_context)

        spans = exporter.get_finished_spans()
        assert len(spans) == 1

        span = spans[0]
        assert span.status.status_code.name == "ERROR"

        event = span.events[0]
        assert event.name == "exception"

        attrs = dict(event.attributes) if event.attributes else {}
        assert attrs["exception.type"] == "KeyError"
        assert attrs["exception.message"] == "'body'"
        assert attrs["exception.stacktrace"] is not None

        traceback = str(attrs["exception.stacktrace"])
        assert traceback.startswith("Traceback (most recent call last):")
        assert traceback.endswith("KeyError: 'body'\n")
