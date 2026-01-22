import importlib

import opentelemetry.trace
import pytest
from aws_lambda_powertools.utilities.typing import LambdaContext
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_lambda_opentelemetry import Instrumentor

exporter = InMemorySpanExporter()
provider = TracerProvider()
processor = BatchSpanProcessor(exporter)
provider.add_span_processor(processor)

instrumentor = Instrumentor()


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


@instrumentor.capture_lambda_handler(name=__name__)
def handler(event, context: LambdaContext):
    return {"statusCode": 200, "body": event["body"]}


@instrumentor.capture_method
def process_body(body: str) -> str:
    return body.upper()


@instrumentor.capture_lambda_handler
def handler_with_method(event, context: LambdaContext):
    return {"statusCode": 200, "body": process_body(event["body"])}


class TestCaptureHandler:
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


class TestCaptureMethod:
    def test_child_span_is_created(self, lambda_context: LambdaContext):
        handler_with_method({"body": "hello"}, lambda_context)

        spans = exporter.get_finished_spans()
        assert len(spans) == 2

        child_span = spans[0]
        parent_span = spans[1]

        assert child_span.name == "process_body"
        assert parent_span.name == "handler_with_method"
        assert child_span.parent.span_id == parent_span.context.span_id

    def test_child_span_status_ok(self, lambda_context: LambdaContext):
        handler_with_method({"body": "hello"}, lambda_context)

        spans = exporter.get_finished_spans()
        child_span = spans[0]
        assert child_span.status.status_code.name == "OK"

    def test_child_span_exception_is_recorded(self, lambda_context: LambdaContext):
        @instrumentor.capture_method
        def failing_method():
            raise RuntimeError("something went wrong")

        @instrumentor.capture_lambda_handler
        def handler_with_failing_method(event, context: LambdaContext):
            return failing_method()

        with pytest.raises(RuntimeError):
            handler_with_failing_method({}, lambda_context)

        spans = exporter.get_finished_spans()
        assert len(spans) == 2

        child_span = spans[0]
        assert child_span.status.status_code.name == "ERROR"

        event = child_span.events[0]
        assert event.name == "exception"

        attrs = dict(event.attributes) if event.attributes else {}
        assert attrs["exception.type"] == "RuntimeError"
        assert attrs["exception.message"] == "something went wrong"

    def test_method_return_value(self, lambda_context: LambdaContext):
        response = handler_with_method({"body": "hello"}, lambda_context)

        assert response["statusCode"] == 200
        assert response["body"] == "HELLO"
