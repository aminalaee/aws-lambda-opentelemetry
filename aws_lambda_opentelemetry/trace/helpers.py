from functools import wraps

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace import (
    SpanKind,
    Status,
    StatusCode,
    get_tracer,
    get_tracer_provider,
)

from aws_lambda_opentelemetry.typing.context import LambdaContext
from aws_lambda_opentelemetry.utils import set_lambda_handler_attributes


def instrument_handler(**kwargs):
    """
    Decorate a Lambda handler function to automatically create and manage
    an OpenTelemetry span for the function invocation.

    Accepts all keyword arguments from Tracer.start_as_current_span():

    :param name: Span name (defaults to function name if not provided)
    :param context: Parent span context
    :param kind: SpanKind (defaults to SERVER if not provided)
    :param attributes: Initial span attributes dict
    :param links: Span links
    :param start_time: Span start timestamp
    :param record_exception: Whether to record exceptions (default True)
    :param set_status_on_exception: Whether to set error status on exception (default True)
    :param end_on_exit: Whether to end the span on exit (default True)
    :return: The decorated handler function.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(event: dict, context: LambdaContext):
            tracer = get_tracer(func.__module__)
            provider = get_tracer_provider()
            kwargs.setdefault("name", func.__name__)
            kwargs.setdefault("kind", SpanKind.SERVER)

            if not isinstance(provider, TracerProvider):
                raise ValueError("Tracer provider not initialized.")

            try:
                with tracer.start_as_current_span(**kwargs) as span:
                    try:
                        response = func(event, context)
                        span.set_status(Status(StatusCode.OK))
                        return response
                    except Exception as exc:
                        span.set_status(Status(StatusCode.ERROR))
                        span.record_exception(exc)
                        raise
                    finally:
                        set_lambda_handler_attributes(event, context, span)
            finally:
                provider.force_flush()

        return wrapper

    return decorator
