from collections.abc import Callable
from functools import wraps
from typing import Any

from aws_lambda_powertools.utilities.typing import LambdaContext
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace import (
    SpanKind,
    Status,
    StatusCode,
    get_tracer,
    get_tracer_provider,
)

from aws_lambda_opentelemetry.utils import AwsAttributesExtractor


class Instrumentor:
    def __init__(self, service: str | None = None):
        self._service = service

    def _get_tracer(self, func: Callable) -> Any:
        return get_tracer(self._service or func.__module__)

    def capture_lambda_handler(self, func: Callable | None = None, **kwargs: Any):
        if func is None:
            return lambda f: self.capture_lambda_handler(f, **kwargs)

        @wraps(func)
        def wrapper(event: dict, context: LambdaContext):
            tracer = self._get_tracer(func)
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
                        extractor = AwsAttributesExtractor(event, context)
                        extractor.add_attributes()
            finally:
                provider.force_flush()

        return wrapper

    def capture_method(self, method: Callable | None = None, **kwargs: Any):
        if method is None:
            return lambda m: self.capture_method(m, **kwargs)

        @wraps(method)
        def wrapper(*args: Any, **func_kwargs: Any):
            tracer = self._get_tracer(method)
            kwargs.setdefault("name", method.__qualname__)

            with tracer.start_as_current_span(**kwargs) as span:
                try:
                    response = method(*args, **func_kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return response
                except Exception as exc:
                    span.set_status(Status(StatusCode.ERROR))
                    span.record_exception(exc)
                    raise

        return wrapper
