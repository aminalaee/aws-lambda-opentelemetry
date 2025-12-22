import random

from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.trace import SpanContext


def generate_span() -> ReadableSpan:
    span_context = SpanContext(
        trace_id=random.getrandbits(128),
        span_id=random.getrandbits(64),
        is_remote=False,
    )
    return ReadableSpan(name="test-span", context=span_context)
