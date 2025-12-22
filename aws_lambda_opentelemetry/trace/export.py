import base64
import enum
import gzip
import logging
import os
import threading
import zlib
from collections.abc import Sequence
from io import BytesIO
from typing import Any

from opentelemetry.exporter.otlp.proto.common.trace_encoder import encode_spans
from opentelemetry.sdk.environment_variables import (
    OTEL_EXPORTER_OTLP_COMPRESSION,
    OTEL_EXPORTER_OTLP_TRACES_COMPRESSION,
)
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SpanExporter,
    SpanExportResult,
)
from uuid_utils import uuid7

logger = logging.getLogger(__name__)


class Compression(enum.Enum):
    NoCompression = "none"
    Deflate = "deflate"
    Gzip = "gzip"

    @classmethod
    def from_env(cls) -> "Compression":
        compression = (
            os.getenv(
                OTEL_EXPORTER_OTLP_TRACES_COMPRESSION,
                os.getenv(OTEL_EXPORTER_OTLP_COMPRESSION, "none"),
            )
            .lower()
            .strip()
        )
        return Compression(compression)


class Base64SpanSerializer:
    def __init__(self, compression: Compression):
        self._compression = compression

    def serialize(self, spans: Sequence[ReadableSpan]) -> str:
        encoded_spans = encode_spans(spans)
        data = encoded_spans.SerializeToString()

        if self._compression == Compression.Gzip:
            gzip_data = BytesIO()
            with gzip.GzipFile(fileobj=gzip_data, mode="w") as gzip_stream:
                gzip_stream.write(data)
            data = gzip_data.getvalue()
        elif self._compression == Compression.Deflate:
            data = zlib.compress(data)

        compressed_serialized_spans = base64.b64encode(data)
        return compressed_serialized_spans.decode("utf-8")


class SQSTraceExporter(SpanExporter):
    """
    Implements OpenTelemetry SpanExporter interface
    which can be used in combination with a SpanProcessor
    to publish traces to Amazon SQS.

    ```
    provider = TracerProvider()
    processor = SimpleSpanProcessor(SQSTraceExporter())
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    ```
    """

    def __init__(
        self,
        queue_url: str,
        sqs_client: Any,
        compression: Compression | None = None,
    ) -> None:
        self._compression = compression or Compression.from_env()
        self._serializer = Base64SpanSerializer(self._compression)
        self._queue_url = queue_url
        self._sqs_client = sqs_client
        self._shutdown_in_progress = threading.Event()
        self._shutdown = False

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        """
        Exports spans to SQS in batches when the batch size is reached.
        """
        if self._shutdown:
            logger.warning("Exporter already shutdown, ignoring batch")
            return SpanExportResult.FAILURE

        entries = []
        for span in spans:
            serialized_span = self._serializer.serialize([span])
            id_ = str(span.context.span_id) if span.context else uuid7().hex
            entries.append({"Id": id_, "MessageBody": serialized_span})

        try:
            self._sqs_client.send_message_batch(
                QueueUrl=self._queue_url, Entries=entries
            )
            return SpanExportResult.SUCCESS
        except Exception as exc:
            logger.exception(f"Unexpected error exporting spans: {exc}")
            return SpanExportResult.FAILURE

    def shutdown(self) -> None:
        """Flush remaining spans before shutdown."""
        if self._shutdown:
            logger.warning("Exporter already shutdown, ignoring call")
            return

        self._shutdown = True
        self._shutdown_in_progress.set()
        self._sqs_client.close()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        """Nothing is buffered in this exporter, so this method does nothing."""
        return True


class SQSBatchSpanProcessor(BatchSpanProcessor):
    """
    BatchSpanProcessor configured for SQS limits.

    Automatically sets max_export_batch_size to 10 (SQS batch limit).

    ```
    provider = TracerProvider()
    exporter = SQSTraceExporter(queue_url="your-sqs-queue-url")
    processor = SQSBatchSpanProcessor(exporter)
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    ```
    """

    MAX_SQS_BATCH_SIZE = 10

    def __init__(
        self,
        span_exporter: SpanExporter,
        max_export_batch_size: int = MAX_SQS_BATCH_SIZE,
        **kwargs,
    ) -> None:
        assert max_export_batch_size <= self.MAX_SQS_BATCH_SIZE
        super().__init__(
            span_exporter=span_exporter,
            max_export_batch_size=max_export_batch_size,
            **kwargs,
        )
