from unittest.mock import MagicMock

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    SpanExportResult,
)

from aws_lambda_opentelemetry.trace.export import (
    Base64SpanSerializer,
    Compression,
    SQSBatchSpanProcessor,
    SQSTraceExporter,
)
from tests.utils import generate_span


class TestSpanSerializer:
    @pytest.mark.parametrize(
        "compression, expected_length",
        [
            (Compression.NoCompression, 276),
            (Compression.Gzip, 248),
            (Compression.Deflate, 232),
        ],
    )
    def test_base64_span_serializer(self, compression, expected_length):
        serializer = Base64SpanSerializer(compression)
        spans = [generate_span()]
        result = serializer.serialize(spans)
        assert isinstance(result, str)
        assert len(result) == expected_length

    @pytest.mark.parametrize(
        "compression_name, expected_compression",
        [
            ("gzip", Compression.Gzip),
            ("deflate", Compression.Deflate),
            ("none", Compression.NoCompression),
        ],
    )
    def test_compression_from_env_var(
        self, monkeypatch, compression_name, expected_compression
    ):
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_COMPRESSION", compression_name)
        assert Compression.from_env() == expected_compression


class TestSqsTraceExporter:
    QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789/test-queue"

    def test_export_when_shutdown_is_called(self, mock_sqs_client):
        exporter = SQSTraceExporter(
            queue_url=self.QUEUE_URL,
            sqs_client=mock_sqs_client,
        )
        exporter.shutdown()

        result = exporter.export([])
        assert result == SpanExportResult.FAILURE

    def test_export_sends_messages_to_sqs(self, mock_sqs_client):
        exporter = SQSTraceExporter(
            queue_url=self.QUEUE_URL,
            sqs_client=mock_sqs_client,
        )

        spans = [generate_span() for _ in range(2)]
        result = exporter.export(spans)
        assert result == SpanExportResult.SUCCESS

        response = mock_sqs_client.receive_message(
            QueueUrl=self.QUEUE_URL,
            MaxNumberOfMessages=10,
        )
        messages = response.get("Messages", [])
        assert len(messages) == 2

    def test_export_handles_sqs_client_exception(self):
        exporter = SQSTraceExporter(
            queue_url=self.QUEUE_URL,
            sqs_client=object(),
        )

        spans = [generate_span() for _ in range(2)]
        result = exporter.export(spans)

        assert result == SpanExportResult.FAILURE

    def test_export_shutdown(self, mock_sqs_client):
        mock_sqs_client.close = MagicMock()
        exporter = SQSTraceExporter(
            queue_url=self.QUEUE_URL,
            sqs_client=mock_sqs_client,
        )

        exporter.shutdown()

        mock_sqs_client.close.assert_called_once()
        assert exporter._shutdown is True
        assert exporter._shutdown_in_progress.is_set()

    def test_export_shutdown_successive_calls(self, mock_sqs_client):
        mock_sqs_client.close = MagicMock()
        exporter = SQSTraceExporter(
            queue_url=self.QUEUE_URL,
            sqs_client=mock_sqs_client,
        )

        exporter.shutdown()
        exporter.shutdown()

        mock_sqs_client.close.assert_called_once()
        assert exporter._shutdown is True
        assert exporter._shutdown_in_progress.is_set()

    def test_export_force_flush(self, mock_sqs_client):
        exporter = SQSTraceExporter(
            queue_url=self.QUEUE_URL,
            sqs_client=mock_sqs_client,
        )

        result = exporter.force_flush()
        assert result is True


class TestSqsBatchSpanProcessor:
    QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789/test-queue"

    def test_sqs_batch_span_processor_exports_in_batches(self, mock_sqs_client):
        exporter = SQSTraceExporter(
            queue_url=self.QUEUE_URL,
            sqs_client=mock_sqs_client,
        )
        processor = SQSBatchSpanProcessor(span_exporter=exporter)
        provider = TracerProvider()
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)

        tracer = trace.get_tracer("test-sqs-batch-span-processor")
        for i in range(15):
            with tracer.start_as_current_span(f"test-span-{i}"):
                ...

        response = mock_sqs_client.receive_message(
            QueueUrl=self.QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
        )
        assert len(response.get("Messages", [])) == 10

        processor.shutdown()

        response = mock_sqs_client.receive_message(
            QueueUrl=self.QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
        )
        assert len(response.get("Messages", [])) == 5
