# Exporters

The SQS exporter is an experimental component of the delivery stack. It
currently serializes each span as a separate base64-encoded OTLP protobuf
request and sends up to ten messages through `SendMessageBatch`. The project
does not yet include a Collector receiver for this wire format.

## SQS Trace Exporter

::: aws_lambda_opentelemetry.trace.export.SQSTraceExporter
    handler: python
    options:
      members:
        - __init__
        - export
        - shutdown
        - force_flush

## SQS Batch Span Processor

::: aws_lambda_opentelemetry.trace.export.SQSBatchSpanProcessor
    handler: python

## Serialization

These serialization types describe the current experimental SQS format. They
are not a stable, language-neutral envelope yet.

::: aws_lambda_opentelemetry.trace.export.Base64SpanSerializer
    handler: python

::: aws_lambda_opentelemetry.trace.export.Compression
    handler: python
