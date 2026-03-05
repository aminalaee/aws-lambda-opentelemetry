# Exporters

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

::: aws_lambda_opentelemetry.trace.export.Base64SpanSerializer
    handler: python

::: aws_lambda_opentelemetry.trace.export.Compression
    handler: python
