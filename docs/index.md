# AWS Lambda OpenTelemetry

This is a reliable OpenTelemetry **delivery stack** for AWS Lambda. It is
designed to combine upstream instrumentation with Lambda-aware exporters,
Collector components, and AWS infrastructure.
Upstream [`opentelemetry-instrumentation-aws-lambda`](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/aws_lambda/aws_lambda.html)
is the default way to create Lambda telemetry; this project explores how to
deliver completed telemetry with clear latency and reliability trade-offs.

## Current status

The released Python surface currently contains:

- an experimental `SQSTraceExporter`;
- an `SQSBatchSpanProcessor` constrained to SQS's ten-entry batch limit; and
- serialization and compression support for the current SQS wire format.

The project does **not** yet ship an SQS Collector receiver, an invocation-level
transport envelope, a direct-OTLP deployment, or a CloudWatch Logs delivery
path. These are roadmap items.

## Recommended setup

Install upstream Lambda instrumentation alongside this package:

```bash
pip install aws-lambda-opentelemetry \
  opentelemetry-instrumentation-aws-lambda boto3
```

Then let upstream instrumentation create handler spans and use this package as
the exporter:

```python
import boto3
from opentelemetry import trace
from opentelemetry.instrumentation.aws_lambda import AwsLambdaInstrumentor
from opentelemetry.sdk.trace import TracerProvider

from aws_lambda_opentelemetry.trace.export import (
    SQSBatchSpanProcessor,
    SQSTraceExporter,
)

provider = TracerProvider()
exporter = SQSTraceExporter(
    queue_url="https://sqs.eu-west-1.amazonaws.com/123456789012/telemetry",
    sqs_client=boto3.client("sqs"),
)
provider.add_span_processor(SQSBatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)


def handler(event, context):
    return {"statusCode": 200, "body": "ok"}


AwsLambdaInstrumentor().instrument(tracer_provider=provider)
```

The Lambda execution role must allow `sqs:SendMessageBatch`. The current
exporter creates one SQS message per span and uses a project-specific wire
format for which no Collector receiver is included yet.
