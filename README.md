# AWS Lambda OpenTelemetry

A reliable OpenTelemetry delivery stack for AWS Lambda.

The stack is designed to combine upstream OpenTelemetry instrumentation with
Lambda-aware exporters, Collector components, and AWS infrastructure. It
explores how completed telemetry can leave Lambda with explicit latency,
reliability, and cost trade-offs. Upstream
[`opentelemetry-instrumentation-aws-lambda`](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/aws_lambda/aws_lambda.html)
creates Lambda spans; this project focuses on delivery paths such as SQS,
direct OTLP, and CloudWatch Logs.

## Project status

The package is alpha software. Today it provides an SQS trace exporter and a
batch span processor configured for SQS limits. The receiver, invocation-level
batch envelope, direct-OTLP reference deployment, and CloudWatch Logs path are
planned work, not released features.

| Capability | Status |
| --- | --- |
| SQS trace exporter | Available, experimental; currently one SQS message per span |
| SQS Collector receiver | Planned |
| Direct OTLP reference path | Planned |
| CloudWatch Logs delivery path | Planned |

## Installation

Install this project with the upstream Lambda instrumentation and your AWS SDK:

```bash
pip install aws-lambda-opentelemetry \
  opentelemetry-instrumentation-aws-lambda boto3
```

## Recommended setup

Use OpenTelemetry contrib to instrument the Lambda handler and configure this
project only as the delivery component:

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

The execution role needs permission to call `sqs:SendMessageBatch` for the
selected queue. The current exporter sends base64-encoded OTLP protobuf and
does not include a Collector receiver; consumers must understand the current
wire format. This limitation is why the exporter is marked experimental.

## Scope

This project does not replace general Lambda, boto3, framework, or client
instrumentation. Those belong in the OpenTelemetry Python ecosystem. Its goal
is to compare and implement Lambda-aware telemetry delivery modes while making
their acknowledgement, retry, duplication, loss, and latency behavior clear.
