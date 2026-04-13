# AWS Lambda OpenTelemetry

OpenTelemetry instrumentation for AWS Lambda functions.

---

## Installation

```bash
pip install aws-lambda-opentelemetry
```

## Quick Start

```python
from aws_lambda_opentelemetry import Instrumentor

instrumentor = Instrumentor(service="my-service")

@instrumentor.capture_lambda_handler
def handler(event, context):
    result = process(event)
    return {"statusCode": 200, "body": result}

@instrumentor.capture_method
def process(event):
    return event["body"].upper()
```

## Features

- **`capture_lambda_handler`** — Wraps Lambda handlers with automatic span creation, AWS attribute extraction, and force flush
- **`capture_method`** — Wraps regular functions/methods as child spans
- **Automatic attribute extraction** — Detects API Gateway, HTTP API, ALB, SQS, SNS, S3, DynamoDB, Kinesis, EventBridge, and CloudWatch Logs events
- **Cold start tracking** — Automatically tracks cold start via `faas.coldstart`
- **SQS trace exporter** — Export spans to SQS with optional gzip/deflate compression

## Supported Event Sources

| Source | Detection | Attributes |
|--------|-----------|------------|
| API Gateway (REST) | `requestContext.apiId` | HTTP method, route, protocol, user agent, body size |
| HTTP API (v2) | `requestContext.http` | HTTP method, route, protocol, user agent, body size |
| ALB/ELB | `requestContext.elb` | HTTP method, path, user agent, body size |
| SQS | `eventSource: aws:sqs` | Queue name, message count, messaging system |
| SNS | `eventSource: aws:sns` | Trigger type |
| S3 | `eventSource: aws:s3` | Trigger type |
| DynamoDB Streams | `eventSource: aws:dynamodb` | Trigger type |
| Kinesis | `eventSource: aws:kinesis` | Trigger type |
| EventBridge | `source` + `detail-type` | Trigger type (timer or pubsub) |
| CloudWatch Logs | `awslogs.data` | Trigger type |

## Usage with Decorators

### Lambda Handler

Both bare decorator and decorator-with-arguments styles are supported:

```python
# Bare decorator
@instrumentor.capture_lambda_handler
def handler(event, context):
    ...

# With arguments
@instrumentor.capture_lambda_handler(name="my-handler")
def handler(event, context):
    ...
```

### Methods and Functions

```python
@instrumentor.capture_method
def my_function(x, y):
    return x + y

@instrumentor.capture_method(name="custom-span-name")
def another_function():
    ...
```

Child spans created by `capture_method` are automatically linked to the parent Lambda handler span.
