import os

from opentelemetry.semconv._incubating.attributes.cloud_attributes import (
    CLOUD_RESOURCE_ID,
)
from opentelemetry.semconv._incubating.attributes.faas_attributes import (
    FAAS_COLDSTART,
    FAAS_INVOCATION_ID,
    FAAS_INVOKED_NAME,
    FAAS_INVOKED_PROVIDER,
    FAAS_INVOKED_REGION,
    FAAS_MAX_MEMORY,
    FAAS_TRIGGER,
    FAAS_VERSION,
    FaasInvokedProviderValues,
    FaasTriggerValues,
)
from opentelemetry.trace import Span

from aws_lambda_opentelemetry import constants
from aws_lambda_opentelemetry.typing.context import LambdaContext

_is_cold_start = True


def set_lambda_handler_attributes(event: dict, context: LambdaContext, span: Span):
    """
    Set standard AWS Lambda attributes on the given span.
    """

    span.set_attributes(
        {
            FAAS_INVOCATION_ID: context.aws_request_id,
            FAAS_INVOKED_NAME: context.function_name,
            FAAS_INVOKED_REGION: context.region,
            FAAS_INVOKED_PROVIDER: FaasInvokedProviderValues.AWS.value,
            FAAS_MAX_MEMORY: context.memory_limit_in_mb,
            FAAS_VERSION: context.function_version,
            FAAS_COLDSTART: _check_cold_start(),
            FAAS_TRIGGER: get_lambda_datasource(event).value,
            CLOUD_RESOURCE_ID: context.invoked_function_arn,
        }
    )


def get_lambda_datasource(event: dict) -> FaasTriggerValues:
    """
    Extract the data source from the Lambda event.
    """

    # HTTP triggers
    http_keys = ["apiId", "http", "elb"]
    if "requestContext" in event:
        if any(key in event["requestContext"] for key in http_keys):
            return FaasTriggerValues.HTTP

    # EventBridge
    if "source" in event and "detail-type" in event:
        if event["detail-type"] == "Scheduled Event":
            return FaasTriggerValues.TIMER
        return FaasTriggerValues.PUBSUB

    # SNS/SQS/S3/DynamoDB/Kinesis
    if "Records" in event and len(event["Records"]) > 0:
        record = event["Records"][0]
        event_source = record.get("eventSource")

        if event_source in {"aws:sns", "aws:sqs"}:
            return FaasTriggerValues.PUBSUB

        if event_source in {"aws:s3", "aws:dynamodb", "aws:kinesis"}:
            return FaasTriggerValues.DATASOURCE

    # CloudWatch Logs
    if "awslogs" in event and "data" in event["awslogs"]:
        return FaasTriggerValues.DATASOURCE

    return FaasTriggerValues.OTHER


def _check_cold_start() -> bool:
    global _is_cold_start

    initialization_type = os.getenv(constants.LAMBDA_INITIALIZATION_TYPE)

    if initialization_type == "provisioned-concurrency":
        _is_cold_start = False
        return False

    if not _is_cold_start:
        return False

    _is_cold_start = False
    return True
