import enum
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
from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MESSAGING_BATCH_MESSAGE_COUNT,
    MESSAGING_DESTINATION_NAME,
    MESSAGING_OPERATION,
    MESSAGING_SYSTEM,
    MessagingOperationTypeValues,
)
from opentelemetry.trace import Span

from aws_lambda_opentelemetry import constants
from aws_lambda_opentelemetry.typing.context import LambdaContext

_is_cold_start = True


class AwsDataSource(enum.Enum):
    API_GATEWAY = "aws.api_gateway"
    HTTP_API = "aws.http_api"
    ELB = "aws.elb"
    SQS = "aws.sqs"
    SNS = "aws.sns"
    S3 = "aws.s3"
    DYNAMODB = "aws.dynamodb"
    KINESIS = "aws.kinesis"
    EVENT_BRIDGE = "aws.event_bridge"
    CLOUDWATCH_LOGS = "aws.cloudwatch_logs"
    OTHER = "aws.other"


def set_handler_attributes(event: dict, context: LambdaContext, span: Span):
    """
    Set standard AWS Lambda attributes on the given span.
    """

    data_source_mapper = DataSourceAttributeMapper(event)

    span.set_attributes(data_source_mapper.attributes)
    span.set_attributes(
        {
            FAAS_INVOCATION_ID: context.aws_request_id,
            FAAS_INVOKED_NAME: context.function_name,
            FAAS_INVOKED_REGION: context.region,
            FAAS_INVOKED_PROVIDER: FaasInvokedProviderValues.AWS.value,
            FAAS_MAX_MEMORY: context.memory_limit_in_mb,
            FAAS_VERSION: context.function_version,
            FAAS_COLDSTART: _check_cold_start(),
            FAAS_TRIGGER: data_source_mapper.faas_trigger.value,
            CLOUD_RESOURCE_ID: context.invoked_function_arn,
        }
    )


class DataSourceAttributeMapper:
    def __init__(self, event: dict):
        self.event = event
        self.data_source, self.faas_trigger = self.get_sources()

    @property
    def attributes(self) -> dict:
        if self.data_source == AwsDataSource.SQS:
            return self._get_sqs_attributes()
        return {}

    def get_sources(self) -> tuple[AwsDataSource, FaasTriggerValues]:
        # HTTP triggers
        if "requestContext" in self.event:
            if "apiId" in self.event["requestContext"]:
                return (AwsDataSource.API_GATEWAY, FaasTriggerValues.HTTP)

            if "http" in self.event["requestContext"]:
                return (AwsDataSource.HTTP_API, FaasTriggerValues.HTTP)

            if "elb" in self.event["requestContext"]:
                return (AwsDataSource.ELB, FaasTriggerValues.HTTP)

        # EventBridge
        if "source" in self.event and "detail-type" in self.event:
            if self.event["detail-type"] == "Scheduled Event":
                return (AwsDataSource.EVENT_BRIDGE, FaasTriggerValues.TIMER)
            return (AwsDataSource.EVENT_BRIDGE, FaasTriggerValues.PUBSUB)

        # SNS/SQS/S3/DynamoDB/Kinesis
        if "Records" in self.event and len(self.event["Records"]) > 0:
            record = self.event["Records"][0]
            event_source = record.get("eventSource")

            if event_source == "aws:sns":
                return (AwsDataSource.SNS, FaasTriggerValues.PUBSUB)

            if event_source == "aws:sqs":
                return (AwsDataSource.SQS, FaasTriggerValues.PUBSUB)

            if event_source == "aws:s3":
                return (AwsDataSource.S3, FaasTriggerValues.DATASOURCE)

            if event_source == "aws:dynamodb":
                return (AwsDataSource.DYNAMODB, FaasTriggerValues.DATASOURCE)

            if event_source == "aws:kinesis":
                return (AwsDataSource.KINESIS, FaasTriggerValues.DATASOURCE)

        # CloudWatch Logs
        if "awslogs" in self.event and "data" in self.event["awslogs"]:
            return (AwsDataSource.CLOUDWATCH_LOGS, FaasTriggerValues.DATASOURCE)

        return (AwsDataSource.OTHER, FaasTriggerValues.OTHER)

    def _get_sqs_attributes(self) -> dict:
        records = self.event.get("Records", [])
        message_count = len(records)
        queue_arn = records[0].get("eventSourceARN", "") if message_count > 0 else ""
        queue_name = queue_arn.split(":")[-1]

        return {
            MESSAGING_SYSTEM: self.data_source.value,
            MESSAGING_OPERATION: MessagingOperationTypeValues.RECEIVE.value,
            MESSAGING_BATCH_MESSAGE_COUNT: message_count,
            MESSAGING_DESTINATION_NAME: queue_name,
            CLOUD_RESOURCE_ID: queue_arn,
        }


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
