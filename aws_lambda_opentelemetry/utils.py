import enum
import os

from opentelemetry import trace
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


class AwsAttributesMapper:
    def __init__(self, event: dict, context: LambdaContext) -> None:
        self.event = event
        self.context = context
        self.span = trace.get_current_span()
        self.data_source = self._get_aws_data_source()
        self.faas_trigger = self._get_faas_trigger()

    def add_attributes(self) -> None:
        """
        Generic method which inspects given event/context
        and tries to add as much metadata to the current span as it can.
        """
        self._add_aws_attributes()

        match self.data_source:
            case AwsDataSource.SQS:
                self._add_sqs_attributes()
            case _:
                ...

    def _get_aws_data_source(self) -> AwsDataSource:
        # HTTP triggers
        if "requestContext" in self.event:
            if "apiId" in self.event["requestContext"]:
                return AwsDataSource.API_GATEWAY

            if "http" in self.event["requestContext"]:
                return AwsDataSource.HTTP_API

            if "elb" in self.event["requestContext"]:
                return AwsDataSource.ELB

        # EventBridge
        if "source" in self.event and "detail-type" in self.event:
            return AwsDataSource.EVENT_BRIDGE

        # SNS/SQS/S3/DynamoDB/Kinesis
        if "Records" in self.event and len(self.event["Records"]) > 0:
            record = self.event["Records"][0]
            event_source = record.get("eventSource")

            if event_source == "aws:sns":
                return AwsDataSource.SNS

            if event_source == "aws:sqs":
                return AwsDataSource.SQS

            if event_source == "aws:s3":
                return AwsDataSource.S3

            if event_source == "aws:dynamodb":
                return AwsDataSource.DYNAMODB

            if event_source == "aws:kinesis":
                return AwsDataSource.KINESIS

        # CloudWatch Logs
        if "awslogs" in self.event and "data" in self.event["awslogs"]:
            return AwsDataSource.CLOUDWATCH_LOGS

        return AwsDataSource.OTHER

    def _get_faas_trigger(self) -> FaasTriggerValues:
        if self.data_source in {
            AwsDataSource.API_GATEWAY,
            AwsDataSource.HTTP_API,
            AwsDataSource.ELB,
        }:
            return FaasTriggerValues.HTTP

        if self.data_source == AwsDataSource.EVENT_BRIDGE:
            if self.event["detail-type"] == "Scheduled Event":
                return FaasTriggerValues.TIMER
            return FaasTriggerValues.PUBSUB

        if self.data_source in {AwsDataSource.SQS, AwsDataSource.SNS}:
            return FaasTriggerValues.PUBSUB

        if self.data_source in {
            AwsDataSource.S3,
            AwsDataSource.DYNAMODB,
            AwsDataSource.KINESIS,
            AwsDataSource.CLOUDWATCH_LOGS,
        }:
            return FaasTriggerValues.DATASOURCE

        return FaasTriggerValues.OTHER

    def _add_aws_attributes(self) -> None:
        self.span.set_attributes(
            {
                FAAS_INVOCATION_ID: self.context.aws_request_id,
                FAAS_INVOKED_NAME: self.context.function_name,
                FAAS_INVOKED_REGION: self.context.region,
                FAAS_INVOKED_PROVIDER: FaasInvokedProviderValues.AWS.value,
                FAAS_MAX_MEMORY: self.context.memory_limit_in_mb,
                FAAS_VERSION: self.context.function_version,
                FAAS_COLDSTART: _check_cold_start(),
                FAAS_TRIGGER: self.faas_trigger.value,
                CLOUD_RESOURCE_ID: self.context.invoked_function_arn,
            }
        )

    def _add_sqs_attributes(self) -> None:
        records = self.event.get("Records", [])
        message_count = len(records)
        queue_arn = records[0].get("eventSourceARN", "") if message_count > 0 else ""
        queue_name = queue_arn.split(":")[-1]

        self.span.set_attributes(
            {
                MESSAGING_SYSTEM: self.data_source.value,
                MESSAGING_OPERATION: MessagingOperationTypeValues.RECEIVE.value,
                MESSAGING_BATCH_MESSAGE_COUNT: message_count,
                MESSAGING_DESTINATION_NAME: queue_name,
                CLOUD_RESOURCE_ID: queue_arn,
            }
        )


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
