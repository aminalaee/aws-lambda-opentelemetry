import enum
import os
from abc import ABC, abstractmethod

from aws_lambda_powertools.utilities.typing import LambdaContext
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
from opentelemetry.semconv._incubating.attributes.http_attributes import (
    HTTP_REQUEST_BODY_SIZE,
)
from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MESSAGING_BATCH_MESSAGE_COUNT,
    MESSAGING_DESTINATION_NAME,
    MESSAGING_OPERATION,
    MESSAGING_SYSTEM,
    MessagingOperationTypeValues,
)
from opentelemetry.semconv.attributes.http_attributes import (
    HTTP_REQUEST_METHOD,
    HTTP_ROUTE,
)
from opentelemetry.semconv.attributes.network_attributes import (
    NETWORK_PROTOCOL_NAME,
    NETWORK_PROTOCOL_VERSION,
)
from opentelemetry.semconv.attributes.url_attributes import URL_FULL
from opentelemetry.semconv.attributes.user_agent_attributes import USER_AGENT_ORIGINAL

from aws_lambda_opentelemetry import constants

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


class AttributeExtractor(ABC):
    """Base class for AWS service-specific attribute extractors."""

    @property
    @abstractmethod
    def data_source(self) -> AwsDataSource:
        """Return the AWS data source this extractor handles."""
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def can_handle(self, event: dict) -> bool:
        """Determine if this extractor can handle the given event."""
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def get_attributes(self, event: dict, context: LambdaContext) -> dict:
        """Extract related attributes from the event and context."""
        raise NotImplementedError()  # pragma: no cover


class GenericAwsExtractor(AttributeExtractor):
    @property
    def data_source(self) -> AwsDataSource:
        return AwsDataSource.OTHER

    def can_handle(self, event: dict) -> bool:
        return True

    def get_attributes(self, event: dict, context: LambdaContext) -> dict:
        return {
            FAAS_INVOCATION_ID: context.aws_request_id,
            FAAS_INVOKED_NAME: context.function_name,
            FAAS_INVOKED_REGION: os.getenv("AWS_DEFAULT_REGION"),
            FAAS_INVOKED_PROVIDER: FaasInvokedProviderValues.AWS.value,
            FAAS_MAX_MEMORY: context.memory_limit_in_mb,
            FAAS_VERSION: context.function_version,
            FAAS_COLDSTART: _check_cold_start(),
            CLOUD_RESOURCE_ID: context.invoked_function_arn,
        }


class HttpApiExtractor(AttributeExtractor):
    @property
    def data_source(self) -> AwsDataSource:
        return AwsDataSource.HTTP_API

    def can_handle(self, event: dict) -> bool:
        return "requestContext" in event and "http" in event["requestContext"]

    def get_attributes(self, event: dict, context: LambdaContext) -> dict:
        request_context = event.get("requestContext", {})
        http_context = request_context.get("http", {})
        protocol = http_context.get("protocol", "")

        return {
            FAAS_TRIGGER: FaasTriggerValues.HTTP.value,
            HTTP_REQUEST_METHOD: http_context.get("method", ""),
            HTTP_ROUTE: event.get("routeKey", ""),
            HTTP_REQUEST_BODY_SIZE: len(event.get("body", "") or ""),
            NETWORK_PROTOCOL_NAME: protocol.split("/")[0] if protocol else "",
            NETWORK_PROTOCOL_VERSION: protocol.split("/")[-1] if protocol else "",
            USER_AGENT_ORIGINAL: http_context.get("userAgent", ""),
            URL_FULL: http_context.get("path", ""),
        }


class ApiGatewayExtractor(AttributeExtractor):
    @property
    def data_source(self) -> AwsDataSource:
        return AwsDataSource.API_GATEWAY

    def can_handle(self, event: dict) -> bool:
        return (
            "requestContext" in event
            and "apiId" in event["requestContext"]
            and "http" not in event["requestContext"]
        )

    def get_attributes(self, event: dict, context: LambdaContext) -> dict:
        request_context = event.get("requestContext", {})
        headers = event.get("headers", {})
        protocol = request_context.get("protocol", "")

        return {
            FAAS_TRIGGER: FaasTriggerValues.HTTP.value,
            HTTP_REQUEST_METHOD: event.get("httpMethod", ""),
            HTTP_ROUTE: event.get("resource", ""),
            HTTP_REQUEST_BODY_SIZE: len(event.get("body", "") or ""),
            NETWORK_PROTOCOL_NAME: protocol.split("/")[0],
            NETWORK_PROTOCOL_VERSION: protocol.split("/")[-1],
            USER_AGENT_ORIGINAL: headers.get("User-Agent", ""),
            URL_FULL: event.get("path", ""),
        }


class ElbExtractor(AttributeExtractor):
    @property
    def data_source(self) -> AwsDataSource:
        return AwsDataSource.ELB

    def can_handle(self, event: dict) -> bool:
        return "requestContext" in event and "elb" in event["requestContext"]

    def get_attributes(self, event: dict, context: LambdaContext) -> dict:
        headers = event.get("headers", {})

        return {
            FAAS_TRIGGER: FaasTriggerValues.HTTP.value,
            HTTP_REQUEST_METHOD: event.get("httpMethod", ""),
            HTTP_ROUTE: event.get("path", ""),
            HTTP_REQUEST_BODY_SIZE: len(event.get("body", "") or ""),
            URL_FULL: event.get("path", ""),
            USER_AGENT_ORIGINAL: headers.get("user-agent", ""),
        }


class SqsExtractor(AttributeExtractor):
    @property
    def data_source(self) -> AwsDataSource:
        return AwsDataSource.SQS

    def can_handle(self, event: dict) -> bool:
        if "Records" not in event or len(event["Records"]) == 0:
            return False
        return event["Records"][0].get("eventSource") == "aws:sqs"

    def get_attributes(self, event: dict, context: LambdaContext) -> dict:
        records = event.get("Records", [])
        message_count = len(records)
        queue_arn = records[0].get("eventSourceARN", "") if message_count > 0 else ""
        queue_name = queue_arn.split(":")[-1]

        return {
            FAAS_TRIGGER: FaasTriggerValues.PUBSUB.value,
            CLOUD_RESOURCE_ID: queue_arn,
            MESSAGING_SYSTEM: self.data_source.value,
            MESSAGING_OPERATION: MessagingOperationTypeValues.RECEIVE.value,
            MESSAGING_BATCH_MESSAGE_COUNT: message_count,
            MESSAGING_DESTINATION_NAME: queue_name,
        }


class SnsExtractor(AttributeExtractor):
    @property
    def data_source(self) -> AwsDataSource:
        return AwsDataSource.SNS

    def can_handle(self, event: dict) -> bool:
        if "Records" not in event or len(event["Records"]) == 0:
            return False
        return event["Records"][0].get("eventSource") == "aws:sns"

    def get_attributes(self, event: dict, context: LambdaContext) -> dict:
        return {
            FAAS_TRIGGER: FaasTriggerValues.PUBSUB.value,
        }


class S3Extractor(AttributeExtractor):
    @property
    def data_source(self) -> AwsDataSource:
        return AwsDataSource.S3

    def can_handle(self, event: dict) -> bool:
        if "Records" not in event or len(event["Records"]) == 0:
            return False
        return event["Records"][0].get("eventSource") == "aws:s3"

    def get_attributes(self, event: dict, context: LambdaContext) -> dict:
        return {
            FAAS_TRIGGER: FaasTriggerValues.DATASOURCE.value,
        }


class DynamoDbExtractor(AttributeExtractor):
    @property
    def data_source(self) -> AwsDataSource:
        return AwsDataSource.DYNAMODB

    def can_handle(self, event: dict) -> bool:
        if "Records" not in event or len(event["Records"]) == 0:
            return False
        return event["Records"][0].get("eventSource") == "aws:dynamodb"

    def get_attributes(self, event: dict, context: LambdaContext) -> dict:
        return {
            FAAS_TRIGGER: FaasTriggerValues.DATASOURCE.value,
        }


class KinesisExtractor(AttributeExtractor):
    @property
    def data_source(self) -> AwsDataSource:
        return AwsDataSource.KINESIS

    def can_handle(self, event: dict) -> bool:
        if "Records" not in event or len(event["Records"]) == 0:
            return False
        return event["Records"][0].get("eventSource") == "aws:kinesis"

    def get_attributes(self, event: dict, context: LambdaContext) -> dict:
        return {
            FAAS_TRIGGER: FaasTriggerValues.DATASOURCE.value,
        }


class EventBridgeExtractor(AttributeExtractor):
    @property
    def data_source(self) -> AwsDataSource:
        return AwsDataSource.EVENT_BRIDGE

    def can_handle(self, event: dict) -> bool:
        return "source" in event and "detail-type" in event

    def get_attributes(self, event: dict, context: LambdaContext) -> dict:
        detail_type = event.get("detail-type", "")
        trigger_type = (
            FaasTriggerValues.TIMER.value
            if detail_type == "Scheduled Event"
            else FaasTriggerValues.PUBSUB.value
        )

        return {
            FAAS_TRIGGER: trigger_type,
        }


class CloudWatchLogsExtractor(AttributeExtractor):
    @property
    def data_source(self) -> AwsDataSource:
        return AwsDataSource.CLOUDWATCH_LOGS

    def can_handle(self, event: dict) -> bool:
        return "awslogs" in event and "data" in event["awslogs"]

    def get_attributes(self, event: dict, context: LambdaContext) -> dict:
        return {
            FAAS_TRIGGER: FaasTriggerValues.DATASOURCE.value,
        }


class AwsAttributesExtractor:
    _EXTRACTORS: list[AttributeExtractor] = [
        GenericAwsExtractor(),
        HttpApiExtractor(),
        ApiGatewayExtractor(),
        ElbExtractor(),
        SqsExtractor(),
        SnsExtractor(),
        S3Extractor(),
        DynamoDbExtractor(),
        KinesisExtractor(),
        EventBridgeExtractor(),
        CloudWatchLogsExtractor(),
    ]

    def __init__(self, event: dict, context: LambdaContext) -> None:
        self.event = event
        self.context = context
        self.span = trace.get_current_span()

    def add_attributes(self) -> None:
        """
        Generic method which inspects given event/context
        and tries to add as much metadata to the current span as it can.
        """
        for extractor in self._EXTRACTORS:
            if extractor.can_handle(self.event):
                attributes = extractor.get_attributes(self.event, self.context)
                self.span.set_attributes(attributes)


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
