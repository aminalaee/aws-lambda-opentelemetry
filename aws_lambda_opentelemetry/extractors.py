import enum
import os
from abc import ABC, abstractmethod

from aws_lambda_powertools.utilities.data_classes import (
    ALBEvent,
    APIGatewayProxyEvent,
    APIGatewayProxyEventV2,
    CloudWatchLogsEvent,
    DynamoDBStreamEvent,
    EventBridgeEvent,
    KinesisStreamEvent,
    S3Event,
    SNSEvent,
    SQSEvent,
)
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
        ...  # pragma: no cover

    @abstractmethod
    def extract(self, event: dict, context: LambdaContext) -> dict | None:
        """Extract attributes if this extractor can handle the event, otherwise return None."""
        ...  # pragma: no cover


class GenericAwsExtractor(AttributeExtractor):
    @property
    def data_source(self) -> AwsDataSource:
        return AwsDataSource.OTHER

    def extract(self, event: dict, context: LambdaContext) -> dict | None:
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

    def extract(self, event: dict, context: LambdaContext) -> dict | None:
        try:
            api_event = APIGatewayProxyEventV2(event)
            http = api_event.request_context.http
            protocol = http.protocol or ""

            return {
                FAAS_TRIGGER: FaasTriggerValues.HTTP.value,
                HTTP_REQUEST_METHOD: http.method or "",
                HTTP_ROUTE: api_event.route_key or "",
                HTTP_REQUEST_BODY_SIZE: len(api_event.body or ""),
                NETWORK_PROTOCOL_NAME: protocol.split("/")[0] if protocol else "",
                NETWORK_PROTOCOL_VERSION: protocol.split("/")[-1] if protocol else "",
                USER_AGENT_ORIGINAL: http.user_agent or "",
                URL_FULL: http.path or "",
            }
        except (KeyError, AttributeError):
            return None


class ApiGatewayExtractor(AttributeExtractor):
    @property
    def data_source(self) -> AwsDataSource:
        return AwsDataSource.API_GATEWAY

    def extract(self, event: dict, context: LambdaContext) -> dict | None:
        try:
            api_event = APIGatewayProxyEvent(event)
            rc = api_event.request_context
            if rc.api_id is None or rc.get("http"):
                return None

            protocol = rc.protocol or ""

            return {
                FAAS_TRIGGER: FaasTriggerValues.HTTP.value,
                HTTP_REQUEST_METHOD: api_event.http_method or "",
                HTTP_ROUTE: api_event.resource or "",
                HTTP_REQUEST_BODY_SIZE: len(api_event.body or ""),
                NETWORK_PROTOCOL_NAME: protocol.split("/")[0],
                NETWORK_PROTOCOL_VERSION: protocol.split("/")[-1],
                USER_AGENT_ORIGINAL: (api_event.headers or {}).get("User-Agent", ""),
                URL_FULL: api_event.path or "",
            }
        except (KeyError, AttributeError):
            return None


class ElbExtractor(AttributeExtractor):
    @property
    def data_source(self) -> AwsDataSource:
        return AwsDataSource.ELB

    def extract(self, event: dict, context: LambdaContext) -> dict | None:
        try:
            alb_event = ALBEvent(event)
            rc = alb_event.request_context
            if not rc or "elb" not in rc:
                return None

            return {
                FAAS_TRIGGER: FaasTriggerValues.HTTP.value,
                HTTP_REQUEST_METHOD: alb_event.http_method or "",
                HTTP_ROUTE: alb_event.path or "",
                HTTP_REQUEST_BODY_SIZE: len(alb_event.body or ""),
                URL_FULL: alb_event.path or "",
                USER_AGENT_ORIGINAL: (alb_event.headers or {}).get("user-agent", ""),
            }
        except (KeyError, AttributeError):
            return None


class SqsExtractor(AttributeExtractor):
    @property
    def data_source(self) -> AwsDataSource:
        return AwsDataSource.SQS

    def extract(self, event: dict, context: LambdaContext) -> dict | None:
        try:
            sqs_event = SQSEvent(event)
            records = list(sqs_event.records)
            if not records or records[0].event_source != "aws:sqs":
                return None
        except (KeyError, AttributeError):
            return None

        first_record = records[0]
        queue_name = first_record.event_source_arn.split(":")[-1]

        return {
            FAAS_TRIGGER: FaasTriggerValues.PUBSUB.value,
            CLOUD_RESOURCE_ID: first_record.event_source_arn,
            MESSAGING_SYSTEM: "aws.sqs",
            MESSAGING_OPERATION: MessagingOperationTypeValues.RECEIVE.value,
            MESSAGING_BATCH_MESSAGE_COUNT: len(records),
            MESSAGING_DESTINATION_NAME: queue_name,
        }


class SnsExtractor(AttributeExtractor):
    @property
    def data_source(self) -> AwsDataSource:
        return AwsDataSource.SNS

    def extract(self, event: dict, context: LambdaContext) -> dict | None:
        try:
            sns_event = SNSEvent(event)
            records = list(sns_event.records)
            if not records or records[0].event_source != "aws:sns":
                return None
        except (KeyError, AttributeError):
            return None

        return {
            FAAS_TRIGGER: FaasTriggerValues.PUBSUB.value,
        }


class S3Extractor(AttributeExtractor):
    @property
    def data_source(self) -> AwsDataSource:
        return AwsDataSource.S3

    def extract(self, event: dict, context: LambdaContext) -> dict | None:
        try:
            s3_event = S3Event(event)
            records = list(s3_event.records)
            if not records or records[0].event_source != "aws:s3":
                return None
        except (KeyError, AttributeError):
            return None

        return {
            FAAS_TRIGGER: FaasTriggerValues.DATASOURCE.value,
        }


class DynamoDbExtractor(AttributeExtractor):
    @property
    def data_source(self) -> AwsDataSource:
        return AwsDataSource.DYNAMODB

    def extract(self, event: dict, context: LambdaContext) -> dict | None:
        try:
            ddb_event = DynamoDBStreamEvent(event)
            records = list(ddb_event.records)
            if not records or records[0].event_source != "aws:dynamodb":
                return None
        except (KeyError, AttributeError):
            return None

        return {
            FAAS_TRIGGER: FaasTriggerValues.DATASOURCE.value,
        }


class KinesisExtractor(AttributeExtractor):
    @property
    def data_source(self) -> AwsDataSource:
        return AwsDataSource.KINESIS

    def extract(self, event: dict, context: LambdaContext) -> dict | None:
        try:
            kinesis_event = KinesisStreamEvent(event)
            records = list(kinesis_event.records)
            if not records or records[0].event_source != "aws:kinesis":
                return None
        except (KeyError, AttributeError):
            return None

        return {
            FAAS_TRIGGER: FaasTriggerValues.DATASOURCE.value,
        }


class EventBridgeExtractor(AttributeExtractor):
    @property
    def data_source(self) -> AwsDataSource:
        return AwsDataSource.EVENT_BRIDGE

    def extract(self, event: dict, context: LambdaContext) -> dict | None:
        try:
            eb_event = EventBridgeEvent(event)
            if not eb_event.source or not eb_event.detail_type:
                return None
        except (KeyError, AttributeError):
            return None

        trigger_type = (
            FaasTriggerValues.TIMER.value
            if eb_event.detail_type == "Scheduled Event"
            else FaasTriggerValues.PUBSUB.value
        )

        return {
            FAAS_TRIGGER: trigger_type,
        }


class CloudWatchLogsExtractor(AttributeExtractor):
    @property
    def data_source(self) -> AwsDataSource:
        return AwsDataSource.CLOUDWATCH_LOGS

    def extract(self, event: dict, context: LambdaContext) -> dict | None:
        try:
            cw_event = CloudWatchLogsEvent(event)
            if not cw_event.raw_logs_data:
                return None
        except (KeyError, AttributeError):
            return None

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
            attributes = extractor.extract(self.event, self.context)
            if attributes is not None:
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
