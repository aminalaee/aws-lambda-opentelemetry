from unittest.mock import MagicMock, patch

import pytest
from aws_lambda_powertools.utilities.typing import LambdaContext
from opentelemetry.sdk.trace import Span

from aws_lambda_opentelemetry import extractors as utils


class TestColdStart:
    def test_cold_start(self):
        utils._is_cold_start = True  # reset cold start flag

        assert utils._check_cold_start() is True
        assert utils._is_cold_start is False
        assert utils._check_cold_start() is False
        assert utils._check_cold_start() is False

    def test_cold_start_provisioned_concurrency(self, monkeypatch):
        utils._is_cold_start = True  # reset cold start flag

        monkeypatch.setenv(
            utils.constants.LAMBDA_INITIALIZATION_TYPE, "provisioned-concurrency"
        )

        assert utils._check_cold_start() is False
        assert utils._is_cold_start is False
        assert utils._check_cold_start() is False
        assert utils._check_cold_start() is False


class TestLambdaDataSource:
    @pytest.mark.parametrize(
        "aws_data_source,extractor_class,event",
        [
            (
                utils.AwsDataSource.API_GATEWAY,
                utils.ApiGatewayExtractor,
                {
                    "requestContext": {"apiId": "test", "protocol": "HTTP/1.1"},
                    "headers": {},
                    "httpMethod": "GET",
                    "resource": "/",
                    "path": "/",
                },
            ),
            (
                utils.AwsDataSource.HTTP_API,
                utils.HttpApiExtractor,
                {
                    "requestContext": {
                        "http": {
                            "method": "GET",
                            "path": "/",
                            "protocol": "HTTP/1.1",
                            "userAgent": "test",
                        },
                    },
                    "routeKey": "$default",
                },
            ),
            (
                utils.AwsDataSource.ELB,
                utils.ElbExtractor,
                {
                    "requestContext": {"elb": {}},
                    "httpMethod": "GET",
                    "path": "/",
                    "headers": {},
                },
            ),
        ],
    )
    def test_http_trigger(
        self,
        aws_data_source: utils.AwsDataSource,
        extractor_class: type[utils.AttributeExtractor],
        event: dict,
        lambda_context: LambdaContext,
    ):
        extractor = extractor_class()
        assert extractor.extract(event, lambda_context) is not None
        assert extractor.data_source == aws_data_source

    def test_eventbridge_trigger(self, lambda_context: LambdaContext):
        event = {"source": "aws.events", "detail-type": "Scheduled Event"}
        extractor = utils.EventBridgeExtractor()

        assert extractor.extract(event, lambda_context) is not None
        assert extractor.data_source == utils.AwsDataSource.EVENT_BRIDGE

    @pytest.mark.parametrize(
        "event_source, aws_data_source, extractor_class",
        [
            (
                "aws:sns",
                utils.AwsDataSource.SNS,
                utils.SnsExtractor,
            ),
            (
                "aws:sqs",
                utils.AwsDataSource.SQS,
                utils.SqsExtractor,
            ),
            (
                "aws:s3",
                utils.AwsDataSource.S3,
                utils.S3Extractor,
            ),
            (
                "aws:dynamodb",
                utils.AwsDataSource.DYNAMODB,
                utils.DynamoDbExtractor,
            ),
            (
                "aws:kinesis",
                utils.AwsDataSource.KINESIS,
                utils.KinesisExtractor,
            ),
        ],
    )
    def test_pubsub_trigger(
        self,
        event_source: str,
        aws_data_source: utils.AwsDataSource,
        extractor_class: type[utils.AttributeExtractor],
        lambda_context: LambdaContext,
    ):
        record = {"eventSource": event_source}
        if event_source == "aws:sqs":
            record["eventSourceARN"] = "arn:aws:sqs:us-east-1:123456789012:TestQueue"
        if event_source == "aws:sns":
            record = {"EventSource": event_source, "Sns": {}}
        event = {"Records": [record]}

        extractor = extractor_class()

        assert extractor.extract(event, lambda_context) is not None
        assert extractor.data_source == aws_data_source

    def test_cloudwatch_logs_trigger(self, lambda_context: LambdaContext):
        event = {
            "awslogs": {
                "data": "example-data",
            }
        }
        extractor = utils.CloudWatchLogsExtractor()

        assert extractor.extract(event, lambda_context) is not None
        assert extractor.data_source == utils.AwsDataSource.CLOUDWATCH_LOGS

    def test_unknown_trigger(self, lambda_context: LambdaContext):
        event = {}
        extractor = utils.GenericAwsExtractor()

        assert extractor.extract(event, lambda_context) is not None
        assert extractor.data_source == utils.AwsDataSource.OTHER


class TestSetLambdaHandlerAttributes:
    def test_general_attributes(self, lambda_context: LambdaContext):
        span = MagicMock(spec=Span)

        with patch(
            "aws_lambda_opentelemetry.extractors.trace.get_current_span"
        ) as mock_span:
            mock_span.return_value = span

            extractor = utils.AwsAttributesExtractor({}, lambda_context)
            extractor.add_attributes()

        assert span.set_attributes.call_count == 1

        attributes = span.set_attributes.call_args_list[0][0][0]
        assert attributes["faas.invocation_id"] == lambda_context.aws_request_id
        assert attributes["faas.invoked_name"] == lambda_context.function_name
        assert attributes["faas.invoked_region"] is None
        assert attributes["faas.invoked_provider"] == "aws"
        assert attributes["faas.max_memory"] == lambda_context.memory_limit_in_mb
        assert attributes["faas.version"] == lambda_context.function_version
        assert attributes["faas.coldstart"] is False
        assert attributes["cloud.resource_id"] == lambda_context.invoked_function_arn

    def test_sqs_attributes(self, sqs_event: dict, lambda_context: LambdaContext):
        span = MagicMock(spec=Span)

        with patch(
            "aws_lambda_opentelemetry.extractors.trace.get_current_span"
        ) as mock_span:
            mock_span.return_value = span

            extractor = utils.AwsAttributesExtractor(sqs_event, lambda_context)
            extractor.add_attributes()

        assert span.set_attributes.call_count == 2

        general_attributes = span.set_attributes.call_args_list[0][0][0]
        assert general_attributes["faas.invocation_id"] == lambda_context.aws_request_id
        assert general_attributes["faas.coldstart"] is False

        sqs_attributes = span.set_attributes.call_args_list[1][0][0]
        assert sqs_attributes["faas.trigger"] == "pubsub"
        assert sqs_attributes["messaging.system"] == "aws.sqs"
        assert sqs_attributes["messaging.destination.name"] == "MyQueue"
        assert sqs_attributes["messaging.operation"] == "receive"
        assert (
            sqs_attributes["cloud.resource_id"]
            == "arn:aws:sqs:us-east-1:123456789012:MyQueue"
        )

    def test_apigateway_attributes(
        self, apigateway_event: dict, lambda_context: LambdaContext
    ):
        span = MagicMock(spec=Span)

        with patch(
            "aws_lambda_opentelemetry.extractors.trace.get_current_span"
        ) as mock_span:
            mock_span.return_value = span

            extractor = utils.AwsAttributesExtractor(apigateway_event, lambda_context)
            extractor.add_attributes()

        assert span.set_attributes.call_count == 2

        general_attributes = span.set_attributes.call_args_list[0][0][0]
        assert general_attributes["faas.invocation_id"] == lambda_context.aws_request_id
        assert general_attributes["faas.coldstart"] is False

        api_gateway_attributes = span.set_attributes.call_args_list[1][0][0]
        assert api_gateway_attributes["faas.trigger"] == "http"
        assert api_gateway_attributes["http.request.method"] == "POST"
        assert api_gateway_attributes["url.full"] == "/path/to/resource"
        assert api_gateway_attributes["http.route"] == "/{proxy+}"
        assert api_gateway_attributes["http.request.body.size"] == 20
        assert api_gateway_attributes["network.protocol.name"] == "HTTP"
        assert api_gateway_attributes["network.protocol.version"] == "1.1"
        assert (
            api_gateway_attributes["user_agent.original"] == "Custom User Agent String"
        )

    def test_http_api_attributes(
        self, http_api_event: dict, lambda_context: LambdaContext
    ):
        span = MagicMock(spec=Span)

        with patch(
            "aws_lambda_opentelemetry.extractors.trace.get_current_span"
        ) as mock_span:
            mock_span.return_value = span

            extractor = utils.AwsAttributesExtractor(http_api_event, lambda_context)
            extractor.add_attributes()

        assert span.set_attributes.call_count == 2

        general_attributes = span.set_attributes.call_args_list[0][0][0]
        assert general_attributes["faas.invocation_id"] == lambda_context.aws_request_id
        assert general_attributes["faas.coldstart"] is False

        http_attributes = span.set_attributes.call_args_list[1][0][0]
        assert http_attributes["faas.trigger"] == "http"
        assert http_attributes["http.request.method"] == "POST"
        assert http_attributes["url.full"] == "/path/to/resource"
        assert http_attributes["http.route"] == "$default"
        assert http_attributes["http.request.body.size"] == 20
        assert http_attributes["network.protocol.name"] == "HTTP"
        assert http_attributes["network.protocol.version"] == "1.1"
        assert http_attributes["user_agent.original"] == "agent"

    def test_http_api_attributes_with_empty_protocol(
        self, http_api_event: dict, lambda_context: LambdaContext
    ):
        span = MagicMock(spec=Span)

        http_api_event["requestContext"]["http"]["protocol"] = ""

        with patch(
            "aws_lambda_opentelemetry.extractors.trace.get_current_span"
        ) as mock_span:
            mock_span.return_value = span

            extractor = utils.AwsAttributesExtractor(http_api_event, lambda_context)
            extractor.add_attributes()

        http_attributes = span.set_attributes.call_args_list[1][0][0]
        assert http_attributes["network.protocol.name"] == ""
        assert http_attributes["network.protocol.version"] == ""

    def test_http_api_attributes_with_missing_body(
        self, http_api_event: dict, lambda_context: LambdaContext
    ):
        span = MagicMock(spec=Span)

        http_api_event["body"] = None

        with patch(
            "aws_lambda_opentelemetry.extractors.trace.get_current_span"
        ) as mock_span:
            mock_span.return_value = span

            extractor = utils.AwsAttributesExtractor(http_api_event, lambda_context)
            extractor.add_attributes()

        http_attributes = span.set_attributes.call_args_list[1][0][0]
        assert http_attributes["http.request.body.size"] == 0

    def test_alb_attributes(self, alb_event: dict, lambda_context: LambdaContext):
        span = MagicMock(spec=Span)

        with patch(
            "aws_lambda_opentelemetry.extractors.trace.get_current_span"
        ) as mock_span:
            mock_span.return_value = span

            extractor = utils.AwsAttributesExtractor(alb_event, lambda_context)
            extractor.add_attributes()

        assert span.set_attributes.call_count == 2

        general_attributes = span.set_attributes.call_args_list[0][0][0]
        assert general_attributes["faas.invocation_id"] == lambda_context.aws_request_id
        assert general_attributes["faas.coldstart"] is False

        alb_attributes = span.set_attributes.call_args_list[1][0][0]
        assert alb_attributes["faas.trigger"] == "http"
        assert alb_attributes["http.request.method"] == "POST"
        assert alb_attributes["url.full"] == "/path/to/resource"
        assert alb_attributes["http.route"] == "/path/to/resource"
        assert alb_attributes["http.request.body.size"] == 20
        assert (
            alb_attributes["user_agent.original"]
            == "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        )
