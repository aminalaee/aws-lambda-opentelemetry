from unittest.mock import MagicMock

import pytest
from opentelemetry.sdk.trace import Span
from opentelemetry.semconv._incubating.attributes.faas_attributes import (
    FaasTriggerValues,
)

from aws_lambda_opentelemetry import utils
from aws_lambda_opentelemetry.typing.context import LambdaContext


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
        "key,aws_data_source",
        [
            ("apiId", utils.AwsDataSource.API_GATEWAY),
            ("http", utils.AwsDataSource.HTTP_API),
            ("elb", utils.AwsDataSource.ELB),
        ],
    )
    def test_http_trigger(self, key: str, aws_data_source: utils.AwsDataSource):
        event = {
            "requestContext": {
                key: "example-api-id",
            }
        }

        mapper = utils.DataSourceAttributeMapper(event)
        assert mapper.faas_trigger == utils.FaasTriggerValues.HTTP
        assert mapper.data_source == aws_data_source

    @pytest.mark.parametrize(
        "detail_type, expected",
        [
            ("Scheduled Event", FaasTriggerValues.TIMER),
            ("Some Other Event", FaasTriggerValues.PUBSUB),
        ],
    )
    def test_eventbridge_trigger(self, detail_type: str, expected: FaasTriggerValues):
        event = {
            "source": "aws.events",
            "detail-type": detail_type,
        }

        mapper = utils.DataSourceAttributeMapper(event)
        assert mapper.faas_trigger == expected
        assert mapper.data_source == utils.AwsDataSource.EVENT_BRIDGE

    @pytest.mark.parametrize(
        "event_source, aws_data_source, faas_trigger",
        [
            ("aws:sns", utils.AwsDataSource.SNS, utils.FaasTriggerValues.PUBSUB),
            ("aws:sqs", utils.AwsDataSource.SQS, utils.FaasTriggerValues.PUBSUB),
            ("aws:s3", utils.AwsDataSource.S3, utils.FaasTriggerValues.DATASOURCE),
            (
                "aws:dynamodb",
                utils.AwsDataSource.DYNAMODB,
                utils.FaasTriggerValues.DATASOURCE,
            ),
            (
                "aws:kinesis",
                utils.AwsDataSource.KINESIS,
                utils.FaasTriggerValues.DATASOURCE,
            ),
        ],
    )
    def test_pubsub_trigger(
        self,
        event_source: str,
        aws_data_source: utils.AwsDataSource,
        faas_trigger: FaasTriggerValues,
    ):
        event = {
            "Records": [
                {
                    "eventSource": event_source,
                }
            ]
        }

        mapper = utils.DataSourceAttributeMapper(event)
        assert mapper.faas_trigger == faas_trigger
        assert mapper.data_source == aws_data_source

    def test_cloudwatch_logs_trigger(self):
        event = {
            "awslogs": {
                "data": "example-data",
            }
        }

        mapper = utils.DataSourceAttributeMapper(event)
        assert mapper.faas_trigger == utils.FaasTriggerValues.DATASOURCE
        assert mapper.data_source == utils.AwsDataSource.CLOUDWATCH_LOGS

    def test_unknown_trigger(self):
        event = {}

        mapper = utils.DataSourceAttributeMapper(event)
        assert mapper.faas_trigger == utils.FaasTriggerValues.OTHER
        assert mapper.data_source == utils.AwsDataSource.OTHER


class TestSetLambdaHandlerAttributes:
    def test_set_general_attributes(self, lambda_context: LambdaContext):
        span = MagicMock(spec=Span)

        utils.set_handler_attributes({}, lambda_context, span)

        attributes = span.set_attributes.call_args_list[1][0][0]
        assert attributes["faas.invocation_id"] == lambda_context.aws_request_id
        assert attributes["faas.invoked_name"] == lambda_context.function_name
        assert attributes["faas.invoked_region"] == lambda_context.region
        assert attributes["faas.invoked_provider"] == "aws"
        assert attributes["faas.max_memory"] == lambda_context.memory_limit_in_mb
        assert attributes["faas.version"] == lambda_context.function_version
        assert attributes["faas.coldstart"] is False
        assert attributes["faas.trigger"] == "other"
        assert attributes["cloud.resource_id"] == lambda_context.invoked_function_arn

    def test_sqs_attributes_set(self, lambda_context: LambdaContext):
        span = MagicMock(spec=Span)

        event = {
            "Records": [
                {
                    "eventSource": "aws:sqs",
                    "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:queue",
                    "awsRegion": "us-east-1",
                }
            ]
        }

        utils.set_handler_attributes(event, lambda_context, span)

        attributes = span.set_attributes.call_args_list[0][0][0]
        assert attributes["messaging.system"] == "aws.sqs"
        assert attributes["messaging.destination.name"] == "queue"
        assert attributes["messaging.operation"] == "receive"
        assert (
            attributes["cloud.resource_id"]
            == "arn:aws:sqs:us-east-1:123456789012:queue"
        )
