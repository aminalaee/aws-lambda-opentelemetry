from unittest.mock import MagicMock

import pytest
from opentelemetry.sdk.trace import Span
from opentelemetry.semconv._incubating.attributes.faas_attributes import (
    # FAAS_COLDSTART,
    # FAAS_INVOCATION_ID,
    # FAAS_INVOKED_NAME,
    # FAAS_INVOKED_PROVIDER,
    # FAAS_INVOKED_REGION,
    # FAAS_MAX_MEMORY,
    # FAAS_TRIGGER,
    # FAAS_VERSION,
    # FaasInvokedProviderValues,
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
        "key",
        [("apiId",), ("httpMethod",), ("elb",)],
    )
    def test_http_trigger(self, key: str):
        event = {
            "requestContext": {
                "apiId": "example-api-id",
            }
        }
        assert utils.get_lambda_datasource(event) == utils.FaasTriggerValues.HTTP

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
        assert utils.get_lambda_datasource(event) == expected

    @pytest.mark.parametrize(
        "event_source, expected",
        [
            ("aws:sns", utils.FaasTriggerValues.PUBSUB),
            ("aws:sqs", utils.FaasTriggerValues.PUBSUB),
            ("aws:s3", utils.FaasTriggerValues.DATASOURCE),
            ("aws:dynamodb", utils.FaasTriggerValues.DATASOURCE),
            ("aws:kinesis", utils.FaasTriggerValues.DATASOURCE),
        ],
    )
    def test_pubsub_trigger(self, event_source: str, expected: FaasTriggerValues):
        event = {
            "Records": [
                {
                    "eventSource": event_source,
                }
            ]
        }
        assert utils.get_lambda_datasource(event) == expected

    def test_cloudwatch_logs_trigger(self):
        event = {
            "awslogs": {
                "data": "example-data",
            }
        }
        assert utils.get_lambda_datasource(event) == utils.FaasTriggerValues.DATASOURCE

    def test_unknown_trigger(self):
        event = {}
        assert utils.get_lambda_datasource(event) == utils.FaasTriggerValues.OTHER


class TestSetLambdaHandlerAttributes:
    def test_set_attributes(self, lambda_context: LambdaContext):
        span = MagicMock(spec=Span)

        utils.set_lambda_handler_attributes({}, lambda_context, span)

        span.set_attributes.assert_called_once()
        attributes = span.set_attributes.call_args[0][0]
        assert attributes["faas.invocation_id"] == lambda_context.aws_request_id
        assert attributes["faas.invoked_name"] == lambda_context.function_name
        assert attributes["faas.invoked_region"] == lambda_context.region
        assert attributes["faas.invoked_provider"] == "aws"
        assert attributes["faas.max_memory"] == lambda_context.memory_limit_in_mb
        assert attributes["faas.version"] == lambda_context.function_version
        assert attributes["faas.coldstart"] is False
        assert attributes["faas.trigger"] == "other"
        assert attributes["cloud.resource_id"] == lambda_context.invoked_function_arn
