import boto3
import pytest
from moto import mock_aws


@pytest.fixture
def mock_sqs_client():
    with mock_aws():
        sqs = boto3.client("sqs", region_name="us-east-1")
        sqs.create_queue(QueueName="test-queue")
        yield sqs
