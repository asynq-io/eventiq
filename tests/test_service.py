from typing import Any
from unittest.mock import MagicMock

from eventiq import CloudEvent, Service
from eventiq.backends.stub import StubBroker


def test_service(service: Service):
    assert isinstance(service, Service)
    assert isinstance(service.broker, StubBroker)
    assert service.name == "test_service"


async def test_consumer_called(
    running_service: Service, ce: CloudEvent, mock_consumer: Any
):
    await running_service.publish(ce)
    mock_consumer.assert_called_once_with(ce)


async def test_bulk_publish(
    running_service: Service, ce: CloudEvent, mock_consumer: MagicMock
):
    messages = [ce] * 10
    for message in messages:
        assert "Content-Type" not in message.headers
        assert message.source is None
    await running_service.bulk_publish(messages)
    assert mock_consumer.call_count == 10
    for message in messages:
        assert message.headers["Content-Type"] == "application/json"
        assert message.source == running_service.name
