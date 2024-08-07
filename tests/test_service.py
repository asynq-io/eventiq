from typing import Any

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
