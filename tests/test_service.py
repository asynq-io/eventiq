from eventiq import CloudEvent, Service
from eventiq.backends.stub import StubBroker


def test_service(service):
    assert isinstance(service, Service)
    assert isinstance(service.broker, StubBroker)
    assert service.name == "test_service"


async def test_consumer_called(running_service: Service, ce: CloudEvent, mock_consumer):
    # publishing event, returns a dict with topics and events (Futures)
    await running_service.publish(ce)
    mock_consumer.assert_awaited_once_with(ce)
