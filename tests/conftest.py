import asyncio
from collections.abc import AsyncGenerator
from datetime import date
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from eventiq import CloudEvent, Consumer, GenericConsumer, Service
from eventiq.backends.stub import StubBroker
from eventiq.middleware import Middleware


@pytest_asyncio.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.stop()


@pytest.fixture(scope="session")
def middleware():
    class EmptyMiddleware(Middleware):
        pass

    return EmptyMiddleware()


@pytest.fixture
def broker():
    return StubBroker()


@pytest.fixture
def service(broker, middleware):
    return Service(name="test_service", broker=broker, middlewares=[middleware])


@pytest.fixture(scope="session")
def handler():
    async def example_handler(message: CloudEvent) -> int:
        assert isinstance(message, CloudEvent)
        return 42

    return example_handler


@pytest.fixture
def test_consumer(service, handler):
    service.subscribe(handler, topic="test_topic", name="test_consumer")
    return service.consumer_group.consumers["test_consumer"]


@pytest.fixture()
def generic_test_consumer(service) -> Consumer:
    generic_consumer_name = "test_generic_consumer"

    @service.subscribe(topic="test_topic")
    class TestConsumer(GenericConsumer[CloudEvent]):
        name = generic_consumer_name

        async def process(self, message: CloudEvent):
            return 42

    return service.consumer_group.consumers[generic_consumer_name]


@pytest.fixture()
def ce() -> CloudEvent:
    return CloudEvent.new(
        {"today": date.today().isoformat(), "arr": [1, "2", 3.0]},
        type="TestEvent",
        topic="test_topic",
    )


@pytest.fixture
def mock_consumer(handler):
    mock = AsyncMock(spec=handler)
    mock.__annotations__ = handler.__annotations__
    return mock


@pytest_asyncio.fixture()
async def running_service(service: Service, mock_consumer) -> AsyncGenerator:
    service.subscribe(topic="test_topic")(mock_consumer)

    async with service.context():
        yield service
