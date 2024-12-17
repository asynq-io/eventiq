import asyncio
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager, suppress
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from eventiq import CloudEvent, Consumer, GenericConsumer, Service
from eventiq.backends.stub import StubBroker
from eventiq.consumer import FnConsumer
from eventiq.middleware import Middleware
from eventiq.middlewares.dlx import DeadLetterQueueMiddleware
from eventiq.middlewares.error import ErrorHandlerMiddleware
from eventiq.middlewares.healthcheck import HealthCheckMiddleware
from eventiq.middlewares.perf_counter import PerfCounterMiddleware
from eventiq.middlewares.rate_limits import RateLimitMiddleware
from eventiq.middlewares.retries import RetryMiddleware
from eventiq.utils import utc_now


@pytest_asyncio.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.stop()


@pytest.fixture(scope="session")
def middleware():
    class EmptyMiddleware(Middleware):
        pass

    return EmptyMiddleware


@pytest.fixture
def broker():
    return StubBroker()


@pytest.fixture
def service(broker, middleware):
    svc = Service(name="test_service", broker=broker, state={int: 5})
    svc.add_middleware(middleware)
    svc.add_middleware(DeadLetterQueueMiddleware)
    svc.add_middleware(RetryMiddleware)
    svc.add_middleware(HealthCheckMiddleware)
    svc.add_middleware(ErrorHandlerMiddleware, callback=AsyncMock())
    svc.add_middleware(PerfCounterMiddleware)
    svc.add_middleware(RateLimitMiddleware)
    return svc


@pytest.fixture(scope="session")
def handler():
    async def example_handler(message: CloudEvent, x: int) -> int:
        assert isinstance(message, CloudEvent)
        assert x == 5
        return 42

    return example_handler


@pytest.fixture
def test_consumer(service, handler):
    service.subscribe(handler, topic="test_topic", name="test_consumer")
    return service.consumer_group.consumers["test_consumer"]


@pytest.fixture
def generic_test_consumer(service) -> Consumer:
    generic_consumer_name = "test_generic_consumer"

    @service.subscribe(topic="test_topic")
    class TestConsumer(GenericConsumer[CloudEvent]):
        name = generic_consumer_name

        async def process(self, message: CloudEvent, x: int = 3) -> int:
            assert isinstance(message, CloudEvent)
            assert x == 5
            return 42

    return service.consumer_group.consumers[generic_consumer_name]


@pytest.fixture
def ce(service) -> CloudEvent:
    ce_ = CloudEvent.new(
        {"today": utc_now().date().isoformat(), "arr": [1, "2", 3.0]},
        type="TestEvent",
        topic="test_topic",
    )
    ce_.set_context(service, None, {})
    return ce_


@pytest.fixture
def mock_consumer():
    # this is workaround for inspect.getsignature() of AsyncMock
    # https://github.com/python/cpython/issues/96127
    mock = MagicMock(return_value=AsyncMock())
    mock.__annotations__ = {"message": CloudEvent, "return": None}
    mock.__name__ = "mock_consumer"
    return mock


@asynccontextmanager
async def service_context(service) -> AsyncIterator[None]:
    task = asyncio.create_task(service.run(enable_signal_handler=False))
    await asyncio.sleep(0)
    yield
    with suppress(asyncio.CancelledError):
        task.cancel()
        await task


@pytest_asyncio.fixture()
async def running_service(service: Service, mock_consumer) -> AsyncGenerator:
    consumer: Consumer = FnConsumer(
        fn=mock_consumer, event_type=CloudEvent, topic="test_topic"
    )
    service.consumer_group.add_consumer(consumer)

    async with service_context(service):
        yield service
