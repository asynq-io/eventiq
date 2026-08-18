import asyncio
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager, suppress
from unittest.mock import AsyncMock, MagicMock

import anyio
import pytest

from eventiq import CloudEvent, Consumer, GenericConsumer, Service
from eventiq.backends.stub import StubBroker
from eventiq.consumer import FnConsumer
from eventiq.context import set_current_service
from eventiq.middleware import Middleware
from eventiq.middlewares.dlx import DeadLetterQueueMiddleware
from eventiq.middlewares.error import ErrorHandlerMiddleware
from eventiq.middlewares.healthcheck import HealthCheckMiddleware
from eventiq.middlewares.perf_counter import PerfCounterMiddleware
from eventiq.middlewares.rate_limits import RateLimitMiddleware
from eventiq.middlewares.retries import RetryMiddleware
from eventiq.utils import utc_now


@pytest.fixture(autouse=True, scope="session")
def anyio_backend():
    return "asyncio"


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
    set_current_service(svc)
    yield svc
    set_current_service(None)


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
def ce() -> CloudEvent:
    ce_ = CloudEvent.new(
        {"today": utc_now().date().isoformat(), "arr": [1, "2", 3.0]},
        type="TestEvent",
        topic="test_topic",
    )
    ce_.set_raw(None, {})
    return ce_


@pytest.fixture
def mock_consumer():
    # this is workaround for inspect.getsignature() of AsyncMock
    # https://github.com/python/cpython/issues/96127
    mock = MagicMock(return_value=AsyncMock())
    mock.__annotations__ = {"message": CloudEvent, "return": None}
    mock.__name__ = "mock_consumer"
    return mock


async def _wait_for_senders(service) -> None:
    """Block until every consumer's sender has registered its topic.

    `StubBroker.sender` inserts into `topics` lazily and `publish` only matches
    topics already present, so publishing before that silently drops the message.
    """
    expected = {c.topic for c in service.consumer_group.consumers.values()}
    with anyio.fail_after(1):
        while not expected <= service.broker.topics.keys():
            await anyio.lowlevel.checkpoint()


@asynccontextmanager
async def service_context(service) -> AsyncIterator[None]:
    task = asyncio.create_task(service.run(enable_signal_handler=False))
    await _wait_for_senders(service)
    yield
    with suppress(asyncio.CancelledError):
        task.cancel()
        await task


@pytest.fixture
async def running_service(service: Service, mock_consumer) -> AsyncGenerator:
    consumer: Consumer = FnConsumer(
        fn=mock_consumer,
        event_type=CloudEvent,
        topic="test_topic",
    )
    service.consumer_group.add_consumer(consumer)

    async with service_context(service):
        yield service
