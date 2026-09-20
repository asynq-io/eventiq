import asyncio
from typing import Annotated, Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from modern_di import Container, Group, Scope, providers

from eventiq import CloudEvent, Consumer, GenericConsumer, Service
from eventiq.backends.stub import StubBroker, StubMessage
from eventiq.context import set_current_service
from eventiq.dependencies import UNRESOLVED
from eventiq.integrations.modern_di import (
    DI_CONTAINER_STATE_KEY,
    FromDI,
    ModernDIMiddleware,
    ModernDIResolver,
    _request_container,
    eventiq_message_provider,
    fetch_di_container,
    setup_di,
)
from eventiq.middleware import Middleware


class Settings:
    """APP scoped dependency."""


class Report:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings


class Echo:
    """REQUEST scoped dependency reading the message being processed."""

    def __init__(self, message: CloudEvent) -> None:
        self.topic = message.topic


class Resource:
    closed = False


finalized: list[Resource] = []


def close_resource(resource: Resource) -> None:
    resource.closed = True
    finalized.append(resource)


class AppGroup(Group):
    settings = providers.Factory(Settings, scope=Scope.APP, cache=True)
    report = providers.Factory(Report, scope=Scope.REQUEST)
    echo = providers.Factory(Echo, scope=Scope.REQUEST)
    resource = providers.Factory(
        Resource,
        scope=Scope.REQUEST,
        cache=providers.CacheSettings(finalizer=close_resource),
    )


@pytest.fixture(autouse=True)
def _clear_finalized() -> None:
    finalized.clear()


@pytest.fixture
def di_service():
    svc = Service(name="di_service", broker=StubBroker(), state={str: "from-state"})
    set_current_service(svc)
    yield svc
    set_current_service(None)


@pytest.fixture
def container(di_service):
    return setup_di(di_service, Container(groups=[AppGroup]))


@pytest.fixture
def di_middleware(container, di_service) -> ModernDIMiddleware:
    return di_service.middlewares[-1]


async def process(service: Service, consumer: Consumer, message: CloudEvent) -> Any:
    """Drive `message` through `consumer` exactly as `Service._process` does."""
    await service.dispatch_before("process_message", consumer=consumer, message=message)
    result = None
    exc = None
    try:
        result = await consumer.process(message)
    except Exception as e:
        exc = e
    await service.dispatch_after(
        "process_message",
        consumer=consumer,
        message=message,
        result=result,
        exc=exc,
    )
    if exc is not None:
        raise exc
    return result


@pytest.fixture
def _stubbed_finalization(di_service, monkeypatch) -> None:
    """Keep `Service.ack`/`nack` dispatching hooks without touching the queue."""
    monkeypatch.setattr(di_service.broker, "ack", AsyncMock())
    monkeypatch.setattr(di_service.broker, "nack", AsyncMock())


async def process_raw(
    service: Service, consumer: Consumer, message: CloudEvent
) -> None:
    """Drive `message` through the real `Service._process`, finalization included."""
    raw = StubMessage(
        data=service.encoder.encode(message),
        queue=asyncio.Queue(),
        event=asyncio.Event(),
        headers={},
    )
    await service._process(consumer, raw, service.decoder, 10.0)


class BrokenAfterMiddleware(Middleware):
    """Stands in for any after-hook that never returns to the ones behind it."""

    async def after_process_message(self, **_: Any) -> None:
        msg = "after hook exploded"
        raise RuntimeError(msg)


def subscribe(service: Service, handler, **options: Any) -> Consumer:
    service.subscribe(handler, topic="test_topic", **options)
    return next(reversed(service.consumers.values()))


def test_setup_di_registers_container_middleware_and_resolver(di_service, container):
    assert di_service.state[DI_CONTAINER_STATE_KEY] is container
    assert fetch_di_container(di_service) is container
    assert isinstance(di_service.middlewares[-1], ModernDIMiddleware)
    assert isinstance(di_service.dependency_resolver, ModernDIResolver)
    assert di_service.dependency_resolver.container is container


def test_setup_di_registers_message_context_provider(container):
    assert (
        container.providers_registry.find_provider(CloudEvent)
        is eventiq_message_provider
    )
    assert eventiq_message_provider.scope is Scope.REQUEST


def test_container_validates_after_setup(container):
    container.validate()


@pytest.mark.anyio
async def test_root_container_opens_and_closes_with_broker(di_service, container):
    container.close_sync()
    assert container.closed

    async with di_service.context():
        assert not container.closed

    assert container.closed


@pytest.mark.anyio
async def test_root_container_survives_restart(di_service, container):
    async with di_service.context():
        assert not container.closed
    async with di_service.context():
        assert not container.closed
    assert container.closed


@pytest.mark.anyio
async def test_marker_injection_by_provider(di_service, container, ce):
    resolved = {}

    async def handler(
        message: CloudEvent,
        report: Annotated[Report, FromDI(AppGroup.report)],
    ) -> None:
        resolved["report"] = report

    consumer = subscribe(di_service, handler)
    await process(di_service, consumer, ce)

    assert isinstance(resolved["report"], Report)
    assert isinstance(resolved["report"].settings, Settings)


@pytest.mark.anyio
async def test_marker_injection_by_type(di_service, container, ce):
    resolved = {}

    async def handler(
        message: CloudEvent,
        report: Annotated[Report, FromDI(Report)],
    ) -> None:
        resolved["report"] = report

    consumer = subscribe(di_service, handler)
    await process(di_service, consumer, ce)

    assert isinstance(resolved["report"], Report)


@pytest.mark.anyio
async def test_plain_type_injection_without_marker(di_service, container, ce):
    resolved = {}

    async def handler(message: CloudEvent, report: Report) -> None:
        resolved["report"] = report

    consumer = subscribe(di_service, handler)
    await process(di_service, consumer, ce)

    assert isinstance(resolved["report"], Report)


@pytest.mark.anyio
async def test_message_injected_as_context(di_service, container, ce):
    resolved = {}

    async def handler(message: CloudEvent, echo: Echo) -> None:
        resolved["echo"] = echo

    consumer = subscribe(di_service, handler)
    await process(di_service, consumer, ce)

    assert resolved["echo"].topic == ce.topic


@pytest.mark.anyio
async def test_request_container_injected(di_service, container, ce):
    resolved = {}

    async def handler(message: CloudEvent, request_container: Container) -> None:
        resolved["container"] = request_container

    consumer = subscribe(di_service, handler)
    await process(di_service, consumer, ce)

    assert resolved["container"].scope is Scope.REQUEST
    assert resolved["container"].parent_container is container


@pytest.mark.anyio
async def test_app_scoped_dependency_is_shared_between_messages(
    di_service, container, ce
):
    resolved = []

    async def handler(message: CloudEvent, report: Report) -> None:
        resolved.append(report)

    consumer = subscribe(di_service, handler)
    await process(di_service, consumer, ce)
    await process(di_service, consumer, ce)

    first, second = resolved
    assert first is not second
    assert first.settings is second.settings


@pytest.mark.anyio
async def test_request_container_closed_after_message(
    di_service, container, di_middleware, ce
):
    async def handler(message: CloudEvent, resource: Resource) -> None:
        assert not resource.closed

    consumer = subscribe(di_service, handler)
    await process(di_service, consumer, ce)

    assert len(finalized) == 1
    assert finalized[0].closed
    assert _request_container.get() is None


@pytest.mark.anyio
async def test_request_container_closed_when_handler_raises(
    di_service, container, di_middleware, ce
):
    async def handler(message: CloudEvent, resource: Resource) -> None:
        msg = "boom"
        raise ValueError(msg)

    consumer = subscribe(di_service, handler)
    with pytest.raises(ValueError, match="boom"):
        await process(di_service, consumer, ce)

    assert len(finalized) == 1
    assert _request_container.get() is None


@pytest.mark.anyio
@pytest.mark.usefixtures("_stubbed_finalization")
async def test_request_container_closed_when_later_after_hook_raises(
    di_service, container, ce
):
    """The finalizers run with the consumer, so no after-hook can skip them."""
    di_service.add_middleware(BrokenAfterMiddleware)

    async def handler(message: CloudEvent, resource: Resource) -> None:
        pass

    consumer = subscribe(di_service, handler)
    await process_raw(di_service, consumer, ce)

    assert len(finalized) == 1
    assert finalized[0].closed
    assert _request_container.get() is None


@pytest.mark.anyio
@pytest.mark.usefixtures("_stubbed_finalization")
async def test_no_per_message_state_accumulates(
    di_service, container, di_middleware, ce
):
    """Consumed messages must not pile up in the middleware nor in the resolver."""
    di_service.add_middleware(BrokenAfterMiddleware)

    async def handler(message: CloudEvent, resource: Resource) -> None:
        pass

    consumer = subscribe(di_service, handler)
    for _ in range(5):
        await process_raw(di_service, consumer, ce)

    assert len(finalized) == 5
    assert all(resource.closed for resource in finalized)
    assert _request_container.get() is None
    assert vars(di_middleware) == {"service": di_service}
    assert vars(di_service.dependency_resolver) == {"container": container}


@pytest.mark.anyio
async def test_no_request_container_built_without_container_dependency(
    di_service, container, ce, monkeypatch
):
    """A handler whose parameters `state` covers never opens a child container."""
    resolver = di_service.dependency_resolver
    build = MagicMock(side_effect=resolver._request_scope)
    monkeypatch.setattr(resolver, "_request_scope", build)

    async def handler(message: CloudEvent, value: str) -> None:
        pass

    consumer = subscribe(di_service, handler)
    await process(di_service, consumer, ce)

    build.assert_not_called()
    assert _request_container.get() is None


@pytest.mark.anyio
async def test_generic_consumer_is_injected(di_service, container, ce):
    resolved = {}

    @di_service.subscribe(topic="test_topic")
    class MyConsumer(GenericConsumer[CloudEvent]):
        name = "generic_di_consumer"

        async def process(self, message: CloudEvent, report: Report) -> None:
            resolved["report"] = report

    consumer = di_service.consumers["generic_di_consumer"]
    await process(di_service, consumer, ce)

    assert isinstance(resolved["report"], Report)


@pytest.mark.anyio
async def test_service_state_takes_precedence_over_container(di_service, container, ce):
    resolved = {}

    async def handler(message: CloudEvent, value: str) -> None:
        resolved["value"] = value

    consumer = subscribe(di_service, handler)
    await process(di_service, consumer, ce)

    assert resolved["value"] == "from-state"


@pytest.mark.anyio
async def test_unregistered_annotation_keeps_default(di_service, container, ce):
    resolved = {}

    async def handler(message: CloudEvent, missing: float = 1.5) -> None:
        resolved["missing"] = missing

    consumer = subscribe(di_service, handler)
    await process(di_service, consumer, ce)

    assert resolved["missing"] == 1.5


@pytest.mark.anyio
async def test_annotated_without_marker_falls_back_to_type(di_service, container, ce):
    resolved = {}

    async def handler(
        message: CloudEvent,
        report: Annotated[Report, "not a marker"],
    ) -> None:
        resolved["report"] = report

    consumer = subscribe(di_service, handler)
    await process(di_service, consumer, ce)

    assert isinstance(resolved["report"], Report)


@pytest.mark.anyio
async def test_close_without_container_in_flight(di_service, container):
    await di_service.dependency_resolver.close()

    assert not finalized


@pytest.mark.anyio
async def test_unregistered_annotation_is_unresolved(di_service, container, ce):
    resolver = di_service.dependency_resolver

    assert await resolver.resolve(ce, complex) is UNRESOLVED
    assert await resolver.resolve(ce, "not a type") is UNRESOLVED
    assert _request_container.get() is None


def test_unresolved_repr():
    assert repr(UNRESOLVED) == "UNRESOLVED"
