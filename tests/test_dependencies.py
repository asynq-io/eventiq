import logging
from typing import Any

import anyio
import pytest

from eventiq import CloudEvent, dependencies
from eventiq.dependencies import (
    UNRESOLVED,
    DefaultDependencyResolver,
    resolved_func,
)

from .unresolvable_handlers import (
    UnresolvableConsumer,
    UnresolvableEvent,
    handler_with_unresolvable_dependency,
)

UNRESOLVED_MATCH = "Could not resolve annotations of"


class RecordingResolver(DefaultDependencyResolver):
    def __init__(self) -> None:
        self.resolved: list[Any] = []
        self.closed = 0

    async def resolve(self, message: CloudEvent, annotation: Any) -> Any:
        self.resolved.append(annotation)
        return await super().resolve(message, annotation)

    async def close(self) -> None:
        self.closed += 1


def test_unresolvable_annotation_raises_on_wrap():
    with pytest.raises(TypeError, match=UNRESOLVED_MATCH):
        resolved_func(handler_with_unresolvable_dependency)


def test_unresolvable_annotation_raises_on_subscribe(service):
    with pytest.raises(TypeError, match=UNRESOLVED_MATCH):
        service.subscribe(
            handler_with_unresolvable_dependency,
            topic="test_topic",
            event_type=UnresolvableEvent,
        )


def test_unresolvable_annotation_raises_on_generic_consumer_subscribe(service):
    with pytest.raises(TypeError, match=UNRESOLVED_MATCH):
        service.subscribe(topic="test_topic")(UnresolvableConsumer)


@pytest.mark.anyio
async def test_unannotated_params_are_not_resolved(service, ce):
    resolver = RecordingResolver()
    service.dependency_resolver = resolver

    async def handler(
        message: CloudEvent, x: int, y: float = 1.0, untyped="default"
    ) -> tuple:
        return x, y, untyped

    result = await resolved_func(handler)(ce)

    assert result == (5, 1.0, "default")
    assert resolver.resolved == [int, float]


@pytest.mark.anyio
async def test_default_resolver_reads_state(service, ce):
    resolver = DefaultDependencyResolver()

    assert await resolver.resolve(ce, int) == 5
    assert await resolver.resolve(ce, float) is UNRESOLVED
    assert await resolver.close() is None


@pytest.mark.anyio
async def test_resolver_closed_after_handler_returns(service, ce):
    resolver = RecordingResolver()
    service.dependency_resolver = resolver

    async def handler(message: CloudEvent, x: int) -> int:
        assert resolver.closed == 0
        return x

    assert await resolved_func(handler)(ce) == 5
    assert resolver.closed == 1


@pytest.mark.anyio
async def test_resolver_closed_when_handler_raises(service, ce):
    resolver = RecordingResolver()
    service.dependency_resolver = resolver

    async def handler(message: CloudEvent, x: int) -> None:
        msg = "boom"
        raise ValueError(msg)

    with pytest.raises(ValueError, match="boom"):
        await resolved_func(handler)(ce)

    assert resolver.closed == 1


@pytest.mark.anyio
async def test_resolver_closed_when_handler_times_out(service, ce):
    """A cancelled consumer must still release what it resolved."""

    class SlowCloseResolver(RecordingResolver):
        async def close(self) -> None:
            await anyio.sleep(0.01)
            await super().close()

    resolver = SlowCloseResolver()
    service.dependency_resolver = resolver

    async def handler(message: CloudEvent, x: int) -> None:
        await anyio.sleep(5)

    with pytest.raises(TimeoutError), anyio.fail_after(0.05):
        await resolved_func(handler)(ce)

    assert resolver.closed == 1


@pytest.mark.anyio
async def test_hanging_close_times_out(service, ce, caplog, monkeypatch):
    class HangingResolver(RecordingResolver):
        async def close(self) -> None:
            await anyio.sleep(5)
            await super().close()

    resolver = HangingResolver()
    service.dependency_resolver = resolver
    monkeypatch.setattr(dependencies, "CLOSE_TIMEOUT", 0.01)

    async def handler(message: CloudEvent, x: int) -> int:
        return x

    with caplog.at_level(logging.ERROR):
        assert await resolved_func(handler)(ce) == 5

    assert resolver.closed == 0
    assert "Timed out closing dependency resolver" in caplog.text


@pytest.mark.anyio
async def test_resolver_not_closed_without_annotated_params(service, ce):
    resolver = RecordingResolver()
    service.dependency_resolver = resolver

    async def handler(message: CloudEvent) -> str:
        return "done"

    assert await resolved_func(handler)(ce) == "done"
    assert resolver.closed == 0
