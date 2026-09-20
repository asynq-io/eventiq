"""Middleware behaviours exercised end to end against a real broker."""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock

import pytest

from eventiq import CloudEvent
from eventiq.middlewares.error import ErrorHandlerMiddleware
from eventiq.middlewares.gzip import GzipMiddleware
from eventiq.middlewares.perf_counter import PerfCounterMiddleware

from .utils import (
    HandlerError,
    make_event,
    receive,
    wait_for_callback,
    wait_for_subscription,
)


@pytest.mark.anyio
async def test_gzip_payload_roundtrip(broker, service, topic):
    """Gzip-compressed payloads are decompressed on receipt."""
    received = []
    service.add_middleware(GzipMiddleware)

    @service.subscribe(topic=topic)
    async def handler(message: CloudEvent) -> None:
        received.append(message)

    async with service.context():
        await wait_for_subscription(broker)
        await service.publish(make_event(topic, n=42), topic=topic)
        await receive(received)

    assert len(received) == 1
    assert received[0].data == {"n": 42}


@pytest.mark.anyio
async def test_perf_counter_records_processing(broker, service, topic, caplog):
    """PerfCounterMiddleware logs a processed message on finalization."""
    received = []
    service.add_middleware(PerfCounterMiddleware)

    @service.subscribe(topic=topic)
    async def handler(message: CloudEvent) -> None:
        received.append(message)

    with caplog.at_level(logging.INFO, logger="eventiq"):
        async with service.context():
            await wait_for_subscription(broker)
            await service.publish(make_event(topic, n=1), topic=topic)
            await receive(received)

    assert any("processed" in r.message for r in caplog.records)


@pytest.mark.anyio
async def test_error_handler_invoked_on_failure(broker, service, topic):
    """ErrorHandlerMiddleware callback fires when the handler raises."""
    received = []
    callback = AsyncMock()

    @service.subscribe(topic=topic)
    async def handler(message: CloudEvent) -> None:
        received.append(message)
        raise HandlerError

    service.add_middleware(ErrorHandlerMiddleware, callback=callback)

    async with service.context():
        await wait_for_subscription(broker)
        await service.publish(make_event(topic, n=1), topic=topic)
        await receive(received)
        # The callback runs during finalization; allow a beat for it.
        await wait_for_callback(callback)

    callback.assert_awaited_once()
    assert callback.await_args.args[0] is service
