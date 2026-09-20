"""Retry middleware against a real broker."""

from __future__ import annotations

import pytest

from eventiq import CloudEvent
from eventiq.middlewares.retries import MaxRetries, RetryMiddleware

from .utils import (
    HandlerError,
    make_event,
    receive,
    wait_for_subscription,
)


@pytest.mark.anyio
async def test_retry_middleware_retries_until_success(broker, service, topic):
    """A handler failing twice then succeeding is retried, then acknowledged."""
    deliveries = []

    @service.subscribe(topic=topic)
    async def handler(message: CloudEvent) -> None:
        deliveries.append(message)
        if len(deliveries) < 3:
            raise HandlerError

    service.add_middleware(RetryMiddleware, retry_strategy=MaxRetries(max_retries=10))

    async with service.context():
        await wait_for_subscription(broker)
        await service.publish(make_event(topic, n=1), topic=topic)
        await receive(deliveries, count=3)

    assert len(deliveries) == 3
    assert len({m.id for m in deliveries}) == 1
