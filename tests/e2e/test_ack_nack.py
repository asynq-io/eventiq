"""Acknowledgement and rejection semantics against a real broker."""

from __future__ import annotations

import anyio
import pytest

from eventiq import CloudEvent
from eventiq.exceptions import Retry, Skip

from .utils import HandlerError, make_event, receive, wait_for_subscription


@pytest.mark.anyio
async def test_skip_acknowledges_and_stops(broker, service, topic):
    """``Skip`` settles the message as acknowledged and does not redeliver."""
    calls = []

    @service.subscribe(topic=topic)
    async def handler(message: CloudEvent) -> None:
        calls.append(message)
        raise Skip(reason="not for us")

    async with service.context():
        await wait_for_subscription(broker)
        await service.publish(make_event(topic, n=1), topic=topic)
        await receive(calls)
        # Give a stray redelivery a chance to arrive before asserting it did not.
        await anyio.sleep(1.0)

    assert len(calls) == 1


@pytest.mark.anyio
async def test_retry_redelivers(broker, service, topic):
    """Raising ``Retry`` nacks the message so it is redelivered."""
    deliveries = []

    @service.subscribe(topic=topic)
    async def handler(message: CloudEvent) -> None:
        deliveries.append(message)
        if len(deliveries) < 3:
            raise Retry(delay=None)

    async with service.context():
        await wait_for_subscription(broker)
        await service.publish(make_event(topic, n=1), topic=topic)
        await receive(deliveries, count=3)

    assert len(deliveries) == 3
    # Redis redelivers the same message: it is the same event id each time.
    assert len({m.id for m in deliveries}) == 1


@pytest.mark.anyio
async def test_nack_default_redelivers_once(broker, service, topic):
    """The default on-exc action (nack) redelivers a failing message.

    A handler that always fails would loop forever under Redis's immediate
    nack redelivery, so the handler flips to success after the first delivery
    to prove the message comes back exactly once more.
    """
    deliveries = []

    @service.subscribe(topic=topic)
    async def handler(message: CloudEvent) -> None:
        deliveries.append(message)
        if len(deliveries) == 1:
            raise HandlerError

    async with service.context():
        await wait_for_subscription(broker)
        await service.publish(make_event(topic, n=1), topic=topic)
        await receive(deliveries, count=2)

    assert len(deliveries) == 2
    assert len({m.id for m in deliveries}) == 1
