"""Consumers processing messages with concurrency > 1."""

from __future__ import annotations

import asyncio

import pytest

from eventiq import CloudEvent

from .utils import make_event, receive, wait_for_subscription


@pytest.mark.anyio
async def test_concurrent_consumer_processes_all(broker, service, topic):
    received = []
    in_flight = 0
    max_in_flight = 0

    @service.subscribe(topic=topic, concurrency=4)
    async def handler(message: CloudEvent) -> None:
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        await asyncio.sleep(0.05)
        in_flight -= 1
        received.append(message)

    count = 8
    async with service.context():
        await wait_for_subscription(broker)
        await service.bulk_publish(
            [make_event(topic, i=i) for i in range(count)], topic=topic
        )
        await receive(received, count=count)

    assert len(received) == count
    assert {m.data["i"] for m in received} == set(range(count))
    # With concurrency 4, several handlers must have overlapped.
    assert max_in_flight > 1
