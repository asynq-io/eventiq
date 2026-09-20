"""Bulk publishing over a real broker."""

from __future__ import annotations

import pytest

from eventiq import CloudEvent

from .utils import make_event, receive, wait_for_subscription


@pytest.mark.anyio
async def test_bulk_publish_delivers_every_message(broker, service, topic):
    received = []

    @service.subscribe(topic=topic)
    async def handler(message: CloudEvent) -> None:
        received.append(message)

    count = 10
    messages = [make_event(topic, i=i) for i in range(count)]

    async with service.context():
        await wait_for_subscription(broker)
        await service.bulk_publish(messages, topic=topic)
        delivered = await receive(received, count=count)

    assert len(delivered) == count
    assert {m.data["i"] for m in delivered} == set(range(count))
