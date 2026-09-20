"""Dynamic (ephemeral) consumers against a real broker."""

from __future__ import annotations

import pytest

from eventiq import CloudEvent
from eventiq.consumer import FnConsumer

from .utils import make_event, receive, wait_for_subscription


@pytest.mark.anyio
async def test_dynamic_consumer_receives(broker, service, topic):
    """A consumer registered with ``dynamic=True`` receives messages."""
    received = []
    consumer = FnConsumer(
        fn=lambda message: received.append(message),
        event_type=CloudEvent,
        topic=topic,
        name="dynamic_e2e",
        dynamic=True,
    )
    service.consumer_group.add_consumer(consumer)

    async with service.context():
        await wait_for_subscription(broker)
        await service.publish(make_event(topic, n=1), topic=topic)
        await receive(received)

    assert len(received) == 1
    assert received[0].data == {"n": 1}
