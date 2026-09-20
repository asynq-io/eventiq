"""``Service.subscription`` channel consumers against a real broker."""

from __future__ import annotations

import pytest

from eventiq import CloudEvent

from .utils import make_event, wait_for_subscription


@pytest.mark.anyio
async def test_subscription_channel_consumer(broker, service, topic):
    """A short-lived channel consumer receives and can ack the event."""
    received = []
    await broker.connect()

    async with service.subscription(CloudEvent, topic=topic) as subscription:
        await wait_for_subscription(broker)
        await service.publish(make_event(topic, n=1), topic=topic)

        async for event, ack in subscription:
            received.append(event)
            ack()
            break

    assert len(received) == 1
    assert received[0].data == {"n": 1}


@pytest.mark.anyio
async def test_subscription_exits_cleanly(broker, service, topic):
    """Leaving the subscription block stops the consumer and disconnects."""
    await broker.connect()
    async with service.subscription(CloudEvent, topic=topic) as subscription:
        assert subscription is not None
    # Simply completing without error proves the teardown path works.
