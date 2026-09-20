"""Dead letter queue middleware against a real broker."""

from __future__ import annotations

import pytest

from eventiq import CloudEvent
from eventiq.exceptions import Fail
from eventiq.middlewares.dlx import DeadLetterQueueMiddleware

from .utils import make_event, receive, wait_for_subscription


@pytest.mark.anyio
async def test_dead_letter_queue_receives_failed_message(broker, service, topic):
    """A failed message is republished to the DLX topic and received there."""
    dlx_topic = f"{topic}.dlx"
    failed = []
    dead_lettered = []

    @service.subscribe(topic=topic)
    async def failing_handler(message: CloudEvent) -> None:
        failed.append(message)
        raise Fail(reason="permanently broken")

    @service.subscribe(topic=dlx_topic, name="dlx_consumer")
    async def dlx_handler(message: CloudEvent) -> None:
        dead_lettered.append(message)

    service.add_middleware(DeadLetterQueueMiddleware, topic=dlx_topic)

    async with service.context():
        await wait_for_subscription(broker)
        await service.publish(make_event(topic, n=1), topic=topic)
        await receive(failed)
        await receive(dead_lettered)

    assert len(failed) == 1
    assert len(dead_lettered) == 1
    assert dead_lettered[0].id == failed[0].id
    # Redis pub/sub does not relay headers, so the ``exc-reason`` header the DLX
    # middleware attaches on publish is not observable on the received message.
