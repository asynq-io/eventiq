"""Broker connect/disconnect lifecycle against a real server."""

from __future__ import annotations

import pytest

from eventiq import CloudEvent

from .utils import make_event, receive, wait_for_subscription


@pytest.mark.anyio
async def test_connect_marks_broker_connected(broker):
    """A fresh broker starts disconnected and reports healthy once connected."""
    assert not broker.is_connected
    assert not await broker.check_health()
    await broker.connect()
    assert broker.is_connected
    # `check_health` does a real round trip, so this proves the server answers.
    assert await broker.check_health()
    await broker.disconnect()
    assert not broker.is_connected


@pytest.mark.anyio
async def test_disconnect_drops_the_client(broker):
    await broker.connect()
    assert broker.is_connected
    await broker.disconnect()
    assert not broker.is_connected


@pytest.mark.anyio
async def test_broker_usable_after_fresh_connect(broker, service, topic):
    """After connecting, publish reaches a running service."""
    received = []

    @service.subscribe(topic=topic)
    async def handler(message: CloudEvent) -> None:
        received.append(message)

    async with service.context():
        await wait_for_subscription(broker)
        await service.publish(make_event(topic, n=1), topic=topic)
        await receive(received)

    assert len(received) == 1
    assert received[0].data == {"n": 1}
