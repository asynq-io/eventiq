"""Shared helpers for the e2e suite."""

from __future__ import annotations

import anyio

from eventiq import CloudEvent


async def wait_for_subscription(broker, *, timeout: float = 10.0) -> None:
    """Wait until the broker's sender has registered its pub/sub pattern.

    ``RedisBroker.sender`` psubscribes asynchronously inside a task that
    ``Service.context()`` only schedules, so a publish immediately after
    entering the context could race the subscription and be dropped (Redis
    pub/sub is fire-and-forget). ``PUBSUB NUMPAT`` reports the global number
    of active pattern subscriptions, which is just this test's once the sender
    has run.
    """
    with anyio.fail_after(timeout):
        while True:
            if int(await broker.redis.execute_command("PUBSUB", "NUMPAT")) >= 1:
                return
            await anyio.lowlevel.checkpoint()


async def wait_until(predicate, *, timeout: float = 10.0) -> None:
    """Spin until ``predicate`` becomes true, bounded by ``timeout`` seconds."""
    with anyio.fail_after(timeout):
        while not predicate():
            await anyio.lowlevel.checkpoint()


async def receive(records: list, *, count: int = 1, timeout: float = 10.0) -> list:
    """Block until ``records`` has at least ``count`` entries and return them."""
    await wait_until(lambda: len(records) >= count, timeout=timeout)
    return records[:count]


def make_event(topic: str, **data: object) -> CloudEvent:
    """Build a CloudEvent on ``topic`` with the given payload.

    The base :class:`CloudEvent` has no default topic, so the topic is always
    required.
    """
    return CloudEvent.new(dict(data), topic=topic)


class HandlerError(Exception):
    """Raised by test handlers to exercise the error-handling paths."""


async def wait_until_settled(*, timeout: float = 1.0) -> None:
    """Give a stray redelivery a chance to arrive before the test ends."""
    with anyio.fail_after(timeout):
        while True:
            await anyio.lowlevel.checkpoint()


async def wait_for_callback(callback, *, timeout: float = 5.0) -> None:
    """Wait until an ``AsyncMock`` has been awaited at least once."""
    with anyio.fail_after(timeout):
        while not callback.await_args:
            await anyio.lowlevel.checkpoint()
