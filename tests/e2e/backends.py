"""The real brokers the e2e suite runs against.

Every backend is backed by a throwaway ``testcontainers`` container, so no
broker here is the in-memory :class:`~eventiq.backends.stub.StubBroker` the
unit suite uses. Images are pinned but overridable, e.g.
``EVENTIQ_E2E_REDIS_IMAGE=redis:7-alpine``.

Only Redis is wired up today; the :class:`Backend` dataclass and the container
lifecycle are shaped so that NATS, JetStream, RabbitMQ and Kafka can be added
by extending :data:`BACKENDS` without touching the fixtures or the tests.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from contextlib import AbstractContextManager

    from eventiq import Broker

REDIS_IMAGE = os.environ.get("EVENTIQ_E2E_REDIS_IMAGE", "redis:7-alpine")


@contextmanager
def _redis_url() -> Generator[str]:
    from testcontainers.community.redis import RedisContainer

    with RedisContainer(REDIS_IMAGE) as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(container.port)
        yield f"redis://{host}:{port}"


@dataclass(frozen=True, slots=True)
class Backend:
    """A broker the e2e suite runs against, and what its server supports.

    The capability flags record what the *server* provides, which is not always
    what a given broker implementation exposes -- tests needing a capability
    the backend lacks are skipped, and the mismatch itself is asserted by a
    dedicated test.
    """

    name: str
    broker_class: type[Broker]
    url: Callable[[], AbstractContextManager[str]]
    #: the broker relays message headers between publisher and consumer
    headers: bool
    #: nack can carry a redelivery delay (as opposed to redelivering at once)
    nack_delay: bool
    #: a nacked message is redelivered to the same consumer
    redelivery: bool
    #: the broker reports how many times a message has been delivered
    num_delivered: bool


BACKENDS: dict[str, Backend] = {
    "redis": Backend(
        name="redis",
        broker_class=__import__(
            "eventiq.backends.redis", fromlist=["RedisBroker"]
        ).RedisBroker,
        url=_redis_url,
        # Redis pub/sub is fire-and-forget: it relays no headers at all.
        headers=False,
        # ``RedisBroker.nack`` republishes immediately and ignores the delay.
        nack_delay=False,
        # republishing on nack redelivers the message to the same consumer.
        redelivery=True,
        # pub/sub messages carry no delivery count.
        num_delivered=False,
    ),
}


def parse_backends(names: str) -> list[Backend]:
    """Resolve a comma separated ``--e2e-backends`` value to backends."""
    selected = [name.strip() for name in names.split(",") if name.strip()]
    unknown = sorted(set(selected) - set(BACKENDS))
    if unknown:
        msg = f"Unknown e2e backends {unknown}; available: {sorted(BACKENDS)}"
        raise ValueError(msg)
    return [BACKENDS[name] for name in selected]
