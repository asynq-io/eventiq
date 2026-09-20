"""HealthCheckMiddleware against a real broker."""

from __future__ import annotations

import anyio
import pytest

from eventiq import CloudEvent
from eventiq.middlewares.healthcheck import (
    HEALTHY_FILE_NAME,
    UNHEALTHY_FILE_NAME,
    HealthCheckMiddleware,
)

from .utils import wait_for_subscription


@pytest.mark.anyio
async def test_healthcheck_marks_healthy(broker, service, topic, tmp_path):
    """After the broker connects, the healthcheck writes the healthy marker."""
    service.add_middleware(HealthCheckMiddleware, interval=1, base_dir=str(tmp_path))

    @service.subscribe(topic=topic)
    async def handler(message: CloudEvent) -> None:
        pass

    healthy = tmp_path / HEALTHY_FILE_NAME
    unhealthy = tmp_path / UNHEALTHY_FILE_NAME

    async with service.context():
        await wait_for_subscription(broker)
        # The probe runs on a background task; give it a chance to write.
        with anyio.fail_after(5):
            while not healthy.exists():
                await anyio.lowlevel.checkpoint()
        assert not unhealthy.exists()
        assert await broker.check_health()
