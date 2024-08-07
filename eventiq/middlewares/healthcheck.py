from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING

from eventiq.middleware import Middleware

if TYPE_CHECKING:
    from eventiq import Service


class HealthCheckMiddleware(Middleware):
    """Middleware for performing basic health checks on broker."""

    def __init__(
        self,
        service: Service,
        interval: int = 5,
        base_dir: str = "/tmp",  # nosec
    ) -> None:
        super().__init__(service)
        self.base_dir = Path(base_dir)
        self.interval = interval
        self._task: asyncio.Task | None = None

    async def after_broker_connect(self) -> None:
        self._task = asyncio.create_task(self._run_forever())

    async def _run_forever(self) -> None:
        p = self.base_dir / "healthy"
        broker = self.service.broker
        while True:
            try:
                unhealthy = not broker.is_connected
            except Exception as e:
                self.logger.exception("Healthcheck failed", exc_info=e)
                unhealthy = True

            if unhealthy and p.exists():
                p.rename(self.base_dir / "unhealthy")
            else:
                p.touch(exist_ok=True)
            await asyncio.sleep(self.interval)

    async def after_broker_disconnect(self) -> None:
        if self._task:
            self._task.cancel()
            self._task = None
