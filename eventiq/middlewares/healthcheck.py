from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

import anyio

from eventiq.middleware import Middleware

if TYPE_CHECKING:
    from eventiq import Service


class HealthCheckMiddleware(Middleware):
    """Middleware for performing basic health checks on broker"""

    BASE_DIR = os.getenv("HEALTHCHECK_DIR", "/tmp")  # nosec

    def __init__(
        self,
        interval: int = 5,
        predicates: list[Callable[..., Awaitable[Any]]] | None = None,
    ) -> None:
        super().__init__()
        self.interval = interval
        self.predicates = predicates or []
        self._task: asyncio.Task | None = None

    async def after_broker_connect(self, *, service: Service) -> None:
        self._task = asyncio.create_task(self._run_forever(service.broker))

    async def _run_forever(self, broker) -> None:
        p = Path(os.path.join(self.BASE_DIR, "healthy"))
        p.touch(exist_ok=True)
        while True:
            try:
                unhealthy = not broker.is_connected

                for predicate in self.predicates:
                    with anyio.move_on_after(3) as scope:
                        res = await predicate()
                    if res is False or scope.cancel_called:
                        unhealthy = True
                        break

            except Exception as e:
                self.logger.exception("Healthcheck failed", exc_info=e)
                unhealthy = True

            if unhealthy:
                p.rename(os.path.join(self.BASE_DIR, "unhealthy"))
            await asyncio.sleep(self.interval)

    async def after_broker_disconnect(self, *, service: Service) -> None:
        if self._task:
            self._task.cancel()
            self._task = None
