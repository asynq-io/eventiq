from __future__ import annotations

import tempfile
from typing import TYPE_CHECKING

import anyio

from eventiq.middleware import Middleware

if TYPE_CHECKING:
    from eventiq import Service

HEALTHY_FILE_NAME = "healthy"
UNHEALTHY_FILE_NAME = "unhealthy"


class HealthCheckMiddleware(Middleware):
    """Middleware for performing basic health checks on broker.

    Maintains exactly one marker file in `base_dir`: `healthy` while the broker
    reports a live connection, `unhealthy` otherwise - including before the first
    probe runs and after the broker disconnects. External probes can watch for
    either file without ever seeing both.
    """

    def __init__(
        self,
        service: Service,
        interval: int = 5,
        base_dir: str = tempfile.gettempdir(),
    ) -> None:
        super().__init__(service)
        self.base_dir = anyio.Path(base_dir)
        self.interval = interval

    async def before_broker_connect(self) -> None:
        # A marker in `base_dir` can outlive the process that wrote it. Claim
        # `unhealthy` up front so a restart that never reaches the broker cannot
        # keep reporting the previous run's `healthy`.
        await self._mark_unhealthy()

    async def after_broker_connect(self) -> None:
        self.service.start_background_task(self._run_forever, name="healthcheck")

    async def after_broker_disconnect(self) -> None:
        await self._mark_unhealthy()

    async def _run_forever(self) -> None:
        while True:
            try:
                await self._write_status()
            except Exception:
                # A single unwritable marker file must not kill the probe, which
                # would leave the process looking healthy forever.
                self.logger.exception("Failed to write healthcheck status")
            await anyio.sleep(self.interval)

    async def _write_status(self) -> None:
        try:
            unhealthy = not await self.service.broker.check_health()
        except Exception as e:
            self.logger.exception("Healthcheck failed", exc_info=e)
            unhealthy = True

        if unhealthy:
            await self._mark(UNHEALTHY_FILE_NAME, stale=HEALTHY_FILE_NAME)
        else:
            await self._mark(HEALTHY_FILE_NAME, stale=UNHEALTHY_FILE_NAME)

    async def _mark_unhealthy(self) -> None:
        try:
            await self._mark(UNHEALTHY_FILE_NAME, stale=HEALTHY_FILE_NAME)
        except Exception:
            # Runs on the connect and shutdown paths, where a marker failure must
            # not replace the real error.
            self.logger.exception("Failed to write healthcheck status")

    async def _mark(self, current: str, stale: str) -> None:
        # Stale marker first: touching before unlinking would leave both present,
        # and a probe watching for either would see a contradictory state.
        await (self.base_dir / stale).unlink(missing_ok=True)
        await (self.base_dir / current).touch(exist_ok=True)
