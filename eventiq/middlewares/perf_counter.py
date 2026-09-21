from __future__ import annotations

from logging import INFO
from typing import TYPE_CHECKING, Any, Literal

from eventiq.middleware import CloudEventType, Middleware

if TYPE_CHECKING:
    from collections.abc import Mapping

    from eventiq import Service

_OPERATION_MESSAGES: Mapping[str, str] = {
    "processed": "Message processed",
    "published": "Message published",
}


class PerfCounterMiddleware(Middleware[CloudEventType]):
    """Logs how long message processing and publishing take."""

    def __init__(
        self,
        service: Service,
        log_level: int = INFO,
    ) -> None:
        super().__init__(service)
        self.log_level = log_level

    def _log_elapsed_time(
        self,
        operation: Literal["processed", "published"],
        elapsed: float,
    ) -> None:
        self.logger.log(
            self.log_level,
            _OPERATION_MESSAGES[operation],
            extra={
                "operation": operation,
                "elapsed_seconds": elapsed,
            },
        )

    async def after_message_finalized(
        self,
        *,
        elapsed: float = 0.0,
        **_: Any,
    ) -> None:
        self._log_elapsed_time("processed", elapsed)

    async def after_publish(self, *, elapsed: float = 0.0, **_: Any) -> None:
        self._log_elapsed_time("published", elapsed)
