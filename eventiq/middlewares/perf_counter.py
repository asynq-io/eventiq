from __future__ import annotations

from logging import INFO
from time import perf_counter
from typing import TYPE_CHECKING, Any, Literal

from eventiq.middleware import CloudEventType, Middleware

if TYPE_CHECKING:
    from eventiq import Consumer, Service
    from eventiq.types import ID


class PerfCounterMiddleware(Middleware[CloudEventType]):
    def __init__(self, service: Service, log_level: int = INFO) -> None:
        super().__init__(service)
        self.log_level = log_level
        self._receive_registry: dict[tuple[str, ID], float] = {}
        self._publish_registry: dict[ID, float] = {}

    def _log_elapsed_time(
        self, operation: Literal["processed", "published"], start_time: float
    ) -> None:
        elapsed = perf_counter() - start_time
        self.logger.log(
            self.log_level, "Message %s in %.2f seconds.", operation, elapsed
        )

    async def before_process_message(
        self, *, consumer: Consumer, message: CloudEventType
    ) -> None:
        self._receive_registry[(consumer.name, message.id)] = perf_counter()

    async def after_process_message(
        self,
        *,
        consumer: Consumer,
        message: CloudEventType,
        result: Any = None,
        exc: Exception | None = None,
    ) -> None:
        start_time = self._receive_registry.pop(
            (consumer.name, message.id), perf_counter()
        )
        self._log_elapsed_time("processed", start_time)

    async def before_publish(self, *, message: CloudEventType, **kwargs: Any) -> None:
        self._publish_registry[message.id] = perf_counter()

    async def after_publish(self, *, message: CloudEventType, **kwargs: Any) -> None:
        start_time = self._publish_registry.pop(message.id, perf_counter())
        self._log_elapsed_time("published", start_time)
