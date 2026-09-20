from __future__ import annotations

from collections import OrderedDict
from logging import INFO
from time import perf_counter
from typing import TYPE_CHECKING, Any, Literal, TypeVar

from eventiq.middleware import CloudEventType, Middleware

if TYPE_CHECKING:
    from collections.abc import Mapping

    from eventiq import Consumer, Service
    from eventiq.types import ID

DEFAULT_MAX_PENDING = 10_000

_OPERATION_MESSAGES: Mapping[str, str] = {
    "processed": "Message processed",
    "published": "Message published",
}

_K = TypeVar("_K")


class PerfCounterMiddleware(Middleware[CloudEventType]):
    """Logs how long message processing and publishing take."""

    def __init__(
        self,
        service: Service,
        log_level: int = INFO,
        max_pending: int = DEFAULT_MAX_PENDING,
    ) -> None:
        super().__init__(service)
        self.log_level = log_level
        # A publish that raises never reaches after_publish, so entries are evicted
        # oldest-first to keep a failing broker from growing these unboundedly.
        self.max_pending = max_pending
        self._receive_registry: OrderedDict[tuple[str, ID], float] = OrderedDict()
        self._publish_registry: OrderedDict[ID, float] = OrderedDict()

    def _record(self, registry: OrderedDict[_K, float], key: _K) -> None:
        registry[key] = perf_counter()
        while len(registry) > self.max_pending:
            registry.popitem(last=False)

    def _log_elapsed_time(
        self,
        operation: Literal["processed", "published"],
        start_time: float,
    ) -> None:
        self.logger.log(
            self.log_level,
            _OPERATION_MESSAGES[operation],
            extra={
                "operation": operation,
                "elapsed_seconds": perf_counter() - start_time,
            },
        )

    async def before_process_message(
        self,
        *,
        consumer: Consumer,
        message: CloudEventType,
    ) -> None:
        self._record(self._receive_registry, (consumer.name, message.id))

    async def after_message_finalized(
        self,
        *,
        consumer: Consumer,
        message: CloudEventType,
        **_: Any,
    ) -> None:
        start_time = self._receive_registry.pop(
            (consumer.name, message.id),
            perf_counter(),
        )
        self._log_elapsed_time("processed", start_time)

    async def before_publish(self, *, message: CloudEventType, **_: Any) -> None:
        self._record(self._publish_registry, message.id)

    async def after_publish(self, *, message: CloudEventType, **_: Any) -> None:
        start_time = self._publish_registry.pop(message.id, perf_counter())
        self._log_elapsed_time("published", start_time)
