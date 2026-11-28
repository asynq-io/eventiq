from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from eventiq.middleware import Middleware

if TYPE_CHECKING:
    from eventiq import Consumer, Service


class MetricsMiddleware(Middleware, ABC):
    """Backend-agnostic message metrics: counts and durations, by consumer.

    `PrometheusMiddleware` and `OpentelemetryMetricsMiddleware` implement the
    same model against different client libraries. This base owns the hook
    wiring once; a subclass only maps the logical metric names below onto its
    backend's instruments, via `_inc` and `_observe_duration`:

    `in_progress`, `total`, `skipped`, `failed`, `retried`, `errored`, `published`.
    """

    def __init__(self, service: Service, *, prefix: str = "eventiq") -> None:
        super().__init__(service)
        self.prefix = prefix

    def format(self, value: str) -> str:
        return f"{self.prefix}_{value}" if self.prefix else value

    @abstractmethod
    def _inc(self, metric: str, consumer: str | None = None, value: int = 1) -> None:
        """Add `value` to `metric`, labelled by service and, if given, consumer."""

    @abstractmethod
    def _observe_duration(self, consumer: str, elapsed: float) -> None:
        """Record a message-processing duration. `elapsed` is in seconds."""

    async def before_process_message(self, *, consumer: Consumer, **_: Any) -> None:
        self._inc("in_progress", consumer.name)

    async def after_message_finalized(
        self,
        *,
        consumer: Consumer,
        elapsed: float = 0.0,
        **_: Any,
    ) -> None:
        self._inc("in_progress", consumer.name, -1)
        self._inc("total", consumer.name)
        self._observe_duration(consumer.name, elapsed)

    async def after_skip_message(self, *, consumer: Consumer, **_: Any) -> None:
        self._inc("skipped", consumer.name)

    async def after_fail_message(self, *, consumer: Consumer, **_: Any) -> None:
        self._inc("failed", consumer.name)

    async def after_retry_message(
        self,
        *,
        consumer: Consumer,
        exc: Exception | None = None,
        **_: Any,
    ) -> None:
        self._inc("retried", consumer.name)
        if exc is not None and exc.__cause__ is not None:
            self._inc("errored", consumer.name)

    async def after_publish(self, **_: Any) -> None:
        self._inc("published")
