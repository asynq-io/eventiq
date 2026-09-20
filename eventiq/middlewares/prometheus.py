from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from eventiq.middleware import Middleware

if TYPE_CHECKING:
    from eventiq import CloudEvent, Consumer, Service
    from eventiq.exceptions import Retry

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)

DEFAULT_BUCKETS = (
    5,
    10,
    25,
    50,
    75,
    100,
    250,
    500,
    750,
    1000,
    2500,
    5000,
    7500,
    10000,
    30000,
    60000,
    600000,
    900000,
    float("inf"),
)


class PrometheusMiddleware(Middleware):
    def __init__(
        self,
        service: Service,
        *,
        registry: CollectorRegistry | None = None,
        run_server: bool = False,
        buckets: tuple[float, ...] = DEFAULT_BUCKETS,
        prefix: str = "eventiq",
        server_port: int = 8888,
        **http_server_options: Any,
    ) -> None:
        super().__init__(service)
        if registry is None:
            from prometheus_client import REGISTRY

            registry = REGISTRY
        self.registry = registry
        self.run_server = run_server
        self.buckets = buckets
        self.server_port = server_port
        self.message_start_times: dict[int, int] = {}
        self.prefix = prefix
        self.http_server_options = http_server_options

        self.in_progress = Gauge(
            self.format("messages_in_progress"),
            "Total number of messages being processed.",
            ["service", "consumer"],
            registry=self.registry,
        )
        self.total_messages_published = Counter(
            self.format("messages_published_total"),
            "Total number of messages published",
            ["service"],
            registry=self.registry,
        )
        self.total_messages = Counter(
            self.format("messages_total"),
            "Total number of messages processed.",
            ["service", "consumer"],
            registry=self.registry,
        )
        self.total_skipped_messages = Counter(
            self.format("messages_skipped_total"),
            "Total number of messages skipped processing.",
            ["service", "consumer"],
            registry=self.registry,
        )
        self.total_retried_messages = Counter(
            self.format("message_retried_total"),
            "Total number of errored messages.",
            ["service", "consumer"],
            registry=self.registry,
        )
        self.total_errored_messages = Counter(
            self.format("message_errored_total"),
            "Total number of errored messages.",
            ["service", "consumer"],
            registry=self.registry,
        )
        self.total_failed_messages = Counter(
            self.format("message_failed_total"),
            "Total number of messages failed",
            ["service", "consumer"],
            registry=self.registry,
        )
        self.message_durations = Histogram(
            self.format("message_duration_ms"),
            "Time spend processing message",
            ["service", "consumer"],
            registry=self.registry,
            buckets=self.buckets,
        )

    @staticmethod
    def current_millis() -> int:
        return time.monotonic_ns() // 1000

    def format(self, value: str) -> str:
        return f"{self.prefix}_{value}" if self.prefix else value

    async def before_process_message(
        self, *, consumer: Consumer, message: CloudEvent
    ) -> None:
        labels = (self.service.name, consumer.name)
        self.in_progress.labels(*labels).inc()
        self.message_start_times[id(message)] = self.current_millis()

    async def after_message_finalized(
        self,
        *,
        consumer: Consumer,
        message: CloudEvent,
        **_: Any,
    ) -> None:
        labels = (self.service.name, consumer.name)
        self.in_progress.labels(*labels).dec()
        self.total_messages.labels(*labels).inc()
        message_start_time = self.message_start_times.pop(
            id(message), self.current_millis()
        )
        message_duration = self.current_millis() - message_start_time
        self.message_durations.labels(*labels).observe(message_duration)

    async def after_retry_message(
        self,
        *,
        consumer: Consumer,
        exc: Retry,
        **_: Any,
    ) -> None:
        labels = (self.service.name, consumer.name)
        self.total_retried_messages.labels(*labels).inc()
        if exc.__cause__ is not None:
            self.total_errored_messages.labels(*labels).inc()

    async def after_skip_message(
        self,
        *,
        consumer: Consumer,
        **_: Any,
    ) -> None:
        labels = (self.service.name, consumer.name)
        self.total_skipped_messages.labels(*labels).inc()

    async def after_fail_message(
        self,
        *,
        consumer: Consumer,
        **_: Any,
    ) -> None:
        labels = (self.service.name, consumer.name)
        self.total_failed_messages.labels(*labels).inc()

    async def after_publish(self, **_: Any) -> None:
        labels = (self.service.name,)
        self.total_messages_published.labels(*labels).inc()

    async def before_broker_connect(self) -> None:
        if self.run_server:
            start_http_server(
                self.server_port,
                registry=self.registry,
                **self.http_server_options,
            )
