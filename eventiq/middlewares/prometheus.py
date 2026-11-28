from __future__ import annotations

from typing import TYPE_CHECKING, Any

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)

from .metrics import MetricsMiddleware

if TYPE_CHECKING:
    from eventiq import Service

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


class PrometheusMiddleware(MetricsMiddleware):
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
        super().__init__(service, prefix=prefix)
        if registry is None:
            from prometheus_client import REGISTRY

            registry = REGISTRY
        self.registry = registry
        self.run_server = run_server
        self.server_port = server_port
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
            buckets=buckets,
        )
        # metric name (as used by MetricsMiddleware's hooks) -> counter, for the
        # ones that follow the plain "service, consumer" label shape.
        self._counters: dict[str, Counter] = {
            "total": self.total_messages,
            "skipped": self.total_skipped_messages,
            "retried": self.total_retried_messages,
            "errored": self.total_errored_messages,
            "failed": self.total_failed_messages,
        }

    def _inc(self, metric: str, consumer: str | None = None, value: int = 1) -> None:
        if metric == "in_progress":
            self.in_progress.labels(self.service.name, consumer).inc(value)
        elif metric == "published":
            self.total_messages_published.labels(self.service.name).inc(value)
        else:
            self._counters[metric].labels(self.service.name, consumer).inc(value)

    def _observe_duration(self, consumer: str, elapsed: float) -> None:
        self.message_durations.labels(self.service.name, consumer).observe(
            elapsed * 1000
        )

    async def before_broker_connect(self) -> None:
        if self.run_server:
            start_http_server(
                self.server_port,
                registry=self.registry,
                **self.http_server_options,
            )
