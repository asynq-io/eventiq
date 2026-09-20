from __future__ import annotations

from typing import Any

from opentelemetry.instrumentation.instrumentor import (  # type: ignore[attr-defined]
    BaseInstrumentor,
)

from eventiq.service import Service

from .middlewares import OpentelemetryMetricsMiddleware, OpenTelemetryTracingMiddleware
from .model import TraceContextCloudEvent
from .package import _instruments
from .version import __version__


class OpentelemetryMiddlewareFactory:
    def __init__(
        self,
        cls: type[OpentelemetryMetricsMiddleware | OpenTelemetryTracingMiddleware],
        **kwargs: Any,
    ) -> None:
        self.cls = cls
        self.kwargs = kwargs

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        kwargs.update(self.kwargs)
        return self.cls(*args, **kwargs)


class EventiqInstrumentator(BaseInstrumentor):
    def instrumentation_dependencies(self) -> tuple[str, ...]:
        return _instruments

    def instrument_service(
        self, service: Service, *, enable_metrics: bool = True, **kwargs: Any
    ) -> None:
        tracer_provider = kwargs.get("tracer_provider")
        record_exceptions = kwargs.get("record_exceptions", True)
        service.add_middleware(
            OpenTelemetryTracingMiddleware,
            tracer_provider=tracer_provider,
            record_exceptions=record_exceptions,
        )
        if enable_metrics:
            meter_provider = kwargs.get("meter_provider")
            service.add_middleware(
                OpentelemetryMetricsMiddleware, meter_provider=meter_provider
            )

    def uninstrument_service(self, service: Service) -> None:
        service.middlewares = [
            m
            for m in service.middlewares
            if not isinstance(
                m, OpentelemetryMetricsMiddleware | OpenTelemetryTracingMiddleware
            )
        ]

    def _instrument(self, **kwargs: Any) -> None:
        tracer_provider = kwargs.get("tracer_provider")
        record_exceptions = kwargs.get("record_exceptions", True)
        Service.default_middlewares.append(
            OpentelemetryMiddlewareFactory(
                OpenTelemetryTracingMiddleware,
                tracer_provider=tracer_provider,
                record_exceptions=record_exceptions,
            )
        )
        meter_provider = kwargs.get("meter_provider")
        Service.default_middlewares.append(
            OpentelemetryMiddlewareFactory(
                OpentelemetryMetricsMiddleware, meter_provider=meter_provider
            )
        )

    def _uninstrument(self, **_: Any) -> None:
        Service.default_middlewares = [
            m
            for m in Service.default_middlewares
            if not isinstance(m, OpentelemetryMiddlewareFactory)
        ]


__all__ = [
    "EventiqInstrumentator",
    "OpenTelemetryTracingMiddleware",
    "OpentelemetryMetricsMiddleware",
    "TraceContextCloudEvent",
    "__version__",
]
