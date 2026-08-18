from __future__ import annotations

import time
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from opentelemetry.metrics import MeterProvider, get_meter
from opentelemetry.propagate import extract, inject
from opentelemetry.propagators.textmap import Getter, Setter
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import (
    Span,
    SpanKind,
    StatusCode,
    TracerProvider,
    get_tracer,
    use_span,
)

from eventiq.middleware import Middleware
from eventiq.models import CloudEvent

from .model import TraceContextCloudEvent
from .version import __version__

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    from eventiq.consumer import Consumer
    from eventiq.service import Service
    from eventiq.types import ID


class EventiqGetter(Getter[TraceContextCloudEvent]):
    def get(self, carrier: TraceContextCloudEvent, key: str) -> list[str] | None:
        val = carrier.tracecontext.get(key, None)
        if val is None:
            return None
        if isinstance(val, Iterable) and not isinstance(val, str):
            return list(val)
        return [val]

    def keys(self, carrier: TraceContextCloudEvent) -> list[str]:
        return list(carrier.tracecontext.keys())


class EventiqSetter(Setter[TraceContextCloudEvent]):
    def set(self, carrier: TraceContextCloudEvent, key: str, value: str) -> None:
        carrier.tracecontext[key] = value


eventiq_getter = EventiqGetter()
eventiq_setter = EventiqSetter()


class OpenTelemetryTracingMiddleware(Middleware[TraceContextCloudEvent]):
    requires = TraceContextCloudEvent

    def __init__(
        self,
        service: Service,
        *,
        tracer_provider: TracerProvider | None = None,
        record_exceptions: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(service, **kwargs)
        self.tracer = get_tracer("eventiq", __version__, tracer_provider)
        self.record_exceptions = record_exceptions
        self.process_span_registry: dict[
            int, tuple[Span, AbstractContextManager[Span]]
        ] = {}
        self.publish_span_registry: dict[
            ID, tuple[Span, AbstractContextManager[Span]]
        ] = {}

    @staticmethod
    def _get_span_attributes(
        message: TraceContextCloudEvent, **extra: str
    ) -> dict[str, str]:
        return {
            SpanAttributes.CLOUDEVENTS_EVENT_SPEC_VERSION: message.specversion,
            SpanAttributes.CLOUDEVENTS_EVENT_TYPE: message.type or "CloudEvent",
            SpanAttributes.CLOUDEVENTS_EVENT_ID: str(message.id),
            SpanAttributes.CLOUDEVENTS_EVENT_SOURCE: message.source or "unknown",
            SpanAttributes.CLOUDEVENTS_EVENT_SUBJECT: message.topic,
            **message.extra_span_attributes,
            **extra,
        }

    async def before_process_message(
        self, *, consumer: Consumer, message: TraceContextCloudEvent
    ) -> None:
        trace_ctx = extract(message, getter=eventiq_getter)
        extra = self.service.broker.get_message_metadata(message.raw)  # type: ignore[attr-defined]
        span_name = f"{consumer.name} receive"
        span_attributes = self._get_span_attributes(message, **extra)
        num_delivered = self.service.broker.get_num_delivered(message.raw) or 0
        if num_delivered > 1:
            span_name += f" (retry: {num_delivered})"
            span_attributes["retries"] = str(num_delivered)
        span = self.tracer.start_span(
            name=span_name,
            kind=SpanKind.CONSUMER,
            context=trace_ctx,
            attributes=span_attributes,
        )
        activation = use_span(span, end_on_exit=True)
        activation.__enter__()
        self.process_span_registry[id(message)] = (
            span,
            activation,
        )

    async def after_message_finalized(
        self,
        *,
        message: TraceContextCloudEvent,
        exc: Exception | None = None,
        **_: Any,
    ) -> None:
        span, activation = self.process_span_registry.pop(id(message), (None, None))
        if not (span and activation):
            self.logger.warning("No active span was found")
            return

        if span.is_recording():
            if exc is None:
                span.set_status(StatusCode.OK)
            else:
                if self.record_exceptions:
                    span.record_exception(exc)
                span.set_status(StatusCode.ERROR, description=str(exc))

        activation.__exit__(None, None, None)

    async def before_publish(
        self, *, message: TraceContextCloudEvent, **_: Any
    ) -> None:
        trace_ctx = None
        if message.tracecontext:
            trace_ctx = extract(message, getter=eventiq_getter)
        source = message.source or self.service.name

        span = self.tracer.start_span(
            f"{source} publish",
            kind=SpanKind.PRODUCER,
            context=trace_ctx,
            attributes=self._get_span_attributes(message),
        )
        activation = use_span(span, end_on_exit=True)
        activation.__enter__()
        self.publish_span_registry[message.id] = (span, activation)

        inject(message, setter=eventiq_setter)

    async def after_publish(self, *, message: TraceContextCloudEvent, **_: Any) -> None:
        span, activation = self.publish_span_registry.pop(message.id, (None, None))
        if span and span.is_recording():
            span.set_status(StatusCode.OK)
        if activation is not None:
            activation.__exit__(None, None, None)


class OpentelemetryMetricsMiddleware(Middleware[CloudEvent]):
    def __init__(
        self,
        service: Service,
        meter_provider: MeterProvider | None = None,
        prefix: str = "eventiq",
        **kwargs: Any,
    ) -> None:
        super().__init__(service, **kwargs)
        self.prefix = prefix
        self.meter = get_meter("eventiq", __version__, meter_provider)
        self.in_progress = self.meter.create_up_down_counter(
            self.format("messages_in_progress"),
            "Total number of messages being processed",
        )
        self.total = self.meter.create_counter(
            self.format("messages_total"), "Total number of messages processed"
        )
        self.total_skipped = self.meter.create_counter(
            self.format("messages_skipped"), "Total number of messages skipped"
        )
        self.total_failed = self.meter.create_counter(
            self.format("messages_failed"), "Total number of messages failed"
        )
        self.total_retried = self.meter.create_counter(
            self.format("messages_retried"), "Total number of errored messages"
        )
        self.total_published = self.meter.create_counter(
            self.format("messages_published"), "Total number of messages published"
        )
        self.message_durations = self.meter.create_histogram(
            self.format("messages_duration"), "Message processing durations"
        )
        self.message_start_times: dict[tuple[str, ID], int] = {}

    def format(self, value: str) -> str:
        return f"{self.prefix}_{value}" if self.prefix else value

    @staticmethod
    def current_millis() -> int:
        return time.monotonic_ns() // 1000

    async def before_process_message(
        self, *, consumer: Consumer, message: CloudEvent
    ) -> None:
        self.in_progress.add(
            1,
            {
                "service": self.service.name,
                "consumer": consumer.name,
            },
        )
        self.message_start_times[(consumer.name, message.id)] = self.current_millis()

    async def after_process_message(
        self,
        *,
        consumer: Consumer,
        message: CloudEvent,
        **_: Any,
    ) -> None:
        attributes = {
            "service": self.service.name,
            "consumer": consumer.name,
        }
        self.in_progress.add(-1, attributes)
        message_start_time = self.message_start_times.pop(
            (consumer.name, message.id), self.current_millis()
        )
        self.message_durations.record(message_start_time, attributes)
        self.total.add(1, attributes)

    async def after_publish(self, **_: Any) -> None:
        self.total_published.add(1, {"service": self.service.name})

    async def after_skip_message(
        self,
        *,
        consumer: Consumer,
        **_: Any,
    ) -> None:
        self.total_skipped.add(
            1,
            {
                "service": self.service.name,
                "consumer": consumer.name,
            },
        )

    async def after_fail_message(
        self,
        *,
        consumer: Consumer,
        **_: Any,
    ) -> None:
        self.total_failed.add(
            1,
            {
                "service": self.service.name,
                "consumer": consumer.name,
            },
        )

    async def after_retry_message(
        self,
        *,
        consumer: Consumer,
        **_: Any,
    ) -> None:
        self.total_retried.add(
            1,
            {
                "service": self.service.name,
                "consumer": consumer.name,
            },
        )
