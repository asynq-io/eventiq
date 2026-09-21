import pytest

from eventiq.opentelemetry.middlewares import (
    OpentelemetryMetricsMiddleware,
    OpenTelemetryTracingMiddleware,
)
from eventiq.opentelemetry.model import TraceContextCloudEvent


@pytest.fixture
def trace_message():
    ce = TraceContextCloudEvent.new({}, type="T", topic="test_topic", source="s")
    ce.set_raw("stub_raw", {})
    return ce


@pytest.mark.anyio
async def test_tracing_middleware_works_with_a_broker_without_message_metadata(
    service, trace_message, mock_consumer
):
    """`before_process_message` must not depend on a broker-specific method.

    Regression test for 1.2: `get_message_metadata` used to be called with a
    `# type: ignore[attr-defined]` because only nats/rabbitmq/kafka implemented
    it, so this raised `AttributeError` against `StubBroker` (and `RedisBroker`)
    on every message.
    """
    middleware = OpenTelemetryTracingMiddleware(service)
    consumer = mock_consumer

    await middleware.before_process_message(consumer=consumer, message=trace_message)


@pytest.mark.anyio
async def test_metrics_middleware_records_a_plausible_elapsed_duration(
    service, trace_message, mock_consumer
):
    """The histogram must observe an elapsed duration, not a raw start timestamp.

    Regression test for 1.3: `OpentelemetryMetricsMiddleware` used to record
    `message_start_time` (a `time.monotonic_ns() // 1000` timestamp) directly,
    so the histogram emitted huge monotonic-clock values instead of durations.
    """
    middleware = OpentelemetryMetricsMiddleware(service)
    recorded: list[float] = []
    middleware.message_durations.record = lambda value, *_a, **_kw: recorded.append(
        value
    )

    await middleware.before_process_message(
        consumer=mock_consumer, message=trace_message
    )
    await middleware.after_message_finalized(
        consumer=mock_consumer, message=trace_message, elapsed=0.005
    )

    assert recorded == [0.005]
    assert 0 <= recorded[0] < 60
