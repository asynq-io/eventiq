import pytest

from eventiq.opentelemetry.middlewares import OpenTelemetryTracingMiddleware
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
