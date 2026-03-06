import asyncio
from contextlib import asynccontextmanager
from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock

import anyio
import pytest

from eventiq import CloudEvent, Service
from eventiq.backends.stub import StubBroker, StubMessage
from eventiq.consumer import ConsumerGroup
from eventiq.context import set_current_service
from eventiq.exceptions import DecodeError, Fail, Retry, Skip
from eventiq.middleware import Middleware


def test_service(service: Service):
    assert isinstance(service, Service)
    assert isinstance(service.broker, StubBroker)
    assert service.name == "test_service"


def test_consumers_property(service, test_consumer):
    assert "test_consumer" in service.consumers


def test_add_consumer_group(service):
    group = ConsumerGroup()

    async def my_handler(message: CloudEvent) -> None:
        pass

    group.subscribe(my_handler, topic="new_topic")
    service.add_consumer_group(group)
    assert "my_handler" in service.consumers


def test_prepare_message_sets_content_type(service, ce):
    topic, body, _ = service.prepare_message(ce)
    assert ce.content_type == service.encoder.CONTENT_TYPE
    assert ce.headers["Content-Type"] == service.encoder.CONTENT_TYPE
    assert isinstance(body, bytes)
    assert topic == ce.topic


def test_prepare_message_custom_topic(service, ce):
    topic, _, _ = service.prepare_message(ce, topic="override.topic")
    assert topic == "override.topic"
    assert ce.headers["Destination"] == "override.topic"


def test_prepare_message_custom_headers(service, ce):
    service.prepare_message(ce, headers={"X-Custom": "value"})
    assert ce.headers["X-Custom"] == "value"


@pytest.mark.anyio
async def test_consumer_called(
    running_service: Service, ce: CloudEvent, mock_consumer: Any
):
    await running_service.publish(ce)
    mock_consumer.assert_called_once_with(ce)


@pytest.mark.anyio
async def test_bulk_publish(
    running_service: Service, ce: CloudEvent, mock_consumer: MagicMock
):
    messages = [ce] * 10
    for message in messages:
        assert "Content-Type" not in message.headers
        assert message.source == running_service.name
    await running_service.bulk_publish(messages)
    assert mock_consumer.call_count == 10
    for message in messages:
        assert message.headers["Content-Type"] == "application/json"


@pytest.mark.anyio
async def test_send_with_string_type(running_service, mock_consumer):
    await running_service.send({"key": "value"}, type="MyEvent", topic="test_topic")
    ce = mock_consumer.call_args[0][0]
    assert isinstance(ce, CloudEvent)
    assert ce.type == "MyEvent"


@pytest.mark.anyio
async def test_send_sets_source(running_service, mock_consumer):
    await running_service.send({"x": 1}, type="TestEvent", topic="test_topic")
    assert mock_consumer.call_args[0][0].source == running_service.name


@pytest.mark.anyio
async def test_context_connects_and_disconnects():
    broker = StubBroker()
    svc = Service(name="ctx_test", broker=broker)
    assert not broker.is_connected
    async with svc.context():
        assert broker.is_connected
    assert not broker.is_connected


@pytest.mark.anyio
async def test_send_with_class_event_type(running_service, mock_consumer):
    class MyTopicEvent(CloudEvent[dict], topic="test_topic"):
        pass

    await running_service.send({"k": "v"}, type=MyTopicEvent)
    received = mock_consumer.call_args[0][0]
    assert received.type == "MyTopicEvent"


@pytest.mark.anyio
async def test_send_with_headers_kwarg(running_service, mock_consumer):
    await running_service.send(
        {"x": 1}, type="T", topic="test_topic", headers={"X-Foo": "bar"}
    )
    received = mock_consumer.call_args[0][0]
    assert received.headers.get("X-Foo") == "bar"


def test_default_middlewares_applied():
    added = []

    class TrackMiddleware(Middleware):
        def __init__(self, service) -> None:
            super().__init__(service)
            added.append(True)

    class MyService(Service):
        default_middlewares: ClassVar = [TrackMiddleware]

    MyService(name="test_default", broker=StubBroker())
    assert len(added) == 1


@pytest.mark.anyio
async def test_dispatch_skips_middleware_by_requires(service, ce):
    called = []

    class SpecificEvent(CloudEvent[dict], topic="specific.topic"):
        pass

    class RequiresSpecificMiddleware(Middleware):
        requires = SpecificEvent

        async def before_process_message(
            self, *, consumer: Any, message: Any, **kwargs: Any
        ) -> None:
            called.append(message)

    saved = service.middlewares[:]
    service.middlewares.clear()
    service.middlewares.append(RequiresSpecificMiddleware(service))
    try:
        await service.dispatch_before(
            "process_message", consumer=MagicMock(), message=ce
        )
        assert called == []
    finally:
        service.middlewares[:] = saved


@pytest.mark.anyio
async def test_dispatch_skips_missing_method(service):
    # Event name not defined on any middleware → method not found path
    await service._dispatch("before_undefined_xyz_event")


# --- _handle_message_finalization paths ---
# Tested directly to cover retry/skip/fail/default branches without needing
# a running broker, by mocking ack/nack on the broker.


@pytest.fixture
def finalization_setup(service, ce):
    """Return (service, consumer_mock, message_with_raw) ready for finalization tests."""
    ce.set_raw("stub_raw", {})
    service.broker.ack = AsyncMock()
    service.broker.nack = AsyncMock()
    consumer = MagicMock()
    return service, consumer, ce


@pytest.mark.anyio
async def test_finalization_success_acks(finalization_setup):
    svc, consumer, message = finalization_setup
    await svc._handle_message_finalization(consumer, message, result=42, exc=None)
    svc.broker.ack.assert_called_once()


@pytest.mark.anyio
async def test_finalization_retry_nacks_with_delay(finalization_setup):
    svc, consumer, message = finalization_setup
    await svc._handle_message_finalization(
        consumer, message, result=None, exc=Retry(delay=10)
    )
    svc.broker.nack.assert_called_once_with(message.raw, 10)


@pytest.mark.anyio
async def test_finalization_skip_acks(finalization_setup):
    svc, consumer, message = finalization_setup
    await svc._handle_message_finalization(
        consumer, message, result=None, exc=Skip("skip")
    )
    svc.broker.ack.assert_called_once()


@pytest.mark.anyio
async def test_finalization_fail_acks(finalization_setup):
    svc, consumer, message = finalization_setup
    await svc._handle_message_finalization(
        consumer, message, result=None, exc=Fail("fail")
    )
    svc.broker.ack.assert_called_once()


@pytest.mark.anyio
async def test_finalization_generic_exc_calls_default_action(finalization_setup):
    svc, consumer, message = finalization_setup
    svc.default_action = AsyncMock()
    await svc._handle_message_finalization(
        consumer, message, result=None, exc=RuntimeError("boom")
    )
    svc.default_action.assert_called_once_with(consumer, message.raw)


@pytest.mark.anyio
async def test_finalization_dispatch_after_exception(finalization_setup):
    svc, consumer, message = finalization_setup
    original = svc._dispatch

    async def failing_dispatch(event: str, **kwargs: Any) -> None:
        if event == "after_process_message":
            msg = "dispatch failed"
            raise RuntimeError(msg)
        return await original(event, **kwargs)

    svc._dispatch = failing_dispatch
    svc.default_action = AsyncMock()
    await svc._handle_message_finalization(consumer, message, result=42, exc=None)
    # dispatch_after raises → exc reassigned → default_action called
    svc.default_action.assert_called_once_with(consumer, message.raw)


# --- publish_sync / bulk_publish_sync ---


@pytest.mark.anyio
async def test_publish_sync(
    running_service: Service, ce: CloudEvent, mock_consumer: Any
):
    await anyio.to_thread.run_sync(lambda: running_service.publish_sync(ce))
    mock_consumer.assert_called_once_with(ce)


@pytest.mark.anyio
async def test_bulk_publish_sync(
    running_service: Service, ce: CloudEvent, mock_consumer: Any
):
    await anyio.to_thread.run_sync(lambda: running_service.bulk_publish_sync([ce]))
    mock_consumer.assert_called_once_with(ce)


# --- _process regular exception path (line 429) ---


@pytest.mark.anyio
async def test_process_exception_captured(service: Service):
    """consumer.process raises regular Exception → captured in exc, passed to finalization."""
    ce: CloudEvent = CloudEvent.new({}, type="T", topic="test_topic", source="s")
    body = service.encoder.encode(ce)
    from eventiq.backends.stub import StubMessage

    raw = StubMessage(
        data=body, queue=asyncio.Queue(), event=asyncio.Event(), headers={}
    )

    consumer = MagicMock()
    consumer.event_type = CloudEvent
    consumer.decoder = None
    consumer.timeout = None
    consumer.process = AsyncMock(side_effect=ValueError("boom"))
    service.broker.ack = AsyncMock()  # type: ignore[method-assign]
    service.broker.nack = AsyncMock()  # type: ignore[method-assign]

    await service._process(consumer, raw, service.decoder, 10.0)
    # default_action called (ack or nack depending on broker default)
    assert service.broker.ack.called or service.broker.nack.called


# --- run() lifespan state (line 271) ---


@pytest.mark.anyio
async def test_run_merges_lifespan_state():
    """run() with a lifespan that yields state → state merged into service.state."""
    broker = StubBroker(wait_on_publish=False)

    @asynccontextmanager
    async def my_lifespan(_svc: Service) -> Any:
        yield {"db": "connection"}

    svc = Service(name="run_lifespan_test", broker=broker, lifespan=my_lifespan)
    # With no consumers and no signal handler, run() completes immediately
    await svc.run(enable_signal_handler=False)
    assert svc.state.get("db") == "connection"


# --- _process decode error paths (lines 399-413) ---


@pytest.mark.anyio
async def test_process_decode_error_acks(service: Service):
    """DecodeError with should_nack=False → acks the message."""
    consumer = MagicMock()
    consumer.event_type = CloudEvent
    consumer.decoder = None
    consumer.timeout = None

    service.broker.decode_message = MagicMock(side_effect=DecodeError("bad data"))  # type: ignore[method-assign]
    service.broker.ack = AsyncMock()  # type: ignore[method-assign]
    service.broker.nack = AsyncMock()  # type: ignore[method-assign]

    raw_msg = MagicMock()
    await service._process(consumer, raw_msg, service.decoder, 10.0)
    service.broker.ack.assert_called_once_with(raw_msg)
    service.broker.nack.assert_not_called()


@pytest.mark.anyio
async def test_process_decode_error_nacks_when_should_nack(service: Service):
    """DecodeError with should_nack=True → nacks the message."""
    consumer = MagicMock()
    consumer.event_type = CloudEvent
    consumer.decoder = None
    consumer.timeout = None

    service.broker.decode_message = MagicMock(side_effect=DecodeError("bad data"))  # type: ignore[method-assign]
    service.broker.should_nack = MagicMock(return_value=True)  # type: ignore[method-assign]
    service.broker.nack = AsyncMock()  # type: ignore[method-assign]
    service.broker.ack = AsyncMock()  # type: ignore[method-assign]

    raw_msg = MagicMock()
    await service._process(consumer, raw_msg, service.decoder, 10.0)
    service.broker.nack.assert_called_once()
    service.broker.ack.assert_not_called()


# --- _process cancellation nack path (lines 428-434) ---


@pytest.mark.anyio
async def test_process_cancellation_nacks():
    """CancelledError during process → nacks the raw message then re-raises."""
    broker = StubBroker(wait_on_publish=False)
    await broker.connect()
    broker.nack = AsyncMock()

    svc = Service(name="cancel_test", broker=broker)

    # Build a real encoded CloudEvent for decode_message to return
    ce = CloudEvent.new({}, type="T", topic="t", source="s")
    body = svc.encoder.encode(ce)

    queue: asyncio.Queue = asyncio.Queue()
    event = asyncio.Event()
    raw = StubMessage(data=body, queue=queue, event=event, headers={})

    consumer = MagicMock()
    consumer.event_type = CloudEvent
    consumer.decoder = None
    consumer.timeout = None
    consumer.process = AsyncMock(side_effect=anyio.get_cancelled_exc_class()())

    with pytest.raises(anyio.get_cancelled_exc_class()):
        await svc._process(consumer, raw, svc.decoder, 10.0)

    broker.nack.assert_called_once_with(raw)
    await broker.disconnect()
    # _process doesn't clear context on cancellation path; clean up manually
    set_current_service(None)


# --- resolved_func loop continuation (dependencies.py line 45->44) ---


@pytest.mark.anyio
async def test_multiple_state_params_injected(service: Service):
    """resolved_func with 2+ state-injectable params → loop iterates >1 (45->44 branch)."""
    from eventiq.dependencies import resolved_func

    received: list[tuple] = []
    service.state[str] = "hello"

    # x: float not in state → if-False branch (45->44) on non-last iteration
    # y: int IS in state → assigned → covers loop with a skipped-then-injected param
    async def multi_handler(message: CloudEvent, x: float = 0.0, y: int = 0) -> None:
        received.append((x, y))

    wrapped = resolved_func(multi_handler)
    ce: CloudEvent = CloudEvent.new({}, type="T", topic="test_topic")
    ce.set_raw(None, {})
    await wrapped(ce)
    assert received == [(0.0, 5)]


# --- context() lifespan state (lines 289) ---


@pytest.mark.anyio
async def test_context_with_lifespan_state():
    """Lifespan that yields state → state is merged into service.state."""
    broker = StubBroker(wait_on_publish=False)

    @asynccontextmanager
    async def my_lifespan(_svc: Service) -> Any:
        yield {"db": "connection"}

    svc = Service(name="lifespan_test", broker=broker, lifespan=my_lifespan)
    # Mock connect/disconnect to avoid spawning raw asyncio tasks outside anyio's control
    svc.connect = AsyncMock()
    svc.disconnect = AsyncMock()
    async with svc.context():
        assert svc.state.get("db") == "connection"


# --- subscription() context manager (lines 505-519) ---


@pytest.mark.anyio
async def test_subscription_context_manager():
    """subscription() delivers published messages to the user stream."""
    broker = StubBroker(wait_on_publish=False)
    svc = Service(name="sub_test", broker=broker)
    await broker.connect()

    class SubEvent(CloudEvent[dict], topic="sub.topic"):
        pass

    received = []

    async with svc.subscription(SubEvent) as stream:
        # Yield to let sender task register the topic queue
        await asyncio.sleep(0)
        await svc.publish(SubEvent.new({"x": 1}, source="sub_test"))
        msg, ack = await stream.receive()
        received.append(msg)
        ack()

    await broker.disconnect()
    assert len(received) == 1


# --- watch_for_signals / signal handler (lines 277, 302-305) ---


@pytest.mark.anyio
async def test_watch_for_signals_cancels_scope():
    """watch_for_signals receives SIGTERM → cancels the scope and exits (lines 302-305)."""
    import os
    import signal as signal_mod

    broker = StubBroker(wait_on_publish=False)
    svc = Service(name="signal_test", broker=broker)

    async def send_sigterm() -> None:
        await anyio.sleep(0.05)
        os.kill(os.getpid(), signal_mod.SIGTERM)

    async with anyio.create_task_group() as tg:
        tg.start_soon(send_sigterm)
        # Pass the real cancel scope; watch_for_signals will cancel it on signal
        await svc.watch_for_signals(tg.cancel_scope)
    # Reaching here means scope.cancel() was called → test passes


@pytest.mark.anyio
async def test_run_with_signal_handler():
    """run(enable_signal_handler=True) starts watch_for_signals (line 277)."""
    import os
    import signal as signal_mod

    broker = StubBroker(wait_on_publish=False)
    svc = Service(name="signal_run_test", broker=broker)

    async def send_sigterm() -> None:
        await anyio.sleep(0.05)
        os.kill(os.getpid(), signal_mod.SIGTERM)

    async with anyio.create_task_group() as tg:
        tg.start_soon(send_sigterm)
        await svc.run(enable_signal_handler=True)


@pytest.mark.anyio
async def test_context_with_signal_handler():
    """context(enable_signal_handler=True) starts watch_for_signals (line 294)."""
    import os
    import signal as signal_mod

    broker = StubBroker(wait_on_publish=False)
    svc = Service(name="signal_ctx_test", broker=broker)

    async def send_sigterm() -> None:
        await anyio.sleep(0.05)
        os.kill(os.getpid(), signal_mod.SIGTERM)

    async with anyio.create_task_group() as outer_tg:
        outer_tg.start_soon(send_sigterm)
        async with svc.context(enable_signal_handler=True):
            await anyio.sleep(10)  # cancelled by signal
