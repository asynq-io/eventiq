import asyncio
import sys
from contextlib import asynccontextmanager
from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock

import anyio
import pytest

if sys.version_info < (3, 11):
    from exceptiongroup import BaseExceptionGroup

from eventiq import CloudEvent, Service
from eventiq.backends.stub import StubBroker, StubMessage
from eventiq.consumer import ConsumerGroup, FnConsumer
from eventiq.context import (
    current_service,
    find_current_service,
    get_current_service,
    reset_current_service,
    set_current_service,
)
from eventiq.dependencies import resolved_func
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


# --- message_source must always reach the broker: it is a required argument ---


def test_prepare_message_sets_source_when_missing(service, ce):
    assert ce.source is None
    _, _, message_kwargs = service.prepare_message(ce)
    assert ce.source == service.name
    assert message_kwargs["message_source"] == service.name


def test_prepare_message_keeps_existing_source(service):
    ce = CloudEvent.new({"x": 1}, type="T", topic="test_topic", source="other_service")
    _, _, message_kwargs = service.prepare_message(ce)
    assert message_kwargs["message_source"] == "other_service"


@pytest.mark.anyio
async def test_publish_sets_message_source_without_ambient_service(service: Service):
    """The `eventiq send` path: an event built with no service ambient still gets a source."""
    token = set_current_service(None)
    try:
        message = CloudEvent.new({"x": 1}, type="T", topic="test_topic")
        assert message.source is None
        service.broker.publish = AsyncMock()  # type: ignore[method-assign]
        await service.publish(message)
    finally:
        reset_current_service(token)
    assert service.broker.publish.call_args.kwargs["message_source"] == service.name


@pytest.mark.anyio
async def test_publish_uses_publishing_service_as_source(service: Service):
    """The publisher names the source, not whichever service happens to be ambient."""
    other = Service(name="other_service", broker=StubBroker(wait_on_publish=False))
    other.broker.publish = AsyncMock()  # type: ignore[method-assign]
    message = CloudEvent.new({"x": 1}, type="T", topic="test_topic")

    await other.publish(message)

    assert message.source == "other_service"
    assert other.broker.publish.call_args.kwargs["message_source"] == "other_service"


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
        assert message.source is None
    await running_service.bulk_publish(messages)
    assert mock_consumer.call_count == 10
    for message in messages:
        assert message.headers["Content-Type"] == "application/json"
        assert message.source == running_service.name


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
async def test_context_exits_when_consumers_are_registered():
    """Consumer tasks never return on their own, so the task group must be cancelled.

    Without that, `context()` blocks on exit and `disconnect` is never reached.
    """
    broker = StubBroker()
    svc = Service(name="ctx_consumers_test", broker=broker)

    async def handler(message: CloudEvent) -> None:
        return None

    svc.subscribe(handler, topic="ctx.consumers.topic")

    with anyio.fail_after(1):
        async with svc.context():
            assert broker.is_connected

    assert not broker.is_connected


@pytest.mark.anyio
async def test_context_exit_dispatches_close_consumers():
    """Shutting down via `context` announces it, just as `watch_for_signals` does."""
    calls: list[str] = []

    class RecordingMiddleware(Middleware):
        async def before_close_consumers(self) -> None:
            calls.append("close_consumers")

    broker = StubBroker()
    svc = Service(name="ctx_close_test", broker=broker)
    svc.add_middleware(RecordingMiddleware)

    async def handler(message: CloudEvent) -> None:
        return None

    svc.subscribe(handler, topic="ctx.close.topic")

    with anyio.fail_after(1):
        async with svc.context():
            assert calls == []

    assert calls == ["close_consumers"]


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

    broker.nack.assert_called_once_with(raw, None)
    await broker.disconnect()


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


# --- the ambient service must reach tasks that did not start it ---


@pytest.mark.anyio
async def test_service_visible_from_task_that_did_not_start_it():
    """ASGI integration: the lifespan starts the service, sibling tasks serve requests."""
    broker = StubBroker(wait_on_publish=False)
    svc = Service(name="sibling_test", broker=broker)
    started = anyio.Event()

    async def lifespan_task() -> None:
        async with svc.context():
            started.set()
            await anyio.sleep(10)

    async with anyio.create_task_group() as tg:
        tg.start_soon(lifespan_task)
        with anyio.fail_after(1):
            await started.wait()

        assert get_current_service() is svc
        # the README's FastAPI route usage, from outside the lifespan task
        await CloudEvent.new({"x": 1}, type="T", topic="sibling.topic").publish()

        tg.cancel_scope.cancel()

    with pytest.raises(RuntimeError, match="outside of context"):
        get_current_service()


@pytest.mark.anyio
async def test_task_local_service_overrides_the_default(service: Service):
    """A service set for the current task wins over the process-wide fallback."""
    other = Service(name="other_service", broker=StubBroker(wait_on_publish=False))
    async with other.context():
        token = set_current_service(service)
        try:
            assert get_current_service() is service
        finally:
            reset_current_service(token)
        assert get_current_service() is other


@pytest.mark.anyio
async def test_concurrent_services_restore_the_fallback_out_of_order():
    """Two services running at once: stopping one keeps the other's fallback."""
    svc_a = Service(name="fallback_a", broker=StubBroker())
    svc_b = Service(name="fallback_b", broker=StubBroker())

    async def run(
        svc: Service, started: anyio.Event, stop: anyio.Event, stopped: anyio.Event
    ) -> None:
        with current_service(svc):
            started.set()
            await stop.wait()
        stopped.set()

    started_a, stop_a, stopped_a = anyio.Event(), anyio.Event(), anyio.Event()
    started_b, stop_b, stopped_b = anyio.Event(), anyio.Event(), anyio.Event()

    with anyio.fail_after(1):
        async with anyio.create_task_group() as tg:
            tg.start_soon(run, svc_a, started_a, stop_a, stopped_a)
            await started_a.wait()
            tg.start_soon(run, svc_b, started_b, stop_b, stopped_b)
            await started_b.wait()

            assert find_current_service() is svc_b

            stop_a.set()
            await stopped_a.wait()
            assert find_current_service() is svc_b

            stop_b.set()
            await stopped_b.wait()
            assert find_current_service() is None


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


# --- nack middleware hooks ---


@pytest.mark.anyio
async def test_nack_dispatches_before_and_after_once(service: Service):
    """nack fires before_nack once and after_nack once, in that order."""
    calls: list[str] = []

    class RecordingMiddleware(Middleware):
        async def before_nack(self, **_: Any) -> None:
            calls.append("before")

        async def after_nack(self, **_: Any) -> None:
            calls.append("after")

    service.middlewares.clear()
    service.add_middleware(RecordingMiddleware)
    service.broker.nack = AsyncMock()  # type: ignore[method-assign]

    async def noop_handler(message: CloudEvent) -> None:
        return None

    consumer = FnConsumer(
        fn=noop_handler, event_type=CloudEvent, topic="test_topic", name="c"
    )

    await service.nack(consumer, "raw-message")

    assert calls == ["before", "after"]


# --- a finalization the broker never completed must not be reported as one ---


@pytest.mark.parametrize("action", ["ack", "nack"])
@pytest.mark.anyio
async def test_after_hook_skipped_when_broker_finalization_times_out(
    service: Service, action: str
):
    """A timed-out ack/nack leaves the message unacknowledged: no `after_*` hook."""
    calls: list[str] = []

    class RecordingMiddleware(Middleware):
        async def before_ack(self, **_: Any) -> None:
            calls.append("before_ack")

        async def after_ack(self, **_: Any) -> None:
            calls.append("after_ack")

        async def before_nack(self, **_: Any) -> None:
            calls.append("before_nack")

        async def after_nack(self, **_: Any) -> None:
            calls.append("after_nack")

    service.middlewares.clear()
    service.add_middleware(RecordingMiddleware)
    service.finalization_timeout = 0

    async def never_completes(*_: Any) -> None:
        await anyio.sleep(10)

    setattr(service.broker, action, never_completes)

    await getattr(service, action)(MagicMock(), "raw-message")

    assert calls == [f"before_{action}"]


# --- the ambient service must be restored, not overwritten, by _process ---


@pytest.mark.anyio
async def test_process_restores_ambient_service(service: Service):
    """`_process` may be driven from a task shared with unrelated code."""
    other = Service(name="ambient_other", broker=StubBroker(wait_on_publish=False))
    other.broker.ack = AsyncMock()  # type: ignore[method-assign]
    other.broker.nack = AsyncMock()  # type: ignore[method-assign]
    seen: list[Service] = []

    async def handler(message: CloudEvent) -> None:
        seen.append(get_current_service())

    consumer = FnConsumer(
        fn=handler, event_type=CloudEvent, topic="test_topic", name="ambient_consumer"
    )
    ce: CloudEvent = CloudEvent.new({}, type="T", topic="test_topic", source="s")
    raw = StubMessage(
        data=other.encoder.encode(ce),
        queue=asyncio.Queue(),
        event=asyncio.Event(),
        headers={},
    )

    token = set_current_service(service)
    try:
        await other._process(consumer, raw, other.decoder, 10.0)
        assert seen == [other]
        assert get_current_service() is service
    finally:
        reset_current_service(token)


# --- importable without installed distribution metadata ---


def test_version_falls_back_without_distribution_metadata(monkeypatch):
    """A source checkout or zipapp has no `.dist-info`: importing must still work."""
    import importlib
    import importlib.metadata

    import eventiq

    def raise_not_found(name: str) -> str:
        raise importlib.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(importlib.metadata, "version", raise_not_found)
    try:
        assert importlib.reload(eventiq).__version__ == "0.0.0"
    finally:
        monkeypatch.undo()
        importlib.reload(eventiq)


# --- decode phase must not crash the receiver on unexpected errors ---


@pytest.mark.anyio
async def test_process_unexpected_decode_error_does_not_propagate(service: Service):
    """A non-DecodeError failure while decoding is contained, not raised."""

    boom = RuntimeError("decoder exploded")

    class BoomDecoder:
        CONTENT_TYPE = "application/json"

        def encode(self, data: Any) -> bytes:
            return b""

        def decode(self, data: str | bytes, as_type: type[Any] | None = None) -> Any:
            raise boom

    async def noop_handler(message: CloudEvent) -> None:
        return None

    consumer = FnConsumer(
        fn=noop_handler,
        event_type=CloudEvent,
        topic="test_topic",
        name="boom_consumer",
    )
    service.broker.ack = AsyncMock()  # type: ignore[method-assign]
    service.broker.nack = AsyncMock()  # type: ignore[method-assign]
    raw = StubMessage(
        data=b"{}", queue=asyncio.Queue(), event=asyncio.Event(), headers={}
    )

    # must not raise: a poison message may not take the service down
    await service._process(consumer, raw, BoomDecoder(), 10)

    assert service.broker.ack.called or service.broker.nack.called


@pytest.mark.anyio
async def test_process_unexpected_decode_error_nacks(service: Service):
    """An unknown decode failure is retried, not acked away (default should_nack=False)."""
    consumer = MagicMock()
    consumer.event_type = CloudEvent
    consumer.decoder = None
    consumer.timeout = None

    # e.g. GzipMiddleware handed a payload that was never gzipped
    service.broker.decode_message = MagicMock(side_effect=OSError("not a gzipped file"))  # type: ignore[method-assign]
    service.broker.ack = AsyncMock()  # type: ignore[method-assign]
    service.broker.nack = AsyncMock()  # type: ignore[method-assign]

    raw_msg = MagicMock()
    await service._process(consumer, raw_msg, service.decoder, 10.0)

    service.broker.nack.assert_called_once_with(
        raw_msg, service.handle_message_finalization_delay
    )
    service.broker.ack.assert_not_called()


# --- dependency injection with postponed annotations ---


# --- a delivery the broker already settled must not be settled again ---


@pytest.mark.anyio
async def test_failing_after_ack_hook_does_not_nack(finalization_setup):
    """`after_ack` runs once the broker acked: failing there cannot undo the ack."""

    class BrokenMiddleware(Middleware):
        async def after_ack(self, **_: Any) -> None:
            msg = "after_ack exploded"
            raise RuntimeError(msg)

    svc, consumer, message = finalization_setup
    svc.middlewares.clear()
    svc.add_middleware(BrokenMiddleware)

    await svc._handle_message_finalization_with_fallback(
        consumer, message, result=None, exc=None
    )

    svc.broker.ack.assert_called_once_with(message.raw)
    svc.broker.nack.assert_not_called()


@pytest.mark.anyio
async def test_failing_after_nack_hook_does_not_nack_twice(finalization_setup):
    """The same holds for `after_nack`: one delivery, one broker decision."""

    class BrokenMiddleware(Middleware):
        async def after_nack(self, **_: Any) -> None:
            msg = "after_nack exploded"
            raise RuntimeError(msg)

    svc, consumer, message = finalization_setup
    svc.middlewares.clear()
    svc.add_middleware(BrokenMiddleware)

    await svc._handle_message_finalization_with_fallback(
        consumer, message, result=None, exc=Retry(delay=5)
    )

    svc.broker.nack.assert_called_once_with(message.raw, 5)
    svc.broker.ack.assert_not_called()


# --- control flow exceptions must survive anyio's exception groups ---


@pytest.mark.anyio
async def test_retry_raised_in_nested_task_group_is_honoured(service: Service):
    """anyio wraps child task exceptions, hiding the `Retry` from the routing."""
    retried: list[Exception] = []

    class RecordingMiddleware(Middleware):
        async def after_retry_message(self, *, exc: Exception, **_: Any) -> None:
            retried.append(exc)

    service.add_middleware(RecordingMiddleware)
    service.broker.ack = AsyncMock()  # type: ignore[method-assign]
    service.broker.nack = AsyncMock()  # type: ignore[method-assign]

    async def child() -> None:
        raise Retry(delay=30)

    async def handler(message: CloudEvent) -> None:
        async with anyio.create_task_group() as tg:
            tg.start_soon(child)

    consumer = FnConsumer(
        fn=handler, event_type=CloudEvent, topic="test_topic", name="nested_consumer"
    )
    ce: CloudEvent = CloudEvent.new({}, type="T", topic="test_topic", source="s")
    raw = StubMessage(
        data=service.encoder.encode(ce),
        queue=asyncio.Queue(),
        event=asyncio.Event(),
        headers={},
    )

    await service._process(consumer, raw, service.decoder, 10.0)

    service.broker.nack.assert_called_once_with(raw, 30)
    service.broker.ack.assert_not_called()
    assert len(retried) == 1
    retry_exc = retried[0]
    assert isinstance(retry_exc, Retry)
    assert retry_exc.delay == 30


@pytest.mark.anyio
async def test_several_control_flow_exceptions_are_a_plain_failure(service: Service):
    """A group stating both `Retry` and `Skip` states no outcome: default action wins."""
    service.broker.ack = AsyncMock()  # type: ignore[method-assign]
    service.broker.nack = AsyncMock()  # type: ignore[method-assign]
    service.default_action = AsyncMock()

    async def raise_retry() -> None:
        raise Retry(delay=30)

    async def raise_skip() -> None:
        msg = "skip"
        raise Skip(msg)

    async def handler(message: CloudEvent) -> None:
        async with anyio.create_task_group() as tg:
            tg.start_soon(raise_retry)
            tg.start_soon(raise_skip)

    consumer = FnConsumer(
        fn=handler, event_type=CloudEvent, topic="test_topic", name="ambiguous_consumer"
    )
    ce: CloudEvent = CloudEvent.new({}, type="T", topic="test_topic", source="s")
    raw = StubMessage(
        data=service.encoder.encode(ce),
        queue=asyncio.Queue(),
        event=asyncio.Event(),
        headers={},
    )

    await service._process(consumer, raw, service.decoder, 10.0)

    service.default_action.assert_called_once_with(consumer, raw)


@pytest.mark.anyio
async def test_dependency_injection_with_postponed_annotations(broker):
    """Handlers in `from __future__ import annotations` modules still get deps."""
    from .postponed_handlers import (
        NOT_INJECTED,
        Dependency,
        PostponedEvent,
        handler_with_dependency,
    )

    dependency = Dependency()
    svc = Service(name="di_test", broker=broker, state={Dependency: dependency})
    token = set_current_service(svc)
    try:
        wrapped = resolved_func(handler_with_dependency)
        message = PostponedEvent.new(1)
        result = await wrapped(message)
    finally:
        reset_current_service(token)

    assert result is dependency
    assert result is not NOT_INJECTED


# --- background tasks ---


@pytest.mark.anyio
async def test_start_background_task_rejected_when_not_running(broker):
    svc = Service(name="bg", broker=broker)

    async def probe() -> None: ...

    with pytest.raises(RuntimeError, match="only be started while the service"):
        svc.start_background_task(probe)


@pytest.mark.anyio
async def test_background_task_runs_and_is_cancelled_on_shutdown(broker):
    svc = Service(name="bg", broker=broker)
    events = []

    async def probe() -> None:
        try:
            while True:
                events.append("tick")
                await anyio.sleep(0.01)
        finally:
            events.append("cancelled")

    class Probing(Middleware):
        async def after_broker_connect(self) -> None:
            self.service.start_background_task(probe)

    svc.add_middleware(Probing)

    async with svc.context():
        await anyio.sleep(0.05)
        assert "tick" in events

    assert events[-1] == "cancelled"


@pytest.mark.anyio
async def test_background_task_stops_before_broker_disconnect(broker):
    """`after_broker_disconnect` must observe background tasks already reaped."""
    svc = Service(name="bg", broker=broker)
    order = []

    async def probe() -> None:
        try:
            await anyio.sleep_forever()
        finally:
            order.append("task_stopped")

    class Probing(Middleware):
        async def after_broker_connect(self) -> None:
            self.service.start_background_task(probe)

        async def after_broker_disconnect(self) -> None:
            order.append("broker_disconnected")

    svc.add_middleware(Probing)

    async with svc.context():
        await anyio.sleep(0.01)

    assert order == ["task_stopped", "broker_disconnected"]


@pytest.mark.anyio
async def test_background_task_failure_propagates(broker):
    svc = Service(name="bg", broker=broker)

    async def boom() -> None:
        msg = "probe exploded"
        raise RuntimeError(msg)

    class Probing(Middleware):
        async def after_broker_connect(self) -> None:
            self.service.start_background_task(boom)

    svc.add_middleware(Probing)

    with pytest.raises(BaseExceptionGroup) as exc_info:
        async with svc.context():
            await anyio.sleep(0.05)

    assert [str(e) for e in exc_info.value.exceptions] == ["probe exploded"]


@pytest.mark.anyio
async def test_run_disconnects_when_connect_fails(broker):
    """`run` must release the broker if an after_broker_connect hook raises."""
    svc = Service(name="bg", broker=broker)
    disconnected = []

    class Exploding(Middleware):
        async def after_broker_connect(self) -> None:
            msg = "hook exploded"
            raise RuntimeError(msg)

        async def after_broker_disconnect(self) -> None:
            disconnected.append(True)

    svc.add_middleware(Exploding)

    with pytest.raises(BaseException):  # noqa: B017, PT011
        await svc.run(enable_signal_handler=False)

    assert disconnected == [True]
    assert not broker.is_connected
