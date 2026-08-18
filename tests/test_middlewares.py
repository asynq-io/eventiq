import gzip
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

import anyio
import pytest
import structlog
from structlog.contextvars import (
    bind_contextvars,
    clear_contextvars,
    get_contextvars,
    merge_contextvars,
)
from structlog.testing import capture_logs

from eventiq.exceptions import Fail, Retry, Skip
from eventiq.middlewares.dlx import DeadLetterQueueMiddleware
from eventiq.middlewares.error import ErrorHandlerMiddleware
from eventiq.middlewares.gzip import GzipMiddleware
from eventiq.middlewares.healthcheck import HealthCheckMiddleware
from eventiq.middlewares.rate_limits import RateLimitMiddleware
from eventiq.middlewares.retries import (
    BaseRetryStrategy,
    MaxAge,
    MaxRetries,
    RetryMiddleware,
    RetryWhen,
    constant,
    expo,
)
from eventiq.middlewares.structlog import StructlogMiddleware


@pytest.fixture
def message(ce):
    # MaxRetries accesses message.raw to pass to broker.get_num_delivered
    ce.set_raw("stub_raw", {})
    return ce


@pytest.fixture
def mock_service():
    svc = MagicMock()
    svc.broker.get_num_delivered = MagicMock(return_value=1)
    return svc


@pytest.fixture
def mock_consumer():
    return MagicMock()


# --- delay generators ---


def test_expo_generator(ce):
    gen = expo(factor=2)
    delay = gen(ce, Exception("err"))
    assert isinstance(delay, int)
    assert delay >= 0


def test_constant_generator(ce):
    gen = constant(interval=15)
    assert gen(ce, Exception()) == 15
    assert gen(ce, RuntimeError()) == 15


# --- BaseRetryStrategy ---


def test_base_retry_strategy_accepts_service_by_keyword(message, mock_service):
    """`service` is public API: callers pass it by keyword."""
    strategy = BaseRetryStrategy()
    with pytest.raises(Retry):
        strategy.maybe_retry(service=mock_service, message=message, exc=ValueError())


def test_base_retry_strategy_throws_by_keyword(message, mock_service):
    strategy = BaseRetryStrategy(throws=(ValueError,))
    with pytest.raises(Fail):
        strategy.maybe_retry(service=mock_service, message=message, exc=ValueError())


# --- MaxAge ---


def test_max_age_retries_within_age(message, mock_service):
    strategy = MaxAge(max_age=timedelta(hours=6))
    with pytest.raises(Retry):
        strategy.maybe_retry(mock_service, message, ValueError("err"))


def test_max_age_fails_when_exceeded(message, mock_service):
    strategy = MaxAge(max_age=timedelta(seconds=0))
    with pytest.raises(Fail):
        strategy.maybe_retry(mock_service, message, ValueError("err"))


def test_max_age_throws_exception_type(message, mock_service):
    strategy = MaxAge(max_age=timedelta(hours=6), throws=(ValueError,))
    with pytest.raises(Fail):
        strategy.maybe_retry(mock_service, message, ValueError("err"))


def test_max_age_does_not_throw_other_types(message, mock_service):
    strategy = MaxAge(max_age=timedelta(hours=6), throws=(ValueError,))
    with pytest.raises(Retry):
        strategy.maybe_retry(mock_service, message, RuntimeError("err"))


# --- MaxRetries ---


def test_max_retries_within_limit(message, mock_service):
    mock_service.broker.get_num_delivered.return_value = 2
    strategy = MaxRetries(max_retries=3)
    with pytest.raises(Retry):
        strategy.maybe_retry(mock_service, message, ValueError("err"))


def test_max_retries_exceeded(message, mock_service):
    mock_service.broker.get_num_delivered.return_value = 5
    strategy = MaxRetries(max_retries=3)
    with pytest.raises(Fail):
        strategy.maybe_retry(mock_service, message, ValueError("err"))


def test_max_retries_unknown_count(message, mock_service):
    mock_service.broker.get_num_delivered.return_value = None
    strategy = MaxRetries(max_retries=100)
    with pytest.raises(Retry):
        strategy.maybe_retry(mock_service, message, ValueError("err"))


# --- RetryWhen ---


def test_retry_when_true(message, mock_service):
    strategy = RetryWhen(retry_when=lambda _msg, _exc: True)
    with pytest.raises(Retry):
        strategy.maybe_retry(mock_service, message, ValueError("err"))


def test_retry_when_false(message, mock_service):
    strategy = RetryWhen(retry_when=lambda _msg, _exc: False)
    with pytest.raises(Fail):
        strategy.maybe_retry(mock_service, message, ValueError("err"))


# --- RetryMiddleware ---


@pytest.mark.anyio
async def test_retry_middleware_no_exc(service, ce, mock_consumer):
    middleware = RetryMiddleware(service)
    await middleware.after_process_message(consumer=mock_consumer, message=ce, exc=None)


@pytest.mark.anyio
async def test_retry_middleware_skip_exc(service, ce, mock_consumer):
    middleware = RetryMiddleware(service)
    await middleware.after_process_message(
        consumer=mock_consumer, message=ce, exc=Skip("skip")
    )


@pytest.mark.anyio
async def test_retry_middleware_fail_exc(service, ce, mock_consumer):
    middleware = RetryMiddleware(service)
    await middleware.after_process_message(
        consumer=mock_consumer, message=ce, exc=Fail("fail")
    )


@pytest.mark.anyio
async def test_retry_middleware_retry_exc(service, ce, mock_consumer):
    middleware = RetryMiddleware(service)
    await middleware.after_process_message(
        consumer=mock_consumer, message=ce, exc=Retry()
    )


@pytest.mark.anyio
async def test_retry_middleware_other_exc_triggers_retry(service, ce, mock_consumer):
    mock_consumer.retry_strategy = None
    middleware = RetryMiddleware(
        service, retry_strategy=MaxAge(max_age=timedelta(hours=6))
    )
    with pytest.raises(Retry):
        await middleware.after_process_message(
            consumer=mock_consumer, message=ce, exc=ValueError("some error")
        )


@pytest.mark.anyio
async def test_retry_middleware_invokes_plain_base_strategy(service, ce, mock_consumer):
    """A bare `BaseRetryStrategy` must retry, not be bypassed by a `TypeError`."""
    mock_consumer.retry_strategy = None
    middleware = RetryMiddleware(service, retry_strategy=BaseRetryStrategy())
    with pytest.raises(Retry):
        await middleware.after_process_message(
            consumer=mock_consumer, message=ce, exc=ValueError("some error")
        )


@pytest.mark.anyio
async def test_retry_middleware_uses_consumer_strategy(service, ce, mock_consumer):
    ce.set_raw("stub_raw", {})
    consumer_strategy = MaxRetries(max_retries=0)
    mock_consumer.retry_strategy = consumer_strategy
    service.broker.get_num_delivered = MagicMock(return_value=99)
    middleware = RetryMiddleware(service)
    with pytest.raises(Fail):
        await middleware.after_process_message(
            consumer=mock_consumer, message=ce, exc=ValueError("some error")
        )


# --- DeadLetterQueueMiddleware ---


@pytest.mark.anyio
async def test_dlx_publishes_on_fail(service, ce):
    middleware = DeadLetterQueueMiddleware(service, topic="dead_letters")
    exc = Fail("unrecoverable")

    service.publish = AsyncMock()
    await middleware.after_fail_message(message=ce, exc=exc)

    service.publish.assert_called_once()
    assert service.publish.call_args[1]["headers"]["exc-reason"] == "unrecoverable"
    assert service.publish.call_args[1]["topic"] == "dead_letters"


@pytest.mark.anyio
async def test_dlx_does_not_mutate_original_headers(service, ce):
    """The dead letter header must not leak onto the message being finalized."""
    middleware = DeadLetterQueueMiddleware(service, topic="dead_letters")

    service.publish = AsyncMock()
    await middleware.after_fail_message(message=ce, exc=Fail("unrecoverable"))

    published_msg = service.publish.call_args[0][0]
    assert "exc-reason" not in ce.headers
    assert published_msg.headers is not ce.headers


@pytest.mark.anyio
async def test_dlx_copies_message(service, ce):
    middleware = DeadLetterQueueMiddleware(service)
    exc = Fail("reason")

    service.publish = AsyncMock()
    await middleware.after_fail_message(message=ce, exc=exc)

    published_msg = service.publish.call_args[0][0]
    assert published_msg.id == ce.id
    assert published_msg is not ce  # must be a copy


@pytest.mark.anyio
async def test_dlx_publishes_when_raw_is_not_copyable(service, ce):
    """Real broker messages hold unpicklable state; copying must not depend on it."""
    import socket

    middleware = DeadLetterQueueMiddleware(service, topic="dead_letters")
    service.publish = AsyncMock()

    with socket.socket() as raw:
        ce.set_raw(raw, {"trace-id": "abc"})
        await middleware.after_fail_message(message=ce, exc=Fail("unrecoverable"))

    service.publish.assert_called_once()
    published_msg = service.publish.call_args[0][0]
    assert published_msg.id == ce.id
    assert published_msg.headers["trace-id"] == "abc"
    assert "exc-reason" not in ce.headers


# --- ErrorHandlerMiddleware ---


@pytest.mark.anyio
async def test_error_handler_called_on_matching_exc(service, ce):
    callback = AsyncMock()
    middleware = ErrorHandlerMiddleware(service, callback=callback, errors=ValueError)
    consumer = MagicMock()
    exc = ValueError("oops")

    await middleware.after_process_message(consumer=consumer, message=ce, exc=exc)
    callback.assert_called_once_with(service, consumer, ce, exc)


@pytest.mark.anyio
async def test_error_handler_not_called_without_exc(service, ce):
    callback = AsyncMock()
    middleware = ErrorHandlerMiddleware(service, callback=callback)
    consumer = MagicMock()

    await middleware.after_process_message(consumer=consumer, message=ce, exc=None)
    callback.assert_not_called()


@pytest.mark.anyio
async def test_error_handler_not_called_wrong_exc_type(service, ce):
    callback = AsyncMock()
    middleware = ErrorHandlerMiddleware(service, callback=callback, errors=ValueError)
    consumer = MagicMock()

    await middleware.after_process_message(
        consumer=consumer, message=ce, exc=TypeError("wrong type")
    )
    callback.assert_not_called()


@pytest.mark.anyio
async def test_error_handler_wraps_sync_callback(service, ce):
    results = []

    def sync_callback(_svc, _consumer, _msg, exc) -> None:
        results.append(exc)

    middleware = ErrorHandlerMiddleware(service, callback=sync_callback)
    consumer = MagicMock()
    exc = RuntimeError("sync test")

    await middleware.after_process_message(consumer=consumer, message=ce, exc=exc)
    assert results == [exc]


# --- RateLimitMiddleware ---


@pytest.mark.anyio
async def test_rate_limit_acquires_from_consumer_options(service, ce):
    limiter = AsyncMock()
    consumer = MagicMock()
    consumer.options = {"limiter": limiter}
    consumer.process = AsyncMock()

    middleware = RateLimitMiddleware(service)
    await middleware.before_process_message(consumer=consumer)
    await consumer.process(ce)

    limiter.acquire.assert_awaited_once()


@pytest.mark.anyio
async def test_rate_limit_acquires_from_middleware(service, ce):
    limiter = AsyncMock()
    consumer = MagicMock()
    consumer.options = {}
    handler = AsyncMock()
    consumer.process = handler

    middleware = RateLimitMiddleware(service, limiter=limiter)
    await middleware.before_process_message(consumer=consumer)
    await consumer.process(ce)

    limiter.acquire.assert_awaited_once()
    handler.assert_awaited_once_with(ce)


@pytest.mark.anyio
async def test_rate_limit_wraps_consumer_only_once(service, ce):
    """Repeated deliveries must not stack a new wrapper on every message."""
    limiter = AsyncMock()
    consumer = MagicMock()
    consumer.options = {}
    consumer.process = AsyncMock()

    middleware = RateLimitMiddleware(service, limiter=limiter)
    await middleware.before_process_message(consumer=consumer)
    wrapped = consumer.process
    await middleware.before_process_message(consumer=consumer)

    assert consumer.process is wrapped
    await consumer.process(ce)
    limiter.acquire.assert_awaited_once()


@pytest.mark.anyio
async def test_rate_limit_skipped_when_no_limiter(service):
    consumer = MagicMock()
    consumer.options = {}
    handler = AsyncMock()
    consumer.process = handler

    middleware = RateLimitMiddleware(service, limiter=None)
    await middleware.before_process_message(consumer=consumer)

    assert consumer.process is handler


@pytest.mark.anyio
async def test_rate_limit_throttles_longer_than_middleware_timeout(service):
    """A limiter slower than `middleware_timeout` must delay a message, not nack it."""
    import asyncio

    import anyio

    from eventiq import CloudEvent
    from eventiq.backends.stub import StubMessage

    class SlowLimiter:
        async def acquire(self) -> None:
            await anyio.sleep(0.2)

    service.middleware_timeout = 0.1
    message = CloudEvent.new({}, type="TestEvent", topic="test_topic", source="s")
    raw = StubMessage(
        data=service.encoder.encode(message),
        queue=asyncio.Queue(),
        event=asyncio.Event(),
        headers={},
    )
    processed = []

    async def process(msg) -> None:
        processed.append(msg.id)

    consumer = MagicMock()
    consumer.event_type = CloudEvent
    consumer.decoder = None
    consumer.timeout = None
    consumer.options = {"limiter": SlowLimiter()}
    consumer.process = process
    service.broker.ack = AsyncMock()
    service.broker.nack = AsyncMock()

    await service._process(consumer, raw, service.decoder, 2.0)

    assert processed == [message.id]
    service.broker.ack.assert_awaited_once()
    service.broker.nack.assert_not_awaited()


# --- GzipMiddleware ---


@pytest.mark.anyio
async def test_gzip_compresses_and_marks_payload(service):
    middleware = GzipMiddleware(service)
    payload, headers = await middleware.encode_payload(b'{"data": 1}', {"a": "b"})

    assert gzip.decompress(payload) == b'{"data": 1}'
    assert headers == {"a": "b", "Content-Encoding": "gzip"}


@pytest.mark.anyio
@pytest.mark.parametrize("headers", [{}, {"Content-Encoding": "gzip"}])
async def test_gzip_round_trip_without_headers(service, headers):
    """Brokers such as redis drop headers, so detection must not rely on them."""
    middleware = GzipMiddleware(service)
    compressed, _ = await middleware.encode_payload(b'{"data": 1}', {})

    assert await middleware.decode_payload(compressed, headers) == b'{"data": 1}'


@pytest.mark.anyio
@pytest.mark.parametrize("payload", [b"", b"{", b'{"data": 1}'])
async def test_gzip_leaves_plain_payload_untouched(service, payload):
    middleware = GzipMiddleware(service)
    assert await middleware.decode_payload(payload, {}) == payload


# --- HealthCheckMiddleware ---


@pytest.mark.anyio
async def test_healthcheck_schedules_probe_on_the_service(service):
    middleware = HealthCheckMiddleware(service, interval=1)
    started = []
    service.start_background_task = lambda func, **_: started.append(func)

    await middleware.after_broker_connect()

    assert started == [middleware._run_forever]


@pytest.mark.anyio
async def test_healthcheck_disconnect_without_connect_is_noop(service):
    middleware = HealthCheckMiddleware(service, interval=1)
    await middleware.after_broker_disconnect()  # should not raise


@pytest.mark.anyio
async def test_healthcheck_writes_healthy_marker_and_clears_unhealthy(
    service, tmp_path
):
    middleware = HealthCheckMiddleware(service, interval=1, base_dir=str(tmp_path))
    (tmp_path / "unhealthy").touch()
    service.broker._connected = True

    await middleware._write_status()

    assert (tmp_path / "healthy").exists()
    assert not (tmp_path / "unhealthy").exists()


@pytest.mark.anyio
async def test_healthcheck_unhealthy_broker_never_reports_healthy(service, tmp_path):
    """A disconnected broker must not create the healthy marker."""
    middleware = HealthCheckMiddleware(service, interval=1, base_dir=str(tmp_path))
    service.broker._connected = False

    await middleware._write_status()

    assert (tmp_path / "unhealthy").exists()
    assert not (tmp_path / "healthy").exists()


@pytest.mark.anyio
async def test_healthcheck_probe_reporting_dead_writes_unhealthy(service, tmp_path):
    """A constructed client is not a live one: the awaited probe decides."""
    middleware = HealthCheckMiddleware(service, interval=1, base_dir=str(tmp_path))
    service.broker._connected = True
    service.broker.check_health = AsyncMock(return_value=False)

    await middleware._write_status()

    assert (tmp_path / "unhealthy").exists()
    assert not (tmp_path / "healthy").exists()


@pytest.mark.anyio
async def test_healthcheck_raising_probe_is_unhealthy(service, tmp_path, caplog):
    """A probe that cannot reach the server is unhealthy, not a crash."""
    import logging

    middleware = HealthCheckMiddleware(service, interval=1, base_dir=str(tmp_path))
    service.broker._connected = True
    service.broker.check_health = AsyncMock(side_effect=ConnectionError("server gone"))

    with caplog.at_level(logging.ERROR):
        await middleware._write_status()

    assert (tmp_path / "unhealthy").exists()
    assert not (tmp_path / "healthy").exists()
    assert "Healthcheck failed" in caplog.text


@pytest.mark.anyio
async def test_healthcheck_never_shows_both_markers(service, tmp_path, monkeypatch):
    """The documented contract: probes must never observe both markers at once."""
    import anyio

    middleware = HealthCheckMiddleware(service, interval=1, base_dir=str(tmp_path))
    (tmp_path / "unhealthy").touch()
    service.broker._connected = True
    observed = []
    original_touch = anyio.Path.touch

    async def touch(self, mode: int = 0o666, *, exist_ok: bool = True) -> None:
        observed.append(sorted(p.name for p in tmp_path.iterdir()))
        await original_touch(self, mode, exist_ok=exist_ok)

    monkeypatch.setattr(anyio.Path, "touch", touch)

    await middleware._write_status()

    assert observed == [[]]  # the stale marker was gone before the new one appeared
    assert (tmp_path / "healthy").exists()
    assert not (tmp_path / "unhealthy").exists()


@pytest.mark.anyio
async def test_healthcheck_exception_in_is_connected(service, tmp_path):
    """A probe that raises is caught and reported as unhealthy."""
    middleware = HealthCheckMiddleware(service, interval=1, base_dir=str(tmp_path))

    orig_prop = type(service.broker).__dict__.get("is_connected")
    type(service.broker).is_connected = property(
        lambda _self: (_ for _ in ()).throw(RuntimeError("broker exploded"))
    )
    try:
        await middleware._write_status()
    finally:
        if orig_prop is not None:
            type(service.broker).is_connected = orig_prop
        else:
            del type(service.broker).is_connected

    assert (tmp_path / "unhealthy").exists()
    assert not (tmp_path / "healthy").exists()


@pytest.mark.anyio
async def test_healthcheck_rename_to_unhealthy(service, tmp_path):
    """A stale healthy marker is replaced once the broker reports dead."""
    middleware = HealthCheckMiddleware(service, interval=1, base_dir=str(tmp_path))
    healthy_path = tmp_path / "healthy"
    healthy_path.touch()

    orig_prop = type(service.broker).__dict__.get("is_connected")
    type(service.broker).is_connected = property(lambda _self: False)
    try:
        await middleware._write_status()
    finally:
        if orig_prop is not None:
            type(service.broker).is_connected = orig_prop
        else:
            del type(service.broker).is_connected

    assert not healthy_path.exists()
    assert (tmp_path / "unhealthy").exists()


@pytest.mark.anyio
async def test_healthcheck_survives_unwritable_base_dir(service, tmp_path, caplog):
    """A failing marker write is logged, and never kills the probing loop."""
    import logging

    middleware = HealthCheckMiddleware(
        service, interval=0, base_dir=str(tmp_path / "does" / "not" / "exist")
    )

    with caplog.at_level(logging.ERROR), anyio.move_on_after(0.1):
        await middleware._run_forever()

    assert "Failed to write healthcheck status" in caplog.text


@pytest.mark.anyio
async def test_healthcheck_disconnect_clears_healthy_marker(service, tmp_path):
    """A stopped service must not keep reporting the last healthy probe."""
    middleware = HealthCheckMiddleware(service, interval=1, base_dir=str(tmp_path))
    service.broker._connected = True
    await middleware._write_status()
    assert (tmp_path / "healthy").exists()

    await middleware.after_broker_disconnect()

    assert (tmp_path / "unhealthy").exists()
    assert not (tmp_path / "healthy").exists()


@pytest.mark.anyio
async def test_healthcheck_connect_clears_stale_healthy_marker(service, tmp_path):
    """A marker left by a previous run must not survive into a failing start."""
    (tmp_path / "healthy").touch()
    middleware = HealthCheckMiddleware(service, interval=1, base_dir=str(tmp_path))

    await middleware.before_broker_connect()

    assert (tmp_path / "unhealthy").exists()
    assert not (tmp_path / "healthy").exists()


@pytest.mark.anyio
async def test_healthcheck_unwritable_dir_does_not_break_shutdown(service, tmp_path):
    missing = tmp_path / "gone"
    middleware = HealthCheckMiddleware(service, interval=1, base_dir=str(missing))

    await middleware.before_broker_connect()
    await middleware.after_broker_disconnect()


# --- MaxAge with dict config (retries.py line 95) ---


def test_max_age_with_dict_config():
    strategy = MaxAge(max_age={"hours": 6})
    assert strategy.max_age == timedelta(hours=6)


# --- retry() with exception that has a delay attr (retries.py line 54->56) ---


def test_retry_exc_with_delay_attr(message, mock_service):
    class DelayedExc(Exception):
        delay = 30

    strategy = MaxAge(max_age=timedelta(hours=6))
    with pytest.raises(Retry) as exc_info:
        strategy.maybe_retry(mock_service, message, DelayedExc())
    assert exc_info.value.delay == 30


# --- retry() with log_exceptions=False (retries.py line 57->64) ---


def test_retry_no_log(message, mock_service):
    strategy = MaxAge(max_age=timedelta(hours=6), log_exceptions=False)
    with pytest.raises(Retry):
        strategy.maybe_retry(mock_service, message, ValueError("err"))


# --- StructlogMiddleware ---


@pytest.fixture
def log_context():
    clear_contextvars()
    yield
    clear_contextvars()


@pytest.mark.anyio
async def test_structlog_middleware_binds_message_id(service, ce, log_context):
    middleware = StructlogMiddleware(service)
    await middleware.before_process_message(consumer=MagicMock(), message=ce)
    assert get_contextvars()["message_id"] == str(ce.id)


@pytest.mark.anyio
async def test_structlog_middleware_custom_key(service, ce, log_context):
    middleware = StructlogMiddleware(service, message_id_key="msg_id")
    await middleware.before_process_message(consumer=MagicMock(), message=ce)
    assert get_contextvars() == {"msg_id": str(ce.id)}


@pytest.mark.anyio
async def test_structlog_middleware_preserves_existing_context(
    service, ce, log_context
):
    """Bindings made by the application must survive message processing."""
    middleware = StructlogMiddleware(service)
    consumer = MagicMock()
    bind_contextvars(request_id="req-1", tenant="acme")

    await middleware.before_process_message(consumer=consumer, message=ce)
    assert get_contextvars() == {
        "request_id": "req-1",
        "tenant": "acme",
        "message_id": str(ce.id),
    }

    await middleware.after_process_message(consumer=consumer, message=ce)
    assert get_contextvars() == {"request_id": "req-1", "tenant": "acme"}


@pytest.mark.anyio
async def test_structlog_middleware_unbinds_message_id(service, ce, log_context):
    middleware = StructlogMiddleware(service)
    consumer = MagicMock()

    await middleware.before_process_message(consumer=consumer, message=ce)
    await middleware.after_process_message(
        consumer=consumer, message=ce, result=None, exc=None
    )
    assert "message_id" not in get_contextvars()


@pytest.mark.anyio
async def test_structlog_middleware_unbind_without_bind(service, log_context):
    """A message skipped before binding must not break finalization."""
    middleware = StructlogMiddleware(service)
    await middleware.after_process_message(consumer=MagicMock(), message=None)
    assert "message_id" not in get_contextvars()


@pytest.mark.anyio
async def test_structlog_middleware_message_id_in_log_entries(service, ce, log_context):
    middleware = StructlogMiddleware(service)
    await middleware.before_process_message(consumer=MagicMock(), message=ce)

    with capture_logs(processors=[merge_contextvars]) as entries:
        structlog.get_logger(__name__).info("processing")

    assert entries[0]["message_id"] == str(ce.id)


@pytest.mark.anyio
async def test_structlog_middleware_bound_while_consumer_runs(service, log_context):
    """The id is bound for the whole handler run and cleared afterwards."""
    import asyncio

    from eventiq import CloudEvent
    from eventiq.backends.stub import StubMessage

    service.add_middleware(StructlogMiddleware)
    message = CloudEvent.new({}, type="TestEvent", topic="test_topic", source="s")
    raw = StubMessage(
        data=service.encoder.encode(message),
        queue=asyncio.Queue(),
        event=asyncio.Event(),
        headers={},
    )
    bound = {}

    async def process(_message) -> None:
        bound.update(get_contextvars())

    consumer = MagicMock()
    consumer.event_type = CloudEvent
    consumer.decoder = None
    consumer.timeout = None
    consumer.options = {}
    consumer.process = process
    service.broker.ack = AsyncMock()

    await service._process(consumer, raw, service.decoder, 10.0)

    assert bound["message_id"] == str(message.id)
    assert "message_id" not in get_contextvars()
