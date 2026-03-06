from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from eventiq.exceptions import Fail, Retry, Skip
from eventiq.middlewares.dlx import DeadLetterQueueMiddleware
from eventiq.middlewares.error import ErrorHandlerMiddleware
from eventiq.middlewares.healthcheck import HealthCheckMiddleware
from eventiq.middlewares.rate_limits import RateLimitMiddleware
from eventiq.middlewares.retries import (
    MaxAge,
    MaxRetries,
    RetryMiddleware,
    RetryWhen,
    constant,
    expo,
)


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
    published_msg = service.publish.call_args[0][0]
    assert published_msg.headers.get("exc-reason") == "unrecoverable"
    assert service.publish.call_args[1]["topic"] == "dead_letters"


@pytest.mark.anyio
async def test_dlx_copies_message(service, ce):
    middleware = DeadLetterQueueMiddleware(service)
    exc = Fail("reason")

    service.publish = AsyncMock()
    await middleware.after_fail_message(message=ce, exc=exc)

    published_msg = service.publish.call_args[0][0]
    assert published_msg.id == ce.id
    assert published_msg is not ce  # must be a copy


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
async def test_rate_limit_acquires_from_consumer_options(service):
    limiter = AsyncMock()
    limiter.acquire = AsyncMock()
    consumer = MagicMock()
    consumer.options = {"limiter": limiter}

    middleware = RateLimitMiddleware(service)
    await middleware.before_process_message(consumer=consumer)
    limiter.acquire.assert_called_once()


@pytest.mark.anyio
async def test_rate_limit_acquires_from_middleware(service):
    limiter = AsyncMock()
    limiter.acquire = AsyncMock()
    consumer = MagicMock()
    consumer.options = {}

    middleware = RateLimitMiddleware(service, limiter=limiter)
    await middleware.before_process_message(consumer=consumer)
    limiter.acquire.assert_called_once()


@pytest.mark.anyio
async def test_rate_limit_skipped_when_no_limiter(service):
    consumer = MagicMock()
    consumer.options = {}

    middleware = RateLimitMiddleware(service, limiter=None)
    # Should not raise
    await middleware.before_process_message(consumer=consumer)


# --- HealthCheckMiddleware ---


@pytest.mark.anyio
async def test_healthcheck_creates_and_cancels_task(service):
    middleware = HealthCheckMiddleware(service, interval=1)
    assert middleware._task is None
    await middleware.after_broker_connect()
    assert middleware._task is not None
    await middleware.after_broker_disconnect()
    assert middleware._task is None


@pytest.mark.anyio
async def test_healthcheck_disconnect_no_task(service):
    """after_broker_disconnect when _task is None → no error (line 48->exit)."""
    middleware = HealthCheckMiddleware(service, interval=1)
    assert middleware._task is None
    await middleware.after_broker_disconnect()  # should not raise


@pytest.mark.anyio
async def test_healthcheck_exception_in_is_connected(service, tmp_path):
    """is_connected raises → exception caught, unhealthy=True (lines 37-39)."""
    import asyncio

    middleware = HealthCheckMiddleware(service, interval=1, base_dir=str(tmp_path))

    orig_prop = type(service.broker).__dict__.get("is_connected")
    type(service.broker).is_connected = property(
        lambda _self: (_ for _ in ()).throw(RuntimeError("broker exploded"))
    )

    await middleware.after_broker_connect()
    await asyncio.sleep(0.05)
    await middleware.after_broker_disconnect()

    if orig_prop is not None:
        type(service.broker).is_connected = orig_prop
    else:
        del type(service.broker).is_connected


@pytest.mark.anyio
async def test_healthcheck_rename_to_unhealthy(service, tmp_path):
    """unhealthy=True and healthy file exists → renamed to unhealthy (line 42)."""
    import asyncio

    middleware = HealthCheckMiddleware(service, interval=1, base_dir=str(tmp_path))
    healthy_path = tmp_path / "healthy"
    healthy_path.touch()

    orig_prop = type(service.broker).__dict__.get("is_connected")
    type(service.broker).is_connected = property(lambda _self: False)

    await middleware.after_broker_connect()
    await asyncio.sleep(0.05)
    await middleware.after_broker_disconnect()

    if orig_prop is not None:
        type(service.broker).is_connected = orig_prop
    else:
        del type(service.broker).is_connected

    assert not healthy_path.exists()
    assert (tmp_path / "unhealthy").exists()


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
