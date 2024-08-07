from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Callable, Generic, NamedTuple

from typing_extensions import ParamSpec

from eventiq.exceptions import Fail, Retry, Skip
from eventiq.logging import LoggerMixin
from eventiq.middleware import CloudEventType, Middleware

if TYPE_CHECKING:
    from eventiq import CloudEvent, Consumer, Service


P = ParamSpec("P")

DelayGenerator = Callable[[CloudEventType, Exception], int]


class MessageStatus(NamedTuple):
    status: str
    delay: int | None


def expo(factor: int = 1) -> DelayGenerator:
    def _expo(message: CloudEvent, _: Exception) -> int:
        return factor * message.age.seconds

    return _expo


def constant(interval: int = 30) -> DelayGenerator:
    def _constant(*_: Any) -> int:
        return interval

    return _constant


class AbstractRetryStrategy(ABC, Generic[CloudEventType]):
    @abstractmethod
    def maybe_retry(
        self,
        *,
        service: Service,
        message: CloudEventType,
        exc: Exception,
    ) -> None:
        raise NotImplementedError


class RetryStrategy(
    AbstractRetryStrategy[CloudEventType], Generic[P, CloudEventType], LoggerMixin
):
    def __init__(
        self,
        throws: tuple[type[Exception], ...] = (),
        delay_generator: Callable[P, DelayGenerator] | None = None,
        min_delay: int = 2,
        log_exceptions: bool = True,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        if Fail not in throws:
            throws = (*throws, Fail)
        self.throws = throws
        self.min_delay = min_delay
        self.log_exceptions = log_exceptions
        self.delay_generator = (
            delay_generator(*args, **kwargs) if delay_generator else expo()
        )

    def retry(self, message: CloudEventType, exc: Exception) -> None:
        delay = getattr(exc, "delay", None)
        if delay is None:
            delay = self.delay_generator(message, exc)
        delay = max(delay, self.min_delay)
        if self.log_exceptions:
            self.logger.warning(
                "Will retry message %s in %d seconds.",
                message.id,
                delay,
                exc_info=exc,
            )
        raise Retry(delay=delay) from exc

    def fail(self, message: CloudEventType, exc: Exception) -> None:
        self.logger.exception(
            "Retry limit exceeded for message %s",
            message.id,
            exc_info=exc,
        )
        raise Fail(reason="Retry limit exceeded") from exc

    def maybe_retry(
        self,
        *,
        service: Service,
        message: CloudEventType,
        exc: Exception,
    ) -> None:
        if not (self.throws and isinstance(exc, self.throws)):
            self.retry(message, exc)
        else:
            self.fail(message, exc)


class MaxAge(RetryStrategy[P, CloudEventType]):
    def __init__(
        self,
        max_age: timedelta | dict[str, Any] = timedelta(seconds=60),
        **extra: Any,
    ) -> None:
        super().__init__(**extra)
        if isinstance(max_age, Mapping):
            max_age = timedelta(**max_age)
        self.max_age: timedelta = max_age

    def maybe_retry(
        self,
        *,
        service: Service,
        message: CloudEventType,
        exc: Exception,
    ) -> None:
        if message.age <= self.max_age:
            super().maybe_retry(service=service, message=message, exc=exc)
        else:
            self.fail(message, exc)


class MaxRetries(RetryStrategy[P, CloudEventType]):
    def __init__(self, max_retries: int = 3, **extra: Any) -> None:
        super().__init__(**extra)
        self.max_retries = max_retries

    def maybe_retry(
        self,
        *,
        service: Service,
        message: CloudEventType,
        exc: Exception,
    ) -> None:
        retries = service.broker.get_num_delivered(message.raw)
        if retries is None:
            self.logger.warning(
                "Retries property not found in message, backing off to message.age.seconds",
            )
            retries = int(message.age.total_seconds() ** 0.5)
        if retries <= self.max_retries:
            super().maybe_retry(service=service, message=message, exc=exc)
        else:
            self.fail(message, exc)


class RetryWhen(RetryStrategy[P, CloudEventType]):
    def __init__(
        self,
        retry_when: Callable[[CloudEventType, Exception], bool],
        **extra: Any,
    ) -> None:
        super().__init__(**extra)
        self.retry_when = retry_when

    def maybe_retry(
        self,
        *,
        service: Service,
        message: CloudEventType,
        exc: Exception,
    ) -> None:
        if self.retry_when(message, exc):
            super().maybe_retry(service=service, message=message, exc=exc)
        else:
            self.fail(message, exc)


class RetryMiddleware(Middleware[CloudEventType]):
    """Retry Message Middleware.
    Supported retry strategies:
    - `MaxAge` (default) - retry with exponential backoff up to max_age
    - `MaxRetries` - retry up to N times (currently supported only by nats)
    - `RetryWhen` - provide custom callable to determine weather message should be retried.
    """

    def __init__(
        self,
        service: Service,
        default_retry_strategy: RetryStrategy = MaxAge(max_age=timedelta(hours=6)),
    ) -> None:
        super().__init__(service)
        self.default_retry_strategy = default_retry_strategy

    async def after_process_message(
        self,
        *,
        consumer: Consumer,
        message: CloudEventType,
        exc: Exception | None = None,
        **_: Any,
    ) -> None:
        if exc is None or isinstance(exc, (Retry, Fail, Skip)):
            return

        retry_strategy = consumer.options.get(
            "retry_strategy",
            self.default_retry_strategy,
        )
        retry_strategy.maybe_retry(service=self.service, message=message, exc=exc)
