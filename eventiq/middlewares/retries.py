from __future__ import annotations

from collections.abc import Mapping
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Callable, Generic, NamedTuple

from typing_extensions import ParamSpec

from eventiq.exceptions import Fail, Retry, Skip
from eventiq.logging import LoggerMixin
from eventiq.middleware import Middleware

if TYPE_CHECKING:
    from eventiq import CloudEvent, Consumer, Service


P = ParamSpec("P")

DelayGenerator = Callable[["CloudEvent", Exception], int]


class MessageStatus(NamedTuple):
    status: str
    delay: int | None


def expo(factor: int = 1) -> DelayGenerator:
    def _expo(message: CloudEvent, _: Exception) -> int:
        return factor * message.age.seconds

    return _expo


def constant(interval: int = 30) -> DelayGenerator:
    def _constant(*_) -> int:
        return interval

    return _constant


class RetryStrategy(Generic[P], LoggerMixin):
    def __init__(
        self,
        throws: tuple[type[Exception], ...] = (),
        delay_generator: Callable[P, DelayGenerator] | None = None,
        min_delay: int = 2,
        *args: P.args,
        **kwargs: P.kwargs,
    ):
        if Fail not in throws:
            throws = (*throws, Fail)
        self.throws = throws
        self.min_delay = min_delay
        self.delay_generator = (
            delay_generator(*args, **kwargs) if delay_generator else expo()
        )

    def retry(self, message: CloudEvent, exc: Exception):
        delay = getattr(exc, "delay", None)
        if delay is None:
            delay = self.delay_generator(message, exc)
        delay = max(delay, self.min_delay)
        self.logger.info(f"Will retry message {message.id} in %d seconds.", delay)
        raise Retry(delay=delay) from exc

    def fail(self, message: CloudEvent, exc: Exception):
        self.logger.error(f"Retry limit exceeded for message {message.id}.")
        self.logger.exception("Original exception:", exc_info=exc)
        raise Fail(reason="Retry limit exceeded") from exc

    def maybe_retry(self, service: Service, message: CloudEvent, exc: Exception):
        if not (self.throws and isinstance(exc, self.throws)):
            self.retry(message, exc)
        else:
            self.fail(message, exc)


class MaxAge(RetryStrategy):
    def __init__(
        self, max_age: timedelta | dict[str, Any] = timedelta(seconds=60), **extra
    ):
        super().__init__(**extra)
        if isinstance(max_age, Mapping):
            max_age = timedelta(**max_age)
        self.max_age: timedelta = max_age

    def maybe_retry(self, service: Service, message: CloudEvent, exc: Exception):
        if message.age <= self.max_age:
            super().maybe_retry(service, message, exc)
        else:
            self.fail(message, exc)


class MaxRetries(RetryStrategy):
    def __init__(self, max_retries: int = 3, **extra):
        super().__init__(**extra)
        self.max_retries = max_retries

    def maybe_retry(self, service: Service, message: CloudEvent, exc: Exception):
        retries = service.broker.get_num_delivered(message.raw)
        if retries is None:
            self.logger.warning(
                "Retries property not found in message, backing off to message.age.seconds"
            )
            retries = int(message.age.seconds**0.5)
        if retries <= self.max_retries:
            super().maybe_retry(service, message, exc)
        else:
            self.fail(message, exc)


class RetryWhen(RetryStrategy):
    def __init__(self, retry_when: Callable[[CloudEvent, Exception], bool], **extra):
        super().__init__(**extra)
        self.retry_when = retry_when

    def maybe_retry(
        self,
        service: Service,
        message: CloudEvent,
        exc: Exception,
    ):
        if self.retry_when(message, exc):
            super().maybe_retry(service, message, exc)
        else:
            self.fail(message, exc)


class RetryMiddleware(Middleware):
    """
    Retry Message Middleware.
    Supported retry strategies:
    - `MaxAge` (default) - retry with exponential backoff up to max_age
    - `MaxRetries` - retry up to N times (currently supported only by nats)
    - `RetryWhen` - provide custom callable to determine weather message should be retried
    """

    def __init__(
        self,
        delay_header: str = "x-delay",
        default_retry_strategy: RetryStrategy = MaxAge(max_age=timedelta(hours=1)),
    ):
        self.delay_header = delay_header
        self.default_retry_strategy = default_retry_strategy

    async def after_process_message(
        self,
        *,
        service: Service,
        consumer: Consumer,
        message: CloudEvent,
        result: Any | None = None,
        exc: Exception | None = None,
    ):
        if exc is None or isinstance(exc, (Retry, Fail, Skip)):
            return

        retry_strategy = consumer.options.get(
            "retry_strategy", self.default_retry_strategy
        )
        retry_strategy.maybe_retry(service, message, exc)

    # async def before_publish(self, broker: B, message: CloudEvent, **kwargs) -> None:
    #     delay = kwargs.get("delay", message.delay)

    #     if delay is not None and self.delay_header:
    #         message.set_header(self.delay_header, str(delay))

    # async def before_process_message(
    #     self, broker: B, service: Service, consumer: Consumer, message: CloudEvent
    # ) -> None:
    #     """Broker agnostic implementation of not-before header."""
    #     if self.delay_header:
    #         delay_header = int(message.headers.get(self.delay_header, 0))
    #         if delay_header and message.age < timedelta(seconds=delay_header):
    #             raise Retry(f"Delay header set to {delay_header}", delay=delay_header)
