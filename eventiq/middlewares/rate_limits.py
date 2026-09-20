from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

from eventiq.middleware import CloudEventType, Middleware

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from eventiq import Consumer, Service


class Limiter(Protocol):
    """
    This is protocol for user to be implemented.
    For single instance rate limits `aiolimiter` (https://github.com/mjpieters/aiolimiter)
    package provides compatible interface.
    For distributed rate limits redis can be used.
    """

    async def acquire(self) -> None: ...


class ThrottledProcess:
    """Consumer handler which waits for the limiter before running."""

    def __init__(
        self,
        limiter: Limiter,
        process: Callable[..., Awaitable[Any]],
    ) -> None:
        self.limiter = limiter
        self.process = process

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        await self.limiter.acquire()
        return await self.process(*args, **kwargs)


class RateLimitMiddleware(Middleware[CloudEventType]):
    """Throttles message processing through a per-consumer or service-wide limiter.

    The limiter is awaited inside the consumer handler rather than in the hook
    itself, because every hook runs under `Service.middleware_timeout` (10s by
    default) while throttling is expected to delay messages for much longer.
    The wait is therefore bounded by the consumer timeout, which is also what
    brokers derive their redelivery deadline from.
    """

    def __init__(self, service: Service, limiter: Limiter | None = None) -> None:
        super().__init__(service)
        self.limiter = limiter

    async def before_process_message(
        self,
        *,
        consumer: Consumer,
        **_: Any,
    ) -> None:
        limiter: Limiter | None = consumer.options.get("limiter", self.limiter)

        if limiter is None or isinstance(consumer.process, ThrottledProcess):
            return

        consumer.process = ThrottledProcess(limiter, consumer.process)
