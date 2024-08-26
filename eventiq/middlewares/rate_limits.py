from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from eventiq.middleware import CloudEventType, Middleware

if TYPE_CHECKING:
    from eventiq import Consumer, Service


class Limiter(Protocol):
    """
    This is protocol for user to be implemented.
    For single instance rate limits `aiolimiter` (https://github.com/mjpieters/aiolimiter)
    package provides compatible interface.
    For distributed rate limits redis can be used.
    """

    async def acquire(self) -> None: ...


class RateLimitMiddleware(Middleware[CloudEventType]):
    def __init__(self, service: Service, limiter: Limiter | None = None) -> None:
        super().__init__(service)
        self.limiter = limiter

    async def before_process_message(
        self, *, consumer: Consumer, message: CloudEventType
    ) -> None:
        limiter: Limiter = consumer.options.get("limiter", self.limiter)

        if limiter is not None:
            await limiter.acquire()
