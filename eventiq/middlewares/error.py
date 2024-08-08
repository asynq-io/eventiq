from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Callable

from eventiq.middleware import CloudEventType, Middleware
from eventiq.utils import to_async

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from eventiq import Consumer, Service


class ErrorHandlerMiddleware(Middleware[CloudEventType]):
    def __init__(
        self,
        service: Service,
        callback: Callable[
            [Service, Consumer, CloudEventType, Exception | None], Awaitable[Any]
        ],
        errors: type[Exception] | tuple[type[Exception]] = Exception,
    ) -> None:
        super().__init__(service)
        if not asyncio.iscoroutinefunction(callback):
            callback = to_async(callback)
        self.callback = callback
        self.exc = errors

    async def after_process_message(
        self,
        *,
        consumer: Consumer,
        message: CloudEventType,
        result: Any | None = None,
        exc: Exception | None = None,
    ) -> None:
        if exc and isinstance(exc, self.exc):
            await self.callback(self.service, consumer, message, exc)
