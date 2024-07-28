from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import TYPE_CHECKING, Any, Callable

from eventiq.middleware import CloudEventType, Middleware
from eventiq.utils import to_async

if TYPE_CHECKING:
    from eventiq import Consumer, Service


class ErrorHandlerMiddleware(Middleware[CloudEventType]):
    def __init__(
        self,
        errors: type[Exception] | tuple[type[Exception]],
        callback: Callable[
            [Service, Consumer, CloudEventType, Exception | None], Awaitable[Any]
        ],
    ) -> None:
        if not asyncio.iscoroutinefunction(callback):
            callback = to_async(callback)
        self.callback = callback
        self.exc = errors

    async def after_process_message(
        self,
        *,
        service: Service,
        consumer: Consumer,
        message: CloudEventType,
        result: Any | None = None,
        exc: Exception | None = None,
    ) -> None:
        if exc and isinstance(exc, self.exc):
            await self.callback(service, consumer, message, exc)
