from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import TYPE_CHECKING, Any, Callable

from eventiq.middleware import Middleware
from eventiq.utils import to_async

if TYPE_CHECKING:
    from eventiq import CloudEvent, Consumer, Service


class ErrorHandlerMiddleware(Middleware):
    def __init__(
        self,
        errors: type[Exception] | tuple[type[Exception]],
        callback: Callable[
            [Service, Consumer, CloudEvent, Exception | None], Awaitable[Any]
        ],
    ):
        if not asyncio.iscoroutinefunction(callback):
            callback = to_async(callback)
        self.callback = callback
        self.exc = errors

    async def after_process_message(
        self,
        *,
        service: Service,
        consumer: Consumer,
        message: CloudEvent,
        result: Any | None = None,
        exc: Exception | None = None,
    ):
        if exc and isinstance(exc, self.exc):
            await self.callback(service, consumer, message, exc)
