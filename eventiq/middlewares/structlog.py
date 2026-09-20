from __future__ import annotations

from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

from structlog.contextvars import (
    bind_contextvars,
    unbind_contextvars,
)

from eventiq.middleware import CloudEventType, Middleware

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from eventiq import Consumer
    from eventiq.service import Service

_bound_keys: ContextVar[tuple[str, ...]] = ContextVar(
    "eventiq_structlog_bound_keys",
    default=(),
)


class StructlogMiddleware(Middleware[CloudEventType]):
    """Binds the currently processed message to the logging context.

    Every entry logged while the message is being processed, by the framework,
    the handler or any other middleware, carries a `message_id` and a
    `consumer` key. Only the keys bound here are managed, so bindings made by
    the application (in a lifespan or in another middleware) survive message
    processing.
    """

    def __init__(
        self,
        service: Service,
        *,
        message_id_key: str = "message_id",
        consumer_name_key: str = "consumer",
        extra_context_provider: Callable[[CloudEventType], Mapping[str, Any]]
        | None = None,
    ) -> None:
        super().__init__(service)
        self.message_id_key = message_id_key
        self.consumer_name_key = consumer_name_key
        self.extra_context_provider = extra_context_provider

    async def before_process_message(
        self,
        *,
        consumer: Consumer,
        message: CloudEventType,
        **_: Any,
    ) -> None:
        context: dict[str, Any] = {
            self.message_id_key: str(message.id),
            self.consumer_name_key: consumer.name,
        }
        if self.extra_context_provider:
            context.update(self.extra_context_provider(message))
        bind_contextvars(**context)
        _bound_keys.set(tuple(context))

    async def after_message_finalized(self, **_: Any) -> None:
        unbind_contextvars(*_bound_keys.get())
        _bound_keys.set(())
