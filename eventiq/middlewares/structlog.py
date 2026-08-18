from __future__ import annotations

from typing import TYPE_CHECKING, Any

from structlog.contextvars import (
    bind_contextvars,
    unbind_contextvars,
)

from eventiq.middleware import CloudEventType, Middleware

if TYPE_CHECKING:
    from eventiq.service import Service


class StructlogMiddleware(Middleware[CloudEventType]):
    """Binds the id of the currently processed message to the logging context.

    Every entry logged while the message is being processed, by the framework,
    the handler or any other middleware, carries a `message_id` key. Only that
    key is managed, so bindings made by the application (in a lifespan or in
    another middleware) survive message processing.
    """

    def __init__(self, service: Service, *, message_id_key: str = "message_id") -> None:
        super().__init__(service)
        self.message_id_key = message_id_key

    async def before_process_message(
        self,
        *,
        message: CloudEventType,
        **_: Any,
    ) -> None:
        bind_contextvars(**{self.message_id_key: str(message.id)})

    async def after_process_message(self, **_: Any) -> None:
        unbind_contextvars(self.message_id_key)
