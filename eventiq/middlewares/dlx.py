from __future__ import annotations

from typing import TYPE_CHECKING, Any

from eventiq.middleware import CloudEventType, Middleware

if TYPE_CHECKING:
    from eventiq import Consumer
    from eventiq.exceptions import Fail


class DeadLetterQueueMiddleware(Middleware[CloudEventType]):
    def __init__(
        self,
        topic: str = "dlx",
        **kwargs: Any,
    ) -> None:
        self.topic = topic
        self.kwargs = kwargs

    async def after_fail_message(
        self,
        *,
        consumer: Consumer,
        message: CloudEventType,
        exc: Fail,
    ) -> None:
        dlx_message = message.copy()
        dlx_message.headers.update(
            {
                "exc-reason": exc.reason,
            }
        )
        await self.service.publish(dlx_message, topic=self.topic, **self.kwargs)
