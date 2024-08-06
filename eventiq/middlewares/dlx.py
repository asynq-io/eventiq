from __future__ import annotations

from typing import TYPE_CHECKING, Any

from eventiq.middleware import CloudEventType, Middleware
from eventiq.models import CloudEvent

if TYPE_CHECKING:
    from eventiq import Service


class DeadLetterQueueMiddleware(Middleware[CloudEventType]):
    def __init__(
        self,
        topic: str = "dlx",
        type_: str = "MessageFailedEvent",
        event_class: type[CloudEvent] = CloudEvent,
        **kwargs: Any,
    ) -> None:
        self.event_class = event_class
        self.topic = topic
        self.type_ = type_
        self.kwargs = kwargs

    async def after_fail_message(
        self,
        *,
        service: Service,
        message: CloudEventType,
        **_: Any,
    ) -> None:
        ce = self.event_class.new(
            message,
            type_=self.type_,
            topic=self.topic,
            **self.kwargs,
        )
        await service.publish(ce)
