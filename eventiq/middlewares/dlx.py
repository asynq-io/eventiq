from __future__ import annotations

from typing import TYPE_CHECKING

from eventiq.exceptions import Fail
from eventiq.middleware import Middleware
from eventiq.models import CloudEvent

if TYPE_CHECKING:
    from eventiq import CloudEvent, Consumer, Service


class DeadLetterQueueMiddleware(Middleware):
    def __init__(self, topic: str = "dlx", type_: str = "MessageFailedEvent", **kwargs):
        self.topic = topic
        self.type_ = type_
        self.kwargs = kwargs

    async def after_fail_message(
        self,
        *,
        service: Service,
        consumer: Consumer,
        message: CloudEvent,
        exc: Fail,
    ):
        topic = self.topic.format(message=message, consumer=consumer, service=service)
        ce = CloudEvent.new(message, type_=self.type_, topic=topic, **self.kwargs)
        await service.publish(ce)
