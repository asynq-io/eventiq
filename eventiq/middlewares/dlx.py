from __future__ import annotations

from typing import TYPE_CHECKING, Any

from eventiq.middleware import CloudEventType, Middleware

if TYPE_CHECKING:
    from eventiq import Service
    from eventiq.exceptions import Fail


class DeadLetterQueueMiddleware(Middleware[CloudEventType]):
    """Republishes permanently failed messages to a dead letter topic."""

    def __init__(
        self,
        service: Service,
        topic: str = "dlx",
        **kwargs: Any,
    ) -> None:
        super().__init__(service)
        self.topic = topic
        self.kwargs = kwargs

    async def after_fail_message(
        self,
        *,
        message: CloudEventType,
        exc: Fail,
        **_: Any,
    ) -> None:
        # The copy must stay shallow: a deep copy would also copy `_raw`, the live
        # broker message, which is frequently not copyable (a NATS `Msg` reaches an
        # open socket through its client) and the resulting error would turn
        # dead-lettering into an endless redelivery loop. Detaching the raw message
        # and the headers dict instead keeps the dead letter header from leaking onto
        # the message being finalized.
        dlx_message = message.model_copy()
        dlx_message.set_raw(None, dict(message.headers))
        await self.service.publish(
            dlx_message,
            topic=self.topic,
            headers={"exc-reason": exc.reason},
            **self.kwargs,
        )
