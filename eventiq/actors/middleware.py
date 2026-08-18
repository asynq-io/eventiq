from __future__ import annotations

from typing import TYPE_CHECKING, Any

from eventiq.middleware import Middleware

from .actor import Actor
from .models import ActorMessage

if TYPE_CHECKING:
    from eventiq import Consumer, Service


class ActorsMiddleware(Middleware[ActorMessage]):
    """Wires actor request/reply: tags queries with a reply topic and sends responses."""

    requires: type[ActorMessage] | None = ActorMessage

    def __init__(
        self, service: Service, *, message_class: type[ActorMessage] = ActorMessage
    ) -> None:
        super().__init__(service)
        self._reply_topic = f"actors.replies.{service.name}.{service.id}"
        self.msg_cls = message_class

        @service.subscribe(
            topic=self._reply_topic,
            name=f"actors-callbacks-{service.id}",
            dynamic=True,
            concurrency=5,
        )
        async def _(message: ActorMessage[Any]) -> None:
            Actor.set_message_result(message.conversation_id, message.data)

    async def before_publish(self, *, message: ActorMessage, **_: Any) -> None:
        if message.kind == "query":
            message.reply_to = self._reply_topic

    async def after_process_message(
        self,
        *,
        consumer: Consumer,
        message: ActorMessage,
        result: Any = None,
        exc: Exception | None = None,
    ) -> None:
        if exc is not None:
            # Only successful runs are replied to, so the caller of `ask` learns
            # about this failure as a `TimeoutError`. Log it with the correlation
            # id so an operator can tie the two together.
            self.logger.error(
                "Actor %s failed handling conversation %s, no reply will be sent "
                "and the caller will time out.",
                consumer.name,
                message.conversation_id,
                exc_info=exc,
            )
            return
        if not message.reply_to:
            return
        response = self.msg_cls.new(
            result,
            topic=message.reply_to,
            kind="response",
            conversation_id=message.conversation_id,
            **consumer.options.get("reply_kwargs", {}),
        )
        await self.service.publish(response)
