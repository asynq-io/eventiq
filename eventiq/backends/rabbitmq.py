from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Any, Union

import aio_pika
from aio_pika.abc import (
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractRobustConnection,
)
from aiormq.abc import ConfirmationFrameType
from anyio import move_on_after
from pydantic import AnyUrl, UrlConstraints

from eventiq.broker import UrlBroker
from eventiq.exceptions import BrokerError
from eventiq.settings import UrlBrokerSettings

if TYPE_CHECKING:
    from anyio.streams.memory import MemoryObjectSendStream

    from eventiq import CloudEvent, Consumer
    from eventiq.types import DecodedMessage, Encoder


RabbitmqUrl = Annotated[AnyUrl, UrlConstraints(allowed_schemes=["amqp"])]


class RabbitMQSettings(UrlBrokerSettings[RabbitmqUrl]):
    default_prefetch_count: int = 10
    exchange_name: str = "default"


class RabbitmqBroker(
    UrlBroker[AbstractIncomingMessage, Union[ConfirmationFrameType, None]],
):
    """RabbitMQ broker implementation, based on `aio_pika` library.
    :param default_prefetch_count: default number of messages to prefetch (per queue)
    :param queue_options: additional queue options
    :param exchange_name: global exchange name
    :param kwargs: Broker base class parameters.
    """

    Settings = RabbitMQSettings
    protocol = "amqp"
    WILDCARD_ONE = "*"
    WILDCARD_MANY = "#"

    def __init__(
        self,
        *,
        default_prefetch_count: int = 10,
        queue_options: dict[str, Any] | None = None,
        exchange_name: str = "events",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.default_prefetch_count = default_prefetch_count
        self.queue_options = queue_options or {}
        self.exchange_name = exchange_name
        self._connection: AbstractRobustConnection | None = None
        self._exchange: AbstractExchange | None = None

    @property
    def connection(self) -> AbstractRobustConnection:
        if self._connection is None:
            msg = "Not connected"
            raise BrokerError(msg)
        return self._connection

    @property
    def exchange(self) -> AbstractExchange:
        if self._exchange is None:
            msg = "Not connected"
            raise BrokerError(msg)
        return self._exchange

    def should_nack(self, raw_message: AbstractIncomingMessage) -> bool:
        return bool(raw_message.redelivered)

    async def connect(self) -> None:
        self._connection = await aio_pika.connect_robust(
            self.url,
            **self.connection_options,
        )
        channel = await self.connection.channel()
        self._exchange = await channel.declare_exchange(
            name=self.exchange_name,
            type=aio_pika.ExchangeType.TOPIC,
            durable=True,
        )

    async def disconnect(self) -> None:
        await self.connection.close()

    async def sender(
        self,
        group: str,
        consumer: Consumer,
        send_stream: MemoryObjectSendStream,
    ) -> None:
        channel = await self.connection.channel()
        prefetch_count = consumer.concurrency * 2
        await channel.set_qos(prefetch_count=prefetch_count)
        options: dict[str, Any] = consumer.options.get(
            "queue_options",
            self.queue_options,
        )
        if not consumer.dynamic:
            options["durable"] = True
        queue = await channel.declare_queue(name=f"{group}:{consumer.name}", **options)
        await queue.bind(self.exchange, routing_key=consumer.topic)
        try:
            async with send_stream, queue.iterator() as q:
                async for message in q:
                    await send_stream.send(message)
        finally:
            with move_on_after(1, shield=True):
                if consumer.dynamic:
                    await queue.unbind(self.exchange, routing_key=consumer.topic)
                await channel.close()

    async def publish(
        self,
        message: CloudEvent,
        encoder: Encoder | None = None,
        **kwargs: Any,
    ) -> ConfirmationFrameType | None:
        data = self._encode_message(message, encoder=encoder)
        timeout = kwargs.get("timeout")
        headers = message.headers
        headers.setdefault("Content-Type", self.encoder.CONTENT_TYPE)
        msg = aio_pika.Message(
            headers=dict(headers),
            body=data,
            app_id=message.source,
            content_type=message.content_type,
            timestamp=message.time,
            message_id=str(message.id),
            type=message.type,
            content_encoding="UTF-8",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        return await self.exchange.publish(
            msg,
            routing_key=message.topic,
            timeout=timeout,
        )

    async def ack(self, raw_message: AbstractIncomingMessage) -> None:
        await raw_message.ack()

    async def nack(
        self,
        raw_message: AbstractIncomingMessage,
        delay: int | None = None,
    ) -> None:
        await raw_message.reject(requeue=True)

    @property
    def is_connected(self) -> bool:
        return not self.connection.is_closed

    @staticmethod
    def decode_message(raw_message: AbstractIncomingMessage) -> DecodedMessage:
        headers = {k: str(v) for k, v in raw_message.headers.items()}
        return raw_message.body, headers

    @staticmethod
    def get_message_metadata(raw_message: AbstractIncomingMessage) -> dict[str, str]:
        d = {
            k: getattr(raw_message, k, None)
            for k in (
                "cluster_id",
                "consumer_tag",
                "redelivered",
                "message_count",
                "routing_key",
                "exchange",
            )
        }
        return {
            f"messaging.rabbitmq.{k}": str(v) for k, v in d.items() if v is not None
        }
