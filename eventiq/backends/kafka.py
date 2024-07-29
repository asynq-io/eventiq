from __future__ import annotations

from datetime import timedelta
from itertools import chain
from typing import TYPE_CHECKING, Annotated, Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord, TopicPartition
from anyio.streams.memory import MemoryObjectSendStream
from pydantic import AnyUrl, UrlConstraints

from eventiq.broker import UrlBroker
from eventiq.exceptions import BrokerError
from eventiq.settings import UrlBrokerSettings
from eventiq.types import Encoder
from eventiq.utils import utc_now

KafkaUrl = Annotated[AnyUrl, UrlConstraints(allowed_schemes=["kafka"])]


class KafkaSettings(UrlBrokerSettings[KafkaUrl]):
    consumer_options: dict[str, Any] = {}


if TYPE_CHECKING:
    from eventiq import CloudEvent, Consumer


class KafkaBroker(UrlBroker[ConsumerRecord, None]):
    """
    Kafka backend
    :param consumer_options: extra options (defaults) for AIOKafkaConsumer
    :param kwargs: Broker base class parameters
    """

    WILDCARD_MANY = "*"
    WILDCARD_ONE = r"\w+"

    Settings = KafkaSettings
    protocol = "kafka"

    def __init__(
        self,
        *,
        consumer_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.consumer_options = consumer_options or {}
        self._publisher = None
        self._subcsribers: dict[int, AIOKafkaConsumer] = {}

    @staticmethod
    def get_message_data(raw_message: ConsumerRecord) -> bytes:
        return raw_message.value or b""

    @staticmethod
    def get_message_metadata(
        raw_message: ConsumerRecord,
    ) -> dict[str, str]:
        metadata = {
            "messaging.kafka.message.offset": str(raw_message.offset),
            "messaging.kafka.destination.partition": str(raw_message.partition),
        }
        if raw_message.key:
            metadata["messaging.kafka.message.key"] = str(raw_message.key)
        return metadata

    @property
    def is_connected(self) -> bool:
        return True

    def should_nack(self, raw_message: ConsumerRecord) -> bool:
        return (
            raw_message.timestamp
            < (utc_now() + timedelta(seconds=self.validate_error_delay)).timestamp()
        )

    async def sender(
        self,
        group: str,
        consumer: Consumer,
        send_stream: MemoryObjectSendStream[ConsumerRecord],
    ):
        subscriber = AIOKafkaConsumer(
            group_id=f"{group}:{consumer.name}",
            bootstrap_servers=self.url,
            enable_auto_commit=False,
            **consumer.options.get("kafka_consumer_options", self.consumer_options),
        )
        subscriber.subscribe(pattern=self.format_topic(consumer.topic))
        await subscriber.start()
        timeout_ms = consumer.options.get("timeout_ms", 600)
        try:
            async with send_stream:
                while True:
                    result = await subscriber.getmany(timeout_ms=timeout_ms)
                    for message in chain.from_iterable(result.values()):
                        self._subcsribers[id(message)] = subscriber
                        await send_stream.send(message)
        finally:
            await subscriber.stop()

            if consumer.dynamic:
                subscriber.unsubscribe()

    async def ack(self, raw_message: ConsumerRecord) -> None:
        subscriber = self._subcsribers.pop(id(raw_message), None)
        if subscriber:
            await subscriber.commit(
                {
                    TopicPartition(
                        raw_message.topic, raw_message.partition
                    ): raw_message.offset + 1
                }
            )

    async def nack(self, raw_message: ConsumerRecord, delay: int | None = None) -> None:
        self._subcsribers.pop(id(raw_message), None)

    async def disconnect(self):
        if self._publisher:
            await self._publisher.stop()

    @property
    def publisher(self) -> AIOKafkaProducer:
        if self._publisher is None:
            raise BrokerError("Broker not connected")
        return self._publisher

    async def connect(self):
        self._publisher = AIOKafkaProducer(
            bootstrap_servers=self.url, **self.connection_options
        )
        await self._publisher.start()

    async def publish(
        self,
        message: CloudEvent,
        encoder: Encoder | None = None,
        key: Any | None = None,
        partition: Any | None = None,
        headers: dict[str, str] | None = None,
        timestamp_ms: int | None = None,
        **kwargs: Any,
    ):
        data = self._encode_message(message, encoder)
        timestamp_ms = timestamp_ms or int(message.time.timestamp() * 1000)
        key = key or getattr(message, "key", str(message.id))
        headers = headers or {}
        headers.setdefault("Content-Type", self.encoder.CONTENT_TYPE)
        await self.publisher.send(
            topic=message.topic,
            value=data,
            key=key,
            partition=partition,
            headers=headers,
            timestamp_ms=timestamp_ms,
        )
