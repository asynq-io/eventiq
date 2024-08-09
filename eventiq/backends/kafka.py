from __future__ import annotations

from datetime import datetime, timedelta
from itertools import chain
from typing import TYPE_CHECKING, Annotated, Any
from weakref import WeakValueDictionary

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord, TopicPartition
from anyio import move_on_after
from pydantic import AnyUrl, Field, UrlConstraints

from eventiq.broker import UrlBroker
from eventiq.settings import UrlBrokerSettings
from eventiq.utils import utc_now

if TYPE_CHECKING:
    from anyio.streams.memory import MemoryObjectSendStream

    from eventiq import Consumer
    from eventiq.types import ID, DecodedMessage

KafkaUrl = Annotated[AnyUrl, UrlConstraints(allowed_schemes=["kafka"])]


class KafkaSettings(UrlBrokerSettings[KafkaUrl]):
    consumer_options: dict[str, Any] = Field({})


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
        self._subcsribers: WeakValueDictionary[int, AIOKafkaConsumer] = (
            WeakValueDictionary()
        )

    @staticmethod
    def decode_message(raw_message: ConsumerRecord) -> DecodedMessage:
        data = raw_message.value or b""
        headers = {k: str(v) for k, v in raw_message.headers}
        return data, headers

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
    ) -> None:
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
            with move_on_after(1, shield=True):
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

    async def disconnect(self) -> None:
        if self._publisher:
            await self._publisher.stop()

    @property
    def publisher(self) -> AIOKafkaProducer:
        if self._publisher is None:
            raise self.connection_error
        return self._publisher

    async def connect(self) -> None:
        if self._publisher is None:
            _publisher = AIOKafkaProducer(
                bootstrap_servers=self.url, **self.connection_options
            )
            self._publisher = _publisher
            await _publisher.start()

    async def publish(
        self,
        topic: str,
        body: bytes,
        *,
        headers: dict[str, str],
        message_id: ID,
        message_time: datetime,
        timestamp_ms: int | None = None,
        partition: int | None = None,
        **kwargs: Any,
    ) -> None:
        if timestamp_ms is None:
            timestamp_ms = int(message_time.timestamp() * 1000)
        await self.publisher.send(
            topic=topic,
            value=body,
            key=str(message_id),
            partition=partition,
            timestamp_ms=timestamp_ms,
            headers=headers,
        )
