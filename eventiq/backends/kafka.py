from __future__ import annotations

from datetime import datetime, timedelta
from itertools import chain
from typing import TYPE_CHECKING, Annotated, Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord, TopicPartition
from aiokafka.abc import ConsumerRebalanceListener
from anyio import move_on_after
from pydantic import AnyUrl, Field, UrlConstraints

from eventiq.broker import UrlBroker
from eventiq.settings import UrlBrokerSettings
from eventiq.utils import get_topic_regex, utc_now

if TYPE_CHECKING:
    from collections.abc import Iterable

    from anyio.streams.memory import MemoryObjectSendStream

    from eventiq import Consumer
    from eventiq.types import ID, DecodedMessage

KafkaUrl = Annotated[AnyUrl, UrlConstraints(allowed_schemes=["kafka"])]

SUBSCRIPTION_ATTR = "eventiq_subscription"


class KafkaSettings(UrlBrokerSettings[KafkaUrl]):
    consumer_options: dict[str, Any] = Field({})


class PartitionOffsets:
    """Tracks acked offsets for one partition, exposing the contiguous watermark.

    Committing `offset + 1` per message would mark earlier, still-unfinished
    messages as consumed whenever concurrency lets a later offset finish first.
    Only offsets below the first gap may be committed.
    """

    def __init__(self, first_offset: int) -> None:
        self.next_offset = first_offset
        self.acked: set[int] = set()
        self.blocked = False

    def ack(self, offset: int) -> int | None:
        """Record an ack, returning the new commit position if it advanced."""
        if self.blocked or offset < self.next_offset:
            return None
        self.acked.add(offset)
        advanced = False
        while self.next_offset in self.acked:
            self.acked.discard(self.next_offset)
            self.next_offset += 1
            advanced = True
        return self.next_offset if advanced else None

    def block(self) -> None:
        """Stop committing this partition, pinning it below the nacked offset.

        The watermark could never cross the gap the nacked offset leaves, so
        later acks are dropped instead of accumulating for the process lifetime.
        """
        self.blocked = True
        self.acked.clear()


class KafkaSubscription:
    """Owns a subscriber and the offset bookkeeping for its partitions.

    A nacked message stops its partition from committing, so it stays
    uncommitted and is redelivered once the partition is assigned again.
    """

    def __init__(self, subscriber: AIOKafkaConsumer) -> None:
        self.subscriber = subscriber
        self._partitions: dict[TopicPartition, PartitionOffsets] = {}

    def track(self, record: ConsumerRecord) -> None:
        tp = TopicPartition(record.topic, record.partition)
        if tp not in self._partitions:
            self._partitions[tp] = PartitionOffsets(record.offset)
        setattr(record, SUBSCRIPTION_ATTR, self)

    def forget(self, partitions: Iterable[TopicPartition]) -> None:
        """Drop bookkeeping for partitions whose ownership just changed."""
        for tp in partitions:
            self._partitions.pop(tp, None)

    async def ack(self, record: ConsumerRecord) -> None:
        tp = TopicPartition(record.topic, record.partition)
        offsets = self._partitions.get(tp)
        if offsets is None:
            return
        commit_at = offsets.ack(record.offset)
        if commit_at is not None:
            await self.subscriber.commit({tp: commit_at})

    def nack(self, record: ConsumerRecord) -> None:
        """Leave `record`, and everything after it, uncommitted."""
        tp = TopicPartition(record.topic, record.partition)
        offsets = self._partitions.get(tp)
        if offsets is not None:
            offsets.block()


class KafkaRebalanceListener(ConsumerRebalanceListener):
    """Discards offset bookkeeping whenever partition ownership changes."""

    def __init__(self, subscription: KafkaSubscription) -> None:
        self.subscription = subscription

    def on_partitions_revoked(self, revoked: Iterable[TopicPartition]) -> None:
        self.subscription.forget(revoked)

    def on_partitions_assigned(self, assigned: Iterable[TopicPartition]) -> None:
        # State held for a partition we are assigned may predate another member
        # owning it, so start tracking from the first record seen again.
        self.subscription.forget(assigned)


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
        self._publisher: AIOKafkaProducer | None = None

    @staticmethod
    def decode_message(raw_message: ConsumerRecord) -> DecodedMessage:
        data = raw_message.value or b""
        # Kafka header values are arbitrary bytes, so a strict decode would make
        # one binary header drop the whole message.
        headers = {
            k: v.decode(errors="replace") if isinstance(v, bytes) else str(v)
            for k, v in raw_message.headers
        }
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
        return self._publisher is not None

    async def check_health(self) -> bool:
        if self._publisher is None:
            return False
        await self._publisher.client.fetch_all_metadata()
        return True

    def should_nack(self, raw_message: ConsumerRecord) -> bool:
        # ConsumerRecord.timestamp is epoch milliseconds; nack only while the
        # message is younger than the retry window.
        cutoff = utc_now() - timedelta(seconds=self.validate_error_delay)
        return raw_message.timestamp / 1000 > cutoff.timestamp()

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
        subscription = KafkaSubscription(subscriber)
        # An anchored, escaped regex: aiokafka matches topics with `re.match`,
        # so `format_topic` output would match unrelated topics (or not compile).
        subscriber.subscribe(
            pattern=get_topic_regex(consumer.topic),
            listener=KafkaRebalanceListener(subscription),
        )
        await subscriber.start()
        timeout_ms = consumer.options.get("timeout_ms", 600)

        try:
            async with send_stream:
                while True:
                    result = await subscriber.getmany(timeout_ms=timeout_ms)
                    for message in chain.from_iterable(result.values()):
                        subscription.track(message)
                        await send_stream.send(message)
        finally:
            with move_on_after(1, shield=True):
                if consumer.dynamic:
                    subscriber.unsubscribe()
                await subscriber.stop()

    async def ack(self, raw_message: ConsumerRecord) -> None:
        subscription = getattr(raw_message, SUBSCRIPTION_ATTR, None)
        if isinstance(subscription, KafkaSubscription):
            await subscription.ack(raw_message)

    async def nack(self, raw_message: ConsumerRecord, delay: int | None = None) -> None:
        """Reject a message by leaving its offset uncommitted.

        Kafka has no per-message redelivery: seeking the partition back would
        refetch the record and every successor on every failure, and nack is the
        default failure action. The record is redelivered when the partition is
        next assigned, to this consumer or another group member.
        """
        if delay is not None:
            self.logger.warning("delay is not supported expected None got %d", delay)
        subscription = getattr(raw_message, SUBSCRIPTION_ATTR, None)
        if isinstance(subscription, KafkaSubscription):
            self.logger.warning(
                "Nacked %s[%s] at offset %s: no offset is committed for this "
                "partition until it is reassigned.",
                raw_message.topic,
                raw_message.partition,
                raw_message.offset,
            )
            subscription.nack(raw_message)

    async def disconnect(self) -> None:
        if self._publisher is None:
            return
        publisher, self._publisher = self._publisher, None
        await publisher.stop()

    @property
    def publisher(self) -> AIOKafkaProducer:
        if self._publisher is None:
            raise self.connection_error
        return self._publisher

    async def connect(self) -> None:
        if self._publisher is None:
            publisher = AIOKafkaProducer(
                bootstrap_servers=self.url,
                **self.connection_options,
            )
            # Assign only after a successful start, so a failed connect can be retried.
            await publisher.start()
            self._publisher = publisher

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
        **_: Any,
    ) -> None:
        if timestamp_ms is None:
            timestamp_ms = int(message_time.timestamp() * 1000)
        # aiokafka serializes nothing by default: `key` must already be bytes and
        # headers an iterable of (str, bytes) pairs. `send_and_wait` surfaces
        # broker-side delivery failures that `send()` would swallow.
        await self.publisher.send_and_wait(
            topic=topic,
            value=body,
            key=str(message_id).encode(),
            partition=partition,
            timestamp_ms=timestamp_ms,
            headers=[(k, v.encode()) for k, v in headers.items()],
        )
