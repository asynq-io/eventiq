"""Tests for broker base class, StubBroker, and all backend broker implementations."""

from __future__ import annotations

import asyncio
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import anyio
import pytest
from aiokafka import TopicPartition
from aiokafka.abc import ConsumerRebalanceListener
from typing_extensions import Self

from eventiq.backends.kafka import (
    KafkaBroker,
    KafkaRebalanceListener,
    KafkaSubscription,
    PartitionOffsets,
)
from eventiq.backends.nats import JetStreamBroker, NatsBroker
from eventiq.backends.rabbitmq import RabbitmqBroker
from eventiq.backends.redis import RedisBroker
from eventiq.backends.stub import StubBroker, StubMessage
from eventiq.broker import Broker, UrlBroker
from eventiq.exceptions import BrokerConnectionError, BrokerError
from eventiq.settings import UrlBrokerSettings
from eventiq.utils import utc_now


def _consumer(
    topic: str = "test.topic",
    *,
    dynamic: bool = False,
    concurrency: int = 1,
) -> MagicMock:
    c = MagicMock()
    c.name = "test_consumer"
    c.topic = topic
    c.options = {}
    c.dynamic = dynamic
    c.concurrency = concurrency
    c.timeout = None
    return c


# ===========================================================================
# Broker base class — general & parametrized
# ===========================================================================

_backends = (NatsBroker, JetStreamBroker, KafkaBroker, RabbitmqBroker, RedisBroker)


@pytest.mark.parametrize("broker_class", _backends)
def test_is_subclass(broker_class):
    assert issubclass(broker_class, Broker)


@pytest.mark.parametrize("broker_class", _backends)
def test_from_env(broker_class):
    os.environ["BROKER_URL"] = f"{broker_class.protocol}://localhost:1111"
    broker_class.from_env()


def test_broker_repr(broker):
    assert repr(broker) == "StubBroker"


def test_broker_should_nack(broker):
    assert broker.should_nack(object()) is False


def test_broker_get_num_delivered_with_attr(broker):
    class Msg:
        num_delivered = 3

    assert broker.get_num_delivered(Msg()) == 3


def test_broker_get_num_delivered_no_attr(broker):
    assert broker.get_num_delivered(object()) is None


@pytest.mark.anyio
async def test_broker_check_health_defaults_to_is_connected(broker):
    """Backends without a real probe keep working through the default."""
    assert await broker.check_health() is False
    await broker.connect()
    assert await broker.check_health() is True
    await broker.disconnect()


def test_stub_broker_from_settings():
    b = StubBroker.from_settings()
    assert isinstance(b, StubBroker)


def test_broker_from_env_missing_var():
    os.environ.pop("BROKER_CLASS", None)
    with pytest.raises(BrokerError, match="BROKER_CLASS"):
        Broker.from_env()


def test_broker_from_env_with_stub():
    os.environ["BROKER_CLASS"] = "eventiq.backends.stub:StubBroker"
    try:
        b = Broker.from_env()
        assert isinstance(b, StubBroker)
    finally:
        del os.environ["BROKER_CLASS"]


def test_broker_subclass_missing_protocol():
    with pytest.raises(ValueError, match="must define a protocol"):

        class BrokenBroker(StubBroker):
            protocol = None


def test_broker_from_settings_explicit():
    from eventiq.backends.stub import StubSettings

    settings = StubSettings(wait_on_publish=False)
    b = StubBroker.from_settings(settings=settings)
    assert isinstance(b, StubBroker)
    assert b.wait_on_publish is False


class _MinimalUrlBroker(UrlBroker):
    protocol = "test"
    Settings = UrlBrokerSettings
    WILDCARD_ONE = r"\w+"
    WILDCARD_MANY = r".*"

    @staticmethod
    def decode_message(raw_message: object) -> tuple[bytes, dict]:
        return b"{}", {}

    @property
    def is_connected(self) -> bool:
        return False

    async def publish(  # type: ignore[override]
        self, _topic: str, _body: bytes, *, headers: dict, **_kwargs: object
    ) -> dict:
        return {}

    async def connect(self) -> None:
        pass

    async def disconnect(self) -> None:
        pass

    async def ack(self, _raw_message: object) -> None:
        pass

    async def nack(self, _raw_message: object, delay: int | None = None) -> None:
        pass

    async def sender(
        self, _group: str, _consumer: object, _send_stream: object
    ) -> None:
        pass


def test_url_broker_connection_error():
    broker = _MinimalUrlBroker(url="test://localhost:1234/mypath")
    err = broker.connection_error
    assert isinstance(err, BrokerConnectionError)


def test_url_broker_get_info():
    broker = _MinimalUrlBroker(url="test://localhost:1234/mypath")
    info = broker.get_info()
    assert info["host"] == "localhost"
    assert "mypath" in info["pathname"]


# ===========================================================================
# StubBroker
# ===========================================================================


@pytest.mark.anyio
async def test_stub_broker_nack_with_delay():
    broker = StubBroker()
    await broker.connect()
    queue: asyncio.Queue = asyncio.Queue()
    event = asyncio.Event()
    raw = StubMessage(data=b"{}", queue=queue, event=event)
    await queue.put(raw)
    await queue.get()
    await broker.nack(raw, delay=60)
    assert queue.empty()
    await broker.disconnect()


@pytest.mark.anyio
async def test_stub_broker_publish_no_match():
    broker = StubBroker(wait_on_publish=False)
    await broker.connect()
    broker.topics["other.topic"]
    result = await broker.publish("test.topic", b"{}", headers={})
    assert result == {}
    await broker.disconnect()


@pytest.mark.anyio
async def test_stub_broker_publish_no_wait():
    broker = StubBroker(wait_on_publish=False)
    await broker.connect()
    broker.topics["test.topic"]
    result = await broker.publish("test.topic", b"{}", headers={})
    assert "test.topic" in result
    await broker.disconnect()


@pytest.mark.anyio
async def test_stub_broker_publish_tolerates_topic_registered_while_awaiting():
    """Senders register their topic lazily, mid-publish, resizing `topics`."""
    broker = StubBroker(wait_on_publish=False)
    await broker.connect()
    queue = broker.topics["test.topic"]
    original_put = queue.put

    async def put_and_register(message: StubMessage) -> None:
        broker.topics["late.topic"]  # a sender starting up while publish awaits
        await original_put(message)

    queue.put = put_and_register
    result = await broker.publish("test.topic", b"{}", headers={})

    assert "test.topic" in result
    await broker.disconnect()


@pytest.mark.anyio
async def test_stub_broker_disconnect_no_task():
    broker = StubBroker()
    await broker.disconnect()


@pytest.mark.anyio
async def test_stub_broker_nack_no_delay():
    broker = StubBroker()
    await broker.connect()
    queue: asyncio.Queue = asyncio.Queue()
    event = asyncio.Event()
    raw = StubMessage(data=b"{}", queue=queue, event=event)
    await queue.put(raw)
    await queue.get()
    await broker.nack(raw)
    assert not queue.empty()
    await broker.disconnect()


@pytest.mark.anyio
async def test_stub_delay_worker_processes_past():
    broker = StubBroker()
    await broker.connect()
    queue: asyncio.Queue = asyncio.Queue()
    event = asyncio.Event()
    raw = StubMessage(data=b"{}", queue=queue, event=event)
    past = datetime.now(tz=timezone.utc) - timedelta(seconds=10)
    await broker._delay_queue.put((raw, past))
    await asyncio.sleep(0.15)
    assert not queue.empty()
    await broker.disconnect()


@pytest.mark.anyio
async def test_stub_delay_worker_requeues_future():
    broker = StubBroker()
    await broker.connect()
    queue: asyncio.Queue = asyncio.Queue()
    event = asyncio.Event()
    raw = StubMessage(data=b"{}", queue=queue, event=event)
    future = datetime.now(tz=timezone.utc) + timedelta(seconds=100)
    await broker._delay_queue.put((raw, future))
    await asyncio.sleep(0.15)
    assert queue.empty()
    await broker.disconnect()


# ===========================================================================
# KafkaBroker
# ===========================================================================


def test_kafka_decode_message():
    record = MagicMock()
    record.value = b'{"x": 1}'
    record.headers = [("content-type", "application/json")]
    data, headers = KafkaBroker.decode_message(record)
    assert data == b'{"x": 1}'
    assert headers == {"content-type": "application/json"}


def test_kafka_decode_message_no_value():
    record = MagicMock()
    record.value = None
    record.headers = []
    data, _ = KafkaBroker.decode_message(record)
    assert data == b""


def test_kafka_get_message_metadata_with_key():
    record = MagicMock()
    record.offset = 42
    record.partition = 0
    record.key = b"my-key"
    meta = KafkaBroker.get_message_metadata(record)
    assert meta["messaging.kafka.message.offset"] == "42"
    assert "messaging.kafka.message.key" in meta


def test_kafka_get_message_metadata_no_key():
    record = MagicMock()
    record.offset = 1
    record.partition = 2
    record.key = None
    meta = KafkaBroker.get_message_metadata(record)
    assert "messaging.kafka.message.key" not in meta


def test_kafka_is_connected_reflects_producer_state():
    assert not KafkaBroker(url="kafka://localhost:9092").is_connected


def test_kafka_should_not_nack_old_message():
    """An old message is past the retry window, so it must not be nacked."""
    broker = KafkaBroker(url="kafka://localhost:9092")
    record = MagicMock()
    record.timestamp = 0
    assert not broker.should_nack(record)


def test_kafka_should_nack_recent_message():
    """A message younger than validate_error_delay is still retryable."""
    broker = KafkaBroker(url="kafka://localhost:9092")
    record = MagicMock()
    record.timestamp = int(utc_now().timestamp() * 1000)
    assert broker.should_nack(record)


def test_kafka_publisher_raises_when_not_connected():
    with pytest.raises(BrokerConnectionError):
        _ = KafkaBroker(url="kafka://localhost:9092").publisher


@pytest.mark.anyio
async def test_kafka_connect_starts_producer():
    mock_producer = AsyncMock()
    with patch("eventiq.backends.kafka.AIOKafkaProducer", return_value=mock_producer):
        broker = KafkaBroker(url="kafka://localhost:9092")
        await broker.connect()
        mock_producer.start.assert_called_once()
        assert broker._publisher is mock_producer


@pytest.mark.anyio
async def test_kafka_connect_idempotent():
    mock_producer = AsyncMock()
    with patch("eventiq.backends.kafka.AIOKafkaProducer", return_value=mock_producer):
        broker = KafkaBroker(url="kafka://localhost:9092")
        await broker.connect()
        await broker.connect()
        mock_producer.start.assert_called_once()


@pytest.mark.anyio
async def test_kafka_check_health_fetches_metadata():
    """A live producer object proves nothing; the probe must reach the cluster."""
    mock_producer = AsyncMock()
    with patch("eventiq.backends.kafka.AIOKafkaProducer", return_value=mock_producer):
        broker = KafkaBroker(url="kafka://localhost:9092")
        await broker.connect()

        assert await broker.check_health() is True

    mock_producer.client.fetch_all_metadata.assert_awaited_once()


@pytest.mark.anyio
async def test_kafka_check_health_surfaces_dead_cluster():
    """`is_connected` still reports True once the cluster is gone."""
    mock_producer = AsyncMock()
    mock_producer.client.fetch_all_metadata = AsyncMock(
        side_effect=ConnectionError("cluster gone")
    )
    with patch("eventiq.backends.kafka.AIOKafkaProducer", return_value=mock_producer):
        broker = KafkaBroker(url="kafka://localhost:9092")
        await broker.connect()

        assert broker.is_connected
        with pytest.raises(ConnectionError):
            await broker.check_health()


@pytest.mark.anyio
async def test_kafka_check_health_before_connect():
    assert await KafkaBroker(url="kafka://localhost:9092").check_health() is False


@pytest.mark.anyio
async def test_kafka_disconnect_stops_producer():
    mock_producer = AsyncMock()
    with patch("eventiq.backends.kafka.AIOKafkaProducer", return_value=mock_producer):
        broker = KafkaBroker(url="kafka://localhost:9092")
        await broker.connect()
        await broker.disconnect()
        mock_producer.stop.assert_called_once()


@pytest.mark.anyio
async def test_kafka_disconnect_no_publisher():
    await KafkaBroker(url="kafka://localhost:9092").disconnect()


@pytest.mark.anyio
async def test_kafka_publish():
    mock_producer = AsyncMock()
    with patch("eventiq.backends.kafka.AIOKafkaProducer", return_value=mock_producer):
        broker = KafkaBroker(url="kafka://localhost:9092")
        await broker.connect()
        await broker.publish(
            "test.topic",
            b"body",
            headers={"Content-Type": "application/json"},
            message_id="msg-1",
            message_time=datetime.now(timezone.utc),
        )
        kwargs = mock_producer.send_and_wait.call_args.kwargs
        assert kwargs["topic"] == "test.topic"
        assert kwargs["value"] == b"body"


@pytest.mark.anyio
async def test_kafka_publish_explicit_timestamp_ms():
    mock_producer = AsyncMock()
    with patch("eventiq.backends.kafka.AIOKafkaProducer", return_value=mock_producer):
        broker = KafkaBroker(url="kafka://localhost:9092")
        await broker.connect()
        await broker.publish(
            "t",
            b"x",
            headers={},
            message_id="id",
            message_time=datetime.now(timezone.utc),
            timestamp_ms=12345,
        )
        assert mock_producer.send_and_wait.call_args.kwargs["timestamp_ms"] == 12345


def _kafka_record(topic: str, partition: int, offset: int) -> MagicMock:
    record = MagicMock()
    record.topic = topic
    record.partition = partition
    record.offset = offset
    return record


def _kafka_subscription() -> tuple[KafkaSubscription, AsyncMock]:
    """Subscription over a mock subscriber whose `seek` stays synchronous."""
    subscriber = AsyncMock()
    subscriber.seek = MagicMock()
    return KafkaSubscription(subscriber), subscriber


@pytest.mark.anyio
async def test_kafka_ack_commits_offset():
    broker = KafkaBroker(url="kafka://localhost:9092")
    subscriber = AsyncMock()
    subscription = KafkaSubscription(subscriber)
    record = _kafka_record("t", 0, 5)
    subscription.track(record)

    await broker.ack(record)

    subscriber.commit.assert_called_once_with({TopicPartition("t", 0): 6})


@pytest.mark.anyio
async def test_kafka_ack_no_subscription_is_noop():
    await KafkaBroker(url="kafka://localhost:9092").ack(MagicMock())


@pytest.mark.anyio
async def test_kafka_ack_commits_only_contiguous_offsets():
    """A later ack must not commit past an earlier message that is still in flight."""
    broker = KafkaBroker(url="kafka://localhost:9092")
    subscriber = AsyncMock()
    subscription = KafkaSubscription(subscriber)
    first = _kafka_record("t", 0, 5)
    second = _kafka_record("t", 0, 6)
    subscription.track(first)
    subscription.track(second)

    await broker.ack(second)
    subscriber.commit.assert_not_called()

    await broker.ack(first)
    subscriber.commit.assert_called_once_with({TopicPartition("t", 0): 7})


@pytest.mark.anyio
async def test_kafka_nack_does_not_commit():
    """A nacked offset stays uncommitted so the message can be redelivered."""
    broker = KafkaBroker(url="kafka://localhost:9092")
    subscription, subscriber = _kafka_subscription()
    nacked = _kafka_record("t", 0, 5)
    later = _kafka_record("t", 0, 6)
    subscription.track(nacked)
    subscription.track(later)

    await broker.nack(nacked)
    await broker.ack(later)

    subscriber.commit.assert_not_called()


@pytest.mark.anyio
async def test_kafka_nack_with_delay_warns():
    await KafkaBroker(url="kafka://localhost:9092").nack(MagicMock(), delay=5)


@pytest.mark.anyio
async def test_kafka_sender_lifecycle():
    mock_sub = MagicMock()
    mock_sub.subscribe = MagicMock()
    mock_sub.start = AsyncMock()
    mock_sub.stop = AsyncMock()

    async def blocking(**kwargs: Any) -> dict:
        await anyio.sleep(10)
        return {}

    mock_sub.getmany = blocking
    with patch("eventiq.backends.kafka.AIOKafkaConsumer", return_value=mock_sub):
        broker = KafkaBroker(url="kafka://localhost:9092")
        send, _ = anyio.create_memory_object_stream()
        async with anyio.create_task_group() as tg:
            tg.start_soon(broker.sender, "grp", _consumer(), send)
            await anyio.sleep(0.01)
            tg.cancel_scope.cancel()

    mock_sub.subscribe.assert_called_once()
    mock_sub.start.assert_called_once()
    mock_sub.stop.assert_called_once()


@pytest.mark.anyio
async def test_kafka_sender_delivers_message():
    mock_sub = MagicMock()
    mock_sub.subscribe = MagicMock()
    mock_sub.start = AsyncMock()
    mock_sub.stop = AsyncMock()
    record = MagicMock()
    call_count = 0

    async def getmany_once(**kwargs: Any) -> dict:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {MagicMock(): [record]}
        await anyio.sleep(10)
        return {}

    mock_sub.getmany = getmany_once
    with patch("eventiq.backends.kafka.AIOKafkaConsumer", return_value=mock_sub):
        broker = KafkaBroker(url="kafka://localhost:9092")
        send, receive = anyio.create_memory_object_stream()
        async with anyio.create_task_group() as tg:
            tg.start_soon(broker.sender, "grp", _consumer(), send)
            received = await receive.receive()
            assert received is record
            tg.cancel_scope.cancel()


@pytest.mark.anyio
async def test_kafka_sender_dynamic_unsubscribes():
    mock_sub = MagicMock()
    mock_sub.subscribe = MagicMock()
    mock_sub.start = AsyncMock()
    mock_sub.stop = AsyncMock()
    mock_sub.unsubscribe = MagicMock()

    async def blocking(**kwargs: Any) -> dict:
        await anyio.sleep(10)
        return {}

    mock_sub.getmany = blocking
    with patch("eventiq.backends.kafka.AIOKafkaConsumer", return_value=mock_sub):
        broker = KafkaBroker(url="kafka://localhost:9092")
        send, _ = anyio.create_memory_object_stream()
        async with anyio.create_task_group() as tg:
            tg.start_soon(broker.sender, "grp", _consumer(dynamic=True), send)
            await anyio.sleep(0.01)
            tg.cancel_scope.cancel()

    mock_sub.unsubscribe.assert_called_once()


async def _run_kafka_sender(topic: str) -> MagicMock:
    """Run `sender` until it blocks on the first fetch, returning the subscriber."""
    mock_sub = MagicMock()
    mock_sub.subscribe = MagicMock()
    mock_sub.start = AsyncMock()
    mock_sub.stop = AsyncMock()

    async def blocking(**kwargs: Any) -> dict:
        await anyio.sleep(10)
        return {}

    mock_sub.getmany = blocking
    with patch("eventiq.backends.kafka.AIOKafkaConsumer", return_value=mock_sub):
        broker = KafkaBroker(url="kafka://localhost:9092")
        send, _ = anyio.create_memory_object_stream()
        async with anyio.create_task_group() as tg:
            tg.start_soon(broker.sender, "grp", _consumer(topic), send)
            await anyio.sleep(0.01)
            tg.cancel_scope.cancel()
    return mock_sub


@pytest.mark.anyio
async def test_kafka_sender_subscribes_with_anchored_pattern():
    """aiokafka matches topics with `re.match`, so the pattern must be anchored."""
    mock_sub = await _run_kafka_sender("test.topic")
    pattern = mock_sub.subscribe.call_args.kwargs["pattern"]
    assert re.match(pattern, "test.topic")
    assert re.match(pattern, "testXtopic") is None
    assert re.match(pattern, "test.topic.v2") is None
    assert re.match(pattern, "prefix.test.topic") is None


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("topic", "matching", "unrelated"),
    [
        ("*", "orders", "orders.created"),
        ("orders.>", "orders.created.v2", "orders"),
        ("orders.{tenant}", "orders.acme", "orders.acme.v2"),
    ],
)
async def test_kafka_sender_pattern_expands_wildcards(topic, matching, unrelated):
    """A wildcard topic must compile: `*` alone is not a valid regex."""
    mock_sub = await _run_kafka_sender(topic)
    pattern = mock_sub.subscribe.call_args.kwargs["pattern"]
    assert re.match(pattern, matching)
    assert re.match(pattern, unrelated) is None


@pytest.mark.anyio
async def test_kafka_sender_subscribes_with_rebalance_listener():
    """Offset bookkeeping must be dropped when partition ownership changes."""
    mock_sub = await _run_kafka_sender("test.topic")
    listener = mock_sub.subscribe.call_args.kwargs["listener"]
    assert isinstance(listener, ConsumerRebalanceListener)


def test_kafka_decode_message_binary_header():
    """Kafka header values are arbitrary bytes, so decoding must never raise."""
    record = MagicMock()
    record.value = b"{}"
    record.headers = [("trace", b"\xff\xfe"), ("content-type", "application/json")]
    data, headers = KafkaBroker.decode_message(record)
    assert data == b"{}"
    assert headers["content-type"] == "application/json"
    assert headers["trace"]


@pytest.mark.anyio
async def test_kafka_publish_key_is_bytes():
    """No key serializer is configured, so the key must already be bytes."""
    mock_producer = AsyncMock()
    with patch("eventiq.backends.kafka.AIOKafkaProducer", return_value=mock_producer):
        broker = KafkaBroker(url="kafka://localhost:9092")
        await broker.connect()
        await broker.publish(
            "t",
            b"x",
            headers={},
            message_id="msg-1",
            message_time=datetime.now(timezone.utc),
        )
    assert mock_producer.send_and_wait.call_args.kwargs["key"] == b"msg-1"


@pytest.mark.anyio
async def test_kafka_nack_does_not_rewind_partition():
    """Kafka has no per-message redelivery: seeking back would refetch forever."""
    broker = KafkaBroker(url="kafka://localhost:9092")
    subscription, subscriber = _kafka_subscription()
    record = _kafka_record("t", 0, 5)
    subscription.track(record)

    for _ in range(3):
        await broker.nack(record)

    subscriber.seek.assert_not_called()
    subscriber.commit.assert_not_called()


@pytest.mark.anyio
async def test_kafka_nack_stops_committing_partition():
    """No offset at or after a nacked one may be committed, in either ack order."""
    broker = KafkaBroker(url="kafka://localhost:9092")
    subscription, subscriber = _kafka_subscription()
    nacked = _kafka_record("t", 0, 5)
    earlier = _kafka_record("t", 0, 6)
    later = _kafka_record("t", 0, 7)
    for record in (nacked, earlier, later):
        subscription.track(record)

    await broker.ack(earlier)
    await broker.nack(nacked)
    await broker.ack(later)

    subscriber.commit.assert_not_called()


@pytest.mark.anyio
async def test_kafka_nack_untracked_partition_is_noop():
    broker = KafkaBroker(url="kafka://localhost:9092")
    subscription, subscriber = _kafka_subscription()
    record = _kafka_record("t", 0, 5)
    subscription.track(record)
    subscription.forget([TopicPartition("t", 0)])

    await broker.nack(record)

    subscriber.seek.assert_not_called()
    subscriber.commit.assert_not_called()


@pytest.mark.anyio
async def test_kafka_nacked_partition_commits_again_after_reassignment():
    """A blocked partition must not stay blocked once its state is dropped."""
    broker = KafkaBroker(url="kafka://localhost:9092")
    subscription, subscriber = _kafka_subscription()
    nacked = _kafka_record("t", 0, 5)
    subscription.track(nacked)
    await broker.nack(nacked)

    KafkaRebalanceListener(subscription).on_partitions_assigned(
        [TopicPartition("t", 0)]
    )
    redelivered = _kafka_record("t", 0, 5)
    subscription.track(redelivered)
    await broker.ack(redelivered)

    subscriber.commit.assert_called_once_with({TopicPartition("t", 0): 6})


class _FakeKafkaConsumer:
    """Consumer whose fetch position `seek` really moves, so a rewind refetches."""

    def __init__(self, records: list[MagicMock]) -> None:
        self.records = records
        self.position = 0
        self.subscribe = MagicMock()
        self.commit = AsyncMock()

    async def start(self) -> None:
        return

    async def stop(self) -> None:
        return

    def seek(self, tp: TopicPartition, offset: int) -> None:
        self.position = offset

    async def getmany(self, timeout_ms: int) -> dict[TopicPartition, list[MagicMock]]:
        if self.position >= len(self.records):
            await anyio.sleep(10)
            return {}
        batch = self.records[self.position :]
        self.position = len(self.records)
        return {TopicPartition("t", 0): batch}


@pytest.mark.anyio
async def test_kafka_sender_does_not_replay_nacked_records():
    """A message that always fails must not refetch itself, or its successors, forever."""
    records = [_kafka_record("t", 0, offset) for offset in range(3)]
    delivered: list[int] = []
    with patch(
        "eventiq.backends.kafka.AIOKafkaConsumer",
        return_value=_FakeKafkaConsumer(records),
    ):
        broker = KafkaBroker(url="kafka://localhost:9092")
        send, receive = anyio.create_memory_object_stream()
        async with anyio.create_task_group() as tg:
            tg.start_soon(broker.sender, "grp", _consumer(), send)
            with anyio.move_on_after(0.2):
                async for record in receive:
                    delivered.append(record.offset)
                    await broker.nack(record)
            tg.cancel_scope.cancel()

    assert delivered == [0, 1, 2]


@pytest.mark.anyio
async def test_kafka_revoked_partition_ack_does_not_commit():
    """Acks from a lost assignment must not commit offsets another member owns."""
    broker = KafkaBroker(url="kafka://localhost:9092")
    subscription, subscriber = _kafka_subscription()
    record = _kafka_record("t", 0, 5)
    subscription.track(record)

    KafkaRebalanceListener(subscription).on_partitions_revoked([TopicPartition("t", 0)])
    await broker.ack(record)

    subscriber.commit.assert_not_called()


@pytest.mark.anyio
async def test_kafka_assigned_partition_tracks_from_first_record():
    """Stale state must not hold the watermark below a newly assigned partition."""
    broker = KafkaBroker(url="kafka://localhost:9092")
    subscription, subscriber = _kafka_subscription()
    subscription.track(_kafka_record("t", 0, 5))

    KafkaRebalanceListener(subscription).on_partitions_assigned(
        [TopicPartition("t", 0)]
    )
    reassigned = _kafka_record("t", 0, 9)
    subscription.track(reassigned)
    await broker.ack(reassigned)

    subscriber.commit.assert_called_once_with({TopicPartition("t", 0): 10})


def test_kafka_partition_offsets_block_stops_advancing_watermark():
    offsets = PartitionOffsets(5)
    assert offsets.ack(5) == 6

    offsets.block()

    assert offsets.ack(6) is None
    assert offsets.next_offset == 6


def test_kafka_partition_offsets_block_drops_pending_acks():
    """A blocked watermark can never cross the gap, so pending acks are dead weight."""
    offsets = PartitionOffsets(5)
    assert offsets.ack(6) is None

    offsets.block()

    assert offsets.acked == set()


def test_kafka_partition_offsets_ignores_already_committed_ack():
    """A seek can replay offsets below the watermark; re-acking them is a no-op."""
    offsets = PartitionOffsets(5)
    assert offsets.ack(5) == 6

    assert offsets.ack(5) is None
    assert offsets.next_offset == 6


# ===========================================================================
# NatsBroker
# ===========================================================================


def _nats_broker() -> tuple[NatsBroker, AsyncMock]:
    broker = NatsBroker(url="nats://localhost:4222")
    client = AsyncMock()
    client.is_connected = False
    broker.client = client
    return broker, client


def test_nats_broker_topic_pattern():
    broker = NatsBroker(url="nats://localhost:4222")
    result = broker.format_topic("events.{param}.*")
    assert result == "events.*.>"


def test_nats_decode_message():
    msg = MagicMock()
    msg.data = b"data"
    msg.headers = {"h": "v"}
    data, headers = NatsBroker.decode_message(msg)
    assert data == b"data"
    assert headers == {"h": "v"}


def test_nats_get_message_metadata_present():
    msg = MagicMock()
    msg.metadata.sequence.consumer = 1
    msg.metadata.sequence.stream = 2
    msg.metadata.num_delivered = 3
    meta = NatsBroker.get_message_metadata(msg)
    assert meta["messaging.nats.num_delivered"] == "3"


def test_nats_get_message_metadata_missing():
    assert NatsBroker.get_message_metadata(MagicMock(spec=[])) == {}


def test_nats_is_connected():
    broker, client = _nats_broker()
    client.is_connected = True
    assert broker.is_connected


@pytest.mark.anyio
async def test_nats_connect_when_not_connected():
    broker, client = _nats_broker()
    client.is_connected = False
    await broker.connect()
    client.connect.assert_called_once()


@pytest.mark.anyio
async def test_nats_connect_skips_when_connected():
    broker, client = _nats_broker()
    client.is_connected = True
    await broker.connect()
    client.connect.assert_not_called()


@pytest.mark.anyio
async def test_nats_disconnect():
    broker, client = _nats_broker()
    client.is_connected = True
    await broker.disconnect()
    client.drain.assert_called_once()
    client.close.assert_called_once()


@pytest.mark.anyio
async def test_nats_disconnect_when_not_connected():
    broker, client = _nats_broker()
    client.is_connected = False
    await broker.disconnect()
    client.drain.assert_not_called()


@pytest.mark.anyio
async def test_nats_flush():
    broker, client = _nats_broker()
    await broker.flush()
    client.flush.assert_called_once()


@pytest.mark.anyio
async def test_nats_core_ack_is_noop():
    """Core NATS messages have no reply subject, so ack() must not touch them."""
    broker, _ = _nats_broker()
    msg = AsyncMock()
    await broker.ack(msg)
    msg.ack.assert_not_called()


@pytest.mark.anyio
async def test_nats_core_nack_is_noop():
    broker, _ = _nats_broker()
    msg = AsyncMock()
    await broker.nack(msg, delay=3)
    msg.nak.assert_not_called()


@pytest.mark.anyio
async def test_jetstream_ack_acknowledges_message():
    broker = JetStreamBroker(url="nats://localhost:4222")
    msg = AsyncMock()
    await broker.ack(msg)
    msg.ack.assert_called_once()


@pytest.mark.anyio
async def test_jetstream_nack_rejects_with_delay():
    broker = JetStreamBroker(url="nats://localhost:4222")
    msg = AsyncMock()
    await broker.nack(msg, delay=3)
    msg.nak.assert_called_once_with(delay=3)


@pytest.mark.anyio
async def test_nats_publish_auto_flush():
    broker, client = _nats_broker()
    broker._auto_flush = True
    await broker.publish("t", b"body", headers={}, reply="")
    client.publish.assert_called_once()
    client.flush.assert_called_once()


@pytest.mark.anyio
async def test_nats_publish_no_auto_flush():
    broker, client = _nats_broker()
    broker._auto_flush = False
    await broker.publish("t", b"body", headers={}, reply="")
    client.flush.assert_not_called()


@pytest.mark.anyio
async def test_nats_publish_manual_flush():
    broker, client = _nats_broker()
    broker._auto_flush = False
    await broker.publish("t", b"body", headers={}, reply="", flush=True)
    client.flush.assert_called_once()


@pytest.mark.anyio
async def test_nats_default_cb_without_error():
    broker, _ = _nats_broker()
    await broker._default_cb("closed")()


@pytest.mark.anyio
async def test_nats_default_cb_with_error():
    broker, _ = _nats_broker()
    await broker._default_cb("error")(error=ValueError("boom"))


@pytest.mark.anyio
async def test_nats_sender_delivers_message():
    broker, client = _nats_broker()
    msg = MagicMock()

    async def messages() -> Any:
        yield msg
        await anyio.sleep(10)

    sub = AsyncMock()
    sub.messages = messages()
    sub.unsubscribe = AsyncMock()
    client.subscribe = AsyncMock(return_value=sub)

    send, receive = anyio.create_memory_object_stream()
    async with anyio.create_task_group() as tg:
        tg.start_soon(broker.sender, "grp", _consumer(), send)
        received = await receive.receive()
        assert received is msg
        tg.cancel_scope.cancel()


def _blocking_nats_subscription() -> AsyncMock:
    async def blocking() -> Any:
        await anyio.sleep(10)
        return
        yield  # makes this an async generator

    sub = AsyncMock()
    sub.messages = blocking()
    sub.unsubscribe = AsyncMock()
    return sub


async def _run_nats_sender(consumer: MagicMock) -> AsyncMock:
    """Run `sender` until it blocks on the subscription, returning the client."""
    broker, client = _nats_broker()
    client.subscribe = AsyncMock(return_value=_blocking_nats_subscription())

    send, _ = anyio.create_memory_object_stream()
    async with anyio.create_task_group() as tg:
        tg.start_soon(broker.sender, "grp", consumer, send)
        await anyio.sleep(0.01)
        tg.cancel_scope.cancel()
    return client


@pytest.mark.anyio
async def test_nats_sender_leaves_pending_limits_at_nats_defaults():
    """Core NATS drops overflow instead of applying backpressure, so a lowered
    default would silently discard bursts."""
    client = await _run_nats_sender(_consumer(concurrency=4))

    kwargs = client.subscribe.call_args.kwargs
    assert "pending_msgs_limit" not in kwargs
    assert "pending_bytes_limit" not in kwargs


@pytest.mark.anyio
async def test_nats_sender_pending_limits_are_overridable():
    consumer = _consumer()
    consumer.options = {"pending_msgs_limit": 7, "pending_bytes_limit": 999}

    client = await _run_nats_sender(consumer)

    kwargs = client.subscribe.call_args.kwargs
    assert kwargs["pending_msgs_limit"] == 7
    assert kwargs["pending_bytes_limit"] == 999


@pytest.mark.anyio
async def test_nats_sender_forwards_only_the_overridden_pending_limit():
    consumer = _consumer()
    consumer.options = {"pending_msgs_limit": 7}

    client = await _run_nats_sender(consumer)

    kwargs = client.subscribe.call_args.kwargs
    assert kwargs["pending_msgs_limit"] == 7
    assert "pending_bytes_limit" not in kwargs


@pytest.mark.anyio
async def test_nats_sender_dynamic_unsubscribes():
    broker, client = _nats_broker()

    async def blocking() -> Any:
        await anyio.sleep(10)
        return
        yield  # makes this an async generator (unreachable but required for async gen type)

    sub = AsyncMock()
    sub.messages = blocking()
    sub.unsubscribe = AsyncMock()
    client.subscribe = AsyncMock(return_value=sub)

    send, _ = anyio.create_memory_object_stream()
    async with anyio.create_task_group() as tg:
        tg.start_soon(broker.sender, "grp", _consumer(dynamic=True), send)
        await anyio.sleep(0.01)
        tg.cancel_scope.cancel()

    sub.unsubscribe.assert_called_once()


# ===========================================================================
# JetStreamBroker
# ===========================================================================


def _js_broker() -> tuple[JetStreamBroker, AsyncMock, AsyncMock]:
    broker = JetStreamBroker(url="nats://localhost:4222")
    client = AsyncMock()
    client.is_connected = True
    broker.client = client
    js = AsyncMock()
    broker.js = js
    return broker, client, js


@pytest.mark.anyio
async def test_js_publish_adds_nats_msg_id():
    broker, _, js = _js_broker()
    broker._auto_flush = False
    await broker.publish("t", b"body", headers={}, message_id="my-id")
    assert js.publish.call_args.kwargs["headers"]["Nats-Msg-Id"] == "my-id"


@pytest.mark.anyio
async def test_js_publish_preserves_existing_nats_msg_id():
    broker, _, js = _js_broker()
    broker._auto_flush = False
    await broker.publish(
        "t", b"body", headers={"Nats-Msg-Id": "existing"}, message_id="new"
    )
    assert js.publish.call_args.kwargs["headers"]["Nats-Msg-Id"] == "existing"


@pytest.mark.anyio
async def test_js_publish_auto_flush():
    broker, client, _ = _js_broker()
    broker._auto_flush = True
    await broker.publish("t", b"body", headers={}, message_id="id")
    client.flush.assert_called_once()


def test_js_should_nack_within_limit():
    broker, _, _ = _js_broker()
    msg = MagicMock()
    msg.metadata.num_delivered = 2
    assert broker.should_nack(msg)


def test_js_should_nack_exceeds_limit():
    broker, _, _ = _js_broker()
    msg = MagicMock()
    msg.metadata.num_delivered = 4
    assert not broker.should_nack(msg)


def test_js_get_num_delivered():
    broker, _, _ = _js_broker()
    msg = MagicMock()
    msg.metadata.num_delivered = 7
    assert broker.get_num_delivered(msg) == 7


@pytest.mark.anyio
async def test_js_sender_fetches_and_delivers():
    broker, _, js = _js_broker()
    msg = MagicMock()
    call_count = 0

    async def fetch(**kwargs: Any) -> list:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return [msg]
        await anyio.sleep(10)
        return []

    sub = AsyncMock()
    sub.fetch = fetch
    sub.unsubscribe = AsyncMock()
    js.pull_subscribe = AsyncMock(return_value=sub)

    send, receive = anyio.create_memory_object_stream()
    async with anyio.create_task_group() as tg:
        tg.start_soon(broker.sender, "grp", _consumer(), send)
        received = await receive.receive()
        assert received is msg
        tg.cancel_scope.cancel()


@pytest.mark.anyio
async def test_js_sender_suppresses_timeout():
    from nats.errors import TimeoutError as NatsTimeout

    broker, _, js = _js_broker()
    call_count = 0

    async def fetch(**kwargs: Any) -> list:
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            raise NatsTimeout
        await anyio.sleep(10)
        return []

    sub = AsyncMock()
    sub.fetch = fetch
    sub.unsubscribe = AsyncMock()
    js.pull_subscribe = AsyncMock(return_value=sub)

    send, _ = anyio.create_memory_object_stream()
    async with anyio.create_task_group() as tg:
        tg.start_soon(broker.sender, "grp", _consumer(), send)
        await anyio.sleep(0.05)
        tg.cancel_scope.cancel()

    assert call_count >= 2


@pytest.mark.anyio
async def test_js_sender_full_buffer_skips_fetch():
    """concurrency=0 → batch==0 every iteration → asyncio.sleep branch, fetch not called."""
    broker, _, js = _js_broker()
    sub = AsyncMock()
    sub.fetch = AsyncMock(return_value=[])
    js.pull_subscribe = AsyncMock(return_value=sub)

    send, _ = anyio.create_memory_object_stream()
    async with anyio.create_task_group() as tg:
        tg.start_soon(broker.sender, "grp", _consumer(concurrency=0), send)
        await anyio.sleep(0.05)
        tg.cancel_scope.cancel()

    sub.fetch.assert_not_called()


@pytest.mark.anyio
async def test_js_sender_dynamic_unsubscribes():
    broker, _, js = _js_broker()

    async def blocking(**kwargs: Any) -> list:
        await anyio.sleep(10)
        return []

    sub = AsyncMock()
    sub.fetch = blocking
    sub.unsubscribe = AsyncMock()
    js.pull_subscribe = AsyncMock(return_value=sub)

    send, _ = anyio.create_memory_object_stream()
    async with anyio.create_task_group() as tg:
        tg.start_soon(broker.sender, "grp", _consumer(dynamic=True), send)
        await anyio.sleep(0.01)
        tg.cancel_scope.cancel()

    sub.unsubscribe.assert_called_once()


# ===========================================================================
# RabbitmqBroker
# ===========================================================================


def _rmq_broker() -> tuple[RabbitmqBroker, MagicMock, AsyncMock]:
    broker = RabbitmqBroker(url="amqp://localhost:5672")
    conn = MagicMock()
    conn.is_closed = False
    conn.close = AsyncMock()
    exchange = AsyncMock()
    exchange.publish = AsyncMock(return_value=None)
    broker._connection = conn
    broker._exchange = exchange
    return broker, conn, exchange


def test_rmq_connection_raises_when_none():
    with pytest.raises(BrokerConnectionError):
        _ = RabbitmqBroker(url="amqp://localhost:5672").connection


def test_rmq_exchange_raises_when_none():
    with pytest.raises(BrokerConnectionError):
        _ = RabbitmqBroker(url="amqp://localhost:5672").exchange


def test_rmq_is_connected():
    broker, conn, _ = _rmq_broker()
    conn.is_closed = False
    assert broker.is_connected


def test_rmq_is_not_connected_when_closed():
    broker, conn, _ = _rmq_broker()
    conn.is_closed = True
    assert not broker.is_connected


def test_rmq_should_nack_redelivered():
    broker, _, _ = _rmq_broker()
    msg = MagicMock()
    msg.redelivered = True
    assert broker.should_nack(msg)


def test_rmq_should_not_nack_first_delivery():
    broker, _, _ = _rmq_broker()
    msg = MagicMock()
    msg.redelivered = False
    assert not broker.should_nack(msg)


def test_rmq_decode_message():
    msg = MagicMock()
    msg.body = b"data"
    msg.headers = {"x-type": "MyEvent"}
    data, headers = RabbitmqBroker.decode_message(msg)
    assert data == b"data"
    assert headers == {"x-type": "MyEvent"}


def test_rmq_get_message_metadata():
    assert RabbitmqBroker.get_message_metadata(MagicMock()) == {}


def test_rmq_get_message_metadata_accepts_keyword():
    """The parameter name is part of the uniform backend surface."""
    assert RabbitmqBroker.get_message_metadata(raw_message=MagicMock()) == {}


@pytest.mark.anyio
async def test_rmq_connect():
    exchange = AsyncMock()
    channel = AsyncMock()
    channel.declare_exchange = AsyncMock(return_value=exchange)
    conn = AsyncMock()
    conn.channel = AsyncMock(return_value=channel)
    with patch(
        "eventiq.backends.rabbitmq.aio_pika.connect_robust",
        AsyncMock(return_value=conn),
    ):
        broker = RabbitmqBroker(url="amqp://localhost:5672")
        await broker.connect()
        assert broker._connection is conn
        channel.declare_exchange.assert_called_once()


@pytest.mark.anyio
async def test_rmq_connect_closes_connection_on_declare_failure():
    """A failed exchange declaration must leave no half-connected broker behind."""
    exchange = AsyncMock()
    failing_channel = AsyncMock()
    failing_channel.declare_exchange = AsyncMock(side_effect=RuntimeError("boom"))
    failing_conn = AsyncMock()
    failing_conn.channel = AsyncMock(return_value=failing_channel)
    channel = AsyncMock()
    channel.declare_exchange = AsyncMock(return_value=exchange)
    conn = AsyncMock()
    conn.is_closed = False
    conn.channel = AsyncMock(return_value=channel)
    with patch(
        "eventiq.backends.rabbitmq.aio_pika.connect_robust",
        AsyncMock(side_effect=[failing_conn, conn]),
    ):
        broker = RabbitmqBroker(url="amqp://localhost:5672")
        with pytest.raises(RuntimeError, match="boom"):
            await broker.connect()
        assert not broker.is_connected
        failing_conn.close.assert_called_once()
        with pytest.raises(BrokerConnectionError):
            _ = broker.exchange

        await broker.connect()

    assert broker.is_connected
    assert broker._exchange is exchange


@pytest.mark.anyio
async def test_rmq_connect_closes_connection_when_cancelled():
    """Cancellation during connect must not leak the robust connection."""
    closed = anyio.Event()

    async def _channel(*_: object, **__: object) -> Any:
        await anyio.sleep_forever()

    async def _close(*_: object, **__: object) -> None:
        await anyio.lowlevel.checkpoint()
        closed.set()

    conn = AsyncMock()
    conn.channel = _channel
    conn.close = _close
    with patch(
        "eventiq.backends.rabbitmq.aio_pika.connect_robust",
        AsyncMock(return_value=conn),
    ):
        broker = RabbitmqBroker(url="amqp://localhost:5672")
        with anyio.move_on_after(0.05) as scope:
            await broker.connect()

    assert scope.cancelled_caught
    assert closed.is_set()
    assert broker._connection is None


@pytest.mark.anyio
async def test_rmq_disconnect():
    broker, conn, _ = _rmq_broker()
    conn.close = AsyncMock()
    await broker.disconnect()
    conn.close.assert_called_once()


@pytest.mark.anyio
async def test_rmq_publish():
    broker, _, exchange = _rmq_broker()
    with patch("eventiq.backends.rabbitmq.aio_pika.Message", return_value=MagicMock()):
        await broker.publish(
            "test.topic",
            b"body",
            headers={"Content-Type": "application/json"},
            message_id="msg-1",
            message_type="TestEvent",
            message_content_type="application/json",
            message_time=datetime.now(timezone.utc),
            message_source="svc",
        )
    exchange.publish.assert_called_once()
    assert exchange.publish.call_args.kwargs["routing_key"] == "test.topic"


@pytest.mark.anyio
async def test_rmq_ack():
    broker, _, _ = _rmq_broker()
    msg = AsyncMock()
    await broker.ack(msg)
    msg.ack.assert_called_once()


@pytest.mark.anyio
async def test_rmq_nack_no_delay():
    broker, _, _ = _rmq_broker()
    msg = AsyncMock()
    await broker.nack(msg)
    msg.reject.assert_called_once_with(requeue=True)


@pytest.mark.anyio
async def test_rmq_nack_with_delay_warns():
    broker, _, _ = _rmq_broker()
    msg = AsyncMock()
    await broker.nack(msg, delay=5)
    msg.reject.assert_called_once_with(requeue=True)


@pytest.mark.anyio
async def test_rmq_sender_delivers_message():
    broker, conn, _exchange = _rmq_broker()
    msg = MagicMock()

    class QueueIter:
        _done: bool = False

        async def __aenter__(self) -> Self:
            return self

        async def __aexit__(self, *args: object) -> None:
            pass

        def __aiter__(self) -> Self:
            return self

        async def __anext__(self) -> Any:
            if not self._done:
                self._done = True
                return msg
            await anyio.sleep(10)
            raise StopAsyncIteration

    queue = AsyncMock()
    queue.bind = AsyncMock()
    queue.unbind = AsyncMock()
    queue.iterator = MagicMock(return_value=QueueIter())
    channel = AsyncMock()
    channel.set_qos = AsyncMock()
    channel.declare_queue = AsyncMock(return_value=queue)
    channel.close = AsyncMock()
    conn.channel = AsyncMock(return_value=channel)

    send, receive = anyio.create_memory_object_stream()
    async with anyio.create_task_group() as tg:
        tg.start_soon(broker.sender, "grp", _consumer(), send)
        received = await receive.receive()
        assert received is msg
        tg.cancel_scope.cancel()


@pytest.mark.anyio
async def test_rmq_sender_dynamic_unbinds():
    broker, conn, _exchange = _rmq_broker()

    class BlockingIter:
        async def __aenter__(self) -> Self:
            return self

        async def __aexit__(self, *args: object) -> None:
            pass

        def __aiter__(self) -> Self:
            return self

        async def __anext__(self) -> Any:
            await anyio.sleep(10)
            raise StopAsyncIteration

    queue = AsyncMock()
    queue.bind = AsyncMock()
    queue.unbind = AsyncMock()
    queue.iterator = MagicMock(return_value=BlockingIter())
    channel = AsyncMock()
    channel.set_qos = AsyncMock()
    channel.declare_queue = AsyncMock(return_value=queue)
    channel.close = AsyncMock()
    conn.channel = AsyncMock(return_value=channel)

    send, _ = anyio.create_memory_object_stream()
    async with anyio.create_task_group() as tg:
        tg.start_soon(broker.sender, "grp", _consumer(dynamic=True), send)
        await anyio.sleep(0.01)
        tg.cancel_scope.cancel()

    queue.unbind.assert_called_once()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("default_prefetch_count", "concurrency", "expected"),
    [(50, 1, 50), (10, 20, 40)],
)
async def test_rmq_sender_prefetch_count(
    default_prefetch_count: int, concurrency: int, expected: int
):
    """`default_prefetch_count` must be reachable, not shadowed by concurrency."""
    broker = RabbitmqBroker(
        url="amqp://localhost:5672",
        default_prefetch_count=default_prefetch_count,
    )
    conn = MagicMock()
    conn.is_closed = False
    broker._connection = conn
    broker._exchange = AsyncMock()

    class BlockingIter:
        async def __aenter__(self) -> Self:
            return self

        async def __aexit__(self, *args: object) -> None:
            pass

        def __aiter__(self) -> Self:
            return self

        async def __anext__(self) -> Any:
            await anyio.sleep(10)
            raise StopAsyncIteration

    queue = AsyncMock()
    queue.iterator = MagicMock(return_value=BlockingIter())
    channel = AsyncMock()
    channel.declare_queue = AsyncMock(return_value=queue)
    conn.channel = AsyncMock(return_value=channel)

    send, _ = anyio.create_memory_object_stream()
    async with anyio.create_task_group() as tg:
        tg.start_soon(broker.sender, "grp", _consumer(concurrency=concurrency), send)
        await anyio.sleep(0.01)
        tg.cancel_scope.cancel()

    channel.set_qos.assert_awaited_once_with(prefetch_count=expected)


# ===========================================================================
# RedisBroker
# ===========================================================================


def _redis_broker() -> tuple[RedisBroker, MagicMock]:
    broker: RedisBroker = RedisBroker(url="redis://localhost:6379")
    redis = MagicMock()
    redis.aclose = AsyncMock()
    redis.publish = AsyncMock()
    redis.connection = MagicMock()
    redis.connection.is_connected = True
    broker._redis = redis
    return broker, redis


def test_redis_raises_when_not_connected():
    with pytest.raises(BrokerConnectionError):
        _ = RedisBroker(url="redis://localhost:6379").redis


def test_redis_is_connected_true():
    broker, redis = _redis_broker()
    redis.connection.is_connected = True
    assert broker.is_connected


def test_redis_is_connected_before_connect():
    """The property must report a status, never raise, before connect()."""
    assert not RedisBroker(url="redis://localhost:6379").is_connected


def test_redis_is_connected_with_pooled_client():
    """A pooled client leaves `.connection` as None while still being usable."""
    broker, redis = _redis_broker()
    redis.connection = None
    assert broker.is_connected


def test_redis_decode_message():
    raw = {"type": b"message", "pattern": None, "channel": b"t", "data": b"payload"}
    data, headers = RedisBroker.decode_message(raw)
    assert data == b"payload"
    assert headers == {}


@pytest.mark.anyio
async def test_redis_connect():
    mock_redis = MagicMock()
    mock_redis.ping = AsyncMock(return_value=True)
    with patch("eventiq.backends.redis.Redis.from_url", return_value=mock_redis):
        broker = RedisBroker(url="redis://localhost:6379")
        await broker.connect()
        assert broker._redis is mock_redis
        mock_redis.ping.assert_awaited_once()


@pytest.mark.anyio
async def test_redis_connect_is_idempotent():
    """Replacing a live client would leak its connection pool's sockets."""
    mock_redis = MagicMock()
    mock_redis.ping = AsyncMock(return_value=True)
    with patch(
        "eventiq.backends.redis.Redis.from_url", return_value=mock_redis
    ) as from_url:
        broker = RedisBroker(url="redis://localhost:6379")
        await broker.connect()
        await broker.connect()

    from_url.assert_called_once()
    assert broker._redis is mock_redis


def test_redis_poll_timeout_from_settings(monkeypatch):
    """`BROKER_POLL_TIMEOUT` must reach the broker like every other setting."""
    monkeypatch.setenv("BROKER_URL", "redis://localhost:6379")
    monkeypatch.setenv("BROKER_POLL_TIMEOUT", "17")

    broker = RedisBroker.from_env()

    assert broker.poll_timeout == 17


def test_redis_poll_timeout_default():
    assert RedisBroker(url="redis://localhost:6379").poll_timeout == 2


@pytest.mark.anyio
async def test_redis_check_health_pings_server():
    broker, redis = _redis_broker()
    redis.ping = AsyncMock(return_value=True)

    assert await broker.check_health() is True
    redis.ping.assert_awaited_once()


@pytest.mark.anyio
async def test_redis_check_health_false_when_ping_fails():
    broker, redis = _redis_broker()
    redis.ping = AsyncMock(return_value=False)

    assert await broker.check_health() is False


@pytest.mark.anyio
async def test_redis_check_health_surfaces_dead_server():
    """`is_connected` keeps reporting True after the server dies."""
    broker, redis = _redis_broker()
    redis.ping = AsyncMock(side_effect=ConnectionError("server gone"))

    assert broker.is_connected
    with pytest.raises(ConnectionError):
        await broker.check_health()


@pytest.mark.anyio
async def test_redis_check_health_before_connect():
    assert await RedisBroker(url="redis://localhost:6379").check_health() is False


@pytest.mark.anyio
async def test_redis_disconnect():
    broker, redis = _redis_broker()
    await broker.disconnect()
    redis.aclose.assert_called_once()
    assert not broker.is_connected


@pytest.mark.anyio
async def test_redis_disconnect_without_connect_is_noop():
    await RedisBroker(url="redis://localhost:6379").disconnect()


@pytest.mark.anyio
async def test_redis_publish():
    broker, redis = _redis_broker()
    await broker.publish("chan", b"data")
    redis.publish.assert_called_once_with("chan", b"data")


@pytest.mark.anyio
async def test_redis_ack_is_noop():
    broker, _ = _redis_broker()
    await broker.ack(
        {"type": b"message", "pattern": None, "channel": b"t", "data": b"x"}
    )


@pytest.mark.anyio
async def test_redis_nack_republishes():
    broker, redis = _redis_broker()
    raw = {"type": b"message", "pattern": None, "channel": b"chan", "data": b"msg"}
    await broker.nack(raw)
    redis.publish.assert_called_once_with(b"chan", b"msg")


@pytest.mark.anyio
async def test_redis_nack_with_delay_warns():
    broker, redis = _redis_broker()
    raw = {"type": b"message", "pattern": None, "channel": b"c", "data": b"d"}
    await broker.nack(raw, delay=3)
    redis.publish.assert_called_once()


@pytest.mark.anyio
async def test_redis_sender_delivers_message():
    broker, redis = _redis_broker()
    msg = {"type": b"message", "pattern": None, "channel": b"t", "data": b"x"}
    call_count = 0

    async def get_message(**kwargs: Any) -> Any:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return msg
        await anyio.sleep(10)
        return None

    pubsub = AsyncMock()
    pubsub.psubscribe = AsyncMock()
    pubsub.get_message = get_message
    pubsub.__aenter__ = AsyncMock(return_value=pubsub)
    pubsub.__aexit__ = AsyncMock(return_value=False)
    redis.pubsub = MagicMock(return_value=pubsub)

    send, receive = anyio.create_memory_object_stream()
    async with anyio.create_task_group() as tg:
        tg.start_soon(broker.sender, "grp", _consumer("test.topic"), send)
        received = await receive.receive()
        assert received is msg
        tg.cancel_scope.cancel()


@pytest.mark.anyio
async def test_redis_sender_skips_none_messages():
    """get_message returning None → if message: branch not taken, loop continues."""
    broker, redis = _redis_broker()
    msg = {"type": b"message", "pattern": None, "channel": b"t", "data": b"x"}
    call_count = 0

    async def get_message(**kwargs: Any) -> Any:
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            await anyio.lowlevel.checkpoint()  # yield so other tasks can run
            return None
        if call_count == 3:
            return msg
        await anyio.sleep(10)
        return None

    pubsub = AsyncMock()
    pubsub.psubscribe = AsyncMock()
    pubsub.get_message = get_message
    pubsub.__aenter__ = AsyncMock(return_value=pubsub)
    pubsub.__aexit__ = AsyncMock(return_value=False)
    redis.pubsub = MagicMock(return_value=pubsub)

    send, receive = anyio.create_memory_object_stream()
    async with anyio.create_task_group() as tg:
        tg.start_soon(broker.sender, "grp", _consumer("test.topic"), send)
        received = await receive.receive()
        assert received is msg
        assert call_count >= 3
        tg.cancel_scope.cancel()
