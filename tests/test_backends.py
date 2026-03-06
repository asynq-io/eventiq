"""Tests for broker base class, StubBroker, and all backend broker implementations."""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import anyio
import pytest
from typing_extensions import Self

from eventiq.backends.kafka import KafkaBroker
from eventiq.backends.nats import JetStreamBroker, NatsBroker
from eventiq.backends.rabbitmq import RabbitmqBroker
from eventiq.backends.redis import RedisBroker
from eventiq.backends.stub import StubBroker, StubMessage
from eventiq.broker import Broker, UrlBroker
from eventiq.exceptions import BrokerConnectionError, BrokerError
from eventiq.settings import UrlBrokerSettings


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


def test_kafka_is_connected():
    assert KafkaBroker(url="kafka://localhost:9092").is_connected


def test_kafka_should_nack_old_message():
    broker = KafkaBroker(url="kafka://localhost:9092")
    record = MagicMock()
    record.timestamp = 0
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
        kwargs = mock_producer.send.call_args.kwargs
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
        assert mock_producer.send.call_args.kwargs["timestamp_ms"] == 12345


@pytest.mark.anyio
async def test_kafka_ack_commits_offset():
    broker = KafkaBroker(url="kafka://localhost:9092")
    record = MagicMock()
    record.topic = "t"
    record.partition = 0
    record.offset = 5
    subscriber = AsyncMock()
    broker._subcsribers[id(record)] = subscriber
    await broker.ack(record)
    subscriber.commit.assert_called_once()


@pytest.mark.anyio
async def test_kafka_ack_no_subscriber():
    await KafkaBroker(url="kafka://localhost:9092").ack(MagicMock())


@pytest.mark.anyio
async def test_kafka_nack_removes_subscriber():
    broker = KafkaBroker(url="kafka://localhost:9092")
    record = MagicMock()
    broker._subcsribers[id(record)] = AsyncMock()
    await broker.nack(record)
    assert id(record) not in broker._subcsribers


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
async def test_nats_ack():
    broker, _ = _nats_broker()
    msg = AsyncMock()
    await broker.ack(msg)
    msg.ack.assert_called_once()


@pytest.mark.anyio
async def test_nats_nack():
    broker, _ = _nats_broker()
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


# ===========================================================================
# RedisBroker
# ===========================================================================


def _redis_broker() -> tuple[RedisBroker, MagicMock]:
    broker: RedisBroker = RedisBroker(url="redis://localhost:6379")
    redis = MagicMock()
    redis.close = AsyncMock()
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


def test_redis_is_connected_no_connection():
    broker, redis = _redis_broker()
    redis.connection = None
    assert not broker.is_connected


def test_redis_decode_message():
    raw = {"type": b"message", "pattern": None, "channel": b"t", "data": b"payload"}
    data, headers = RedisBroker.decode_message(raw)
    assert data == b"payload"
    assert headers is None


@pytest.mark.anyio
async def test_redis_connect():
    mock_redis = MagicMock()
    with patch("eventiq.backends.redis.Redis.from_url", return_value=mock_redis):
        broker = RedisBroker(url="redis://localhost:6379")
        await broker.connect()
        assert broker._redis is mock_redis


@pytest.mark.anyio
async def test_redis_disconnect():
    broker, redis = _redis_broker()
    await broker.disconnect()
    redis.close.assert_called_once()


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
