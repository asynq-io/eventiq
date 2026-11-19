from typing import Any
from unittest.mock import AsyncMock, MagicMock

import anyio
import pytest
from nats.js.api import ConsumerConfig
from nats.js.errors import BadRequestError, NotFoundError

from eventiq import CloudEvent
from eventiq.backends.nats import JetStreamBroker, NatsBroker
from eventiq.consumer import FnConsumer


def test_nats_broker_topic_pattern() -> None:
    broker = NatsBroker(url="nats://localhost:4222")
    pattern = "events.{param}.*"
    result = broker.format_topic(pattern)
    assert result == "events.*.>"


@pytest.fixture
def jetstream_broker() -> JetStreamBroker:
    return JetStreamBroker(url="nats://localhost:4222")


def make_consumer(**options: Any) -> FnConsumer:
    async def handler(message: CloudEvent) -> None:
        pass

    return FnConsumer(fn=handler, event_type=CloudEvent, topic="test.topic", **options)


def make_message(reply: str) -> MagicMock:
    message = MagicMock()
    message.reply = reply
    message.is_acked = False

    async def settle(**_: Any) -> None:
        message.is_acked = True

    message.ack = AsyncMock(side_effect=settle)
    message.nak = AsyncMock(side_effect=settle)
    message.in_progress = AsyncMock()
    return message


@pytest.mark.parametrize(
    ("options", "expected"),
    [
        ({}, None),
        ({"ack_wait": 60}, 30.0),
        ({"ack_wait": 60, "in_progress_interval": 10}, 10.0),
        ({"ack_wait": 60, "in_progress_interval": None}, 30.0),
    ],
)
def test_in_progress_interval(options: dict[str, Any], expected: float | None) -> None:
    consumer = make_consumer(timeout=600, **options)
    assert JetStreamBroker._get_in_progress_interval(consumer, 600.0) == expected


@pytest.mark.parametrize("interval", [0, 60, 120])
def test_in_progress_interval_out_of_range(interval: float) -> None:
    consumer = make_consumer(timeout=600, ack_wait=60, in_progress_interval=interval)
    with pytest.raises(ValueError, match="in_progress_interval"):
        JetStreamBroker._get_in_progress_interval(consumer, 600.0)


def test_in_progress_interval_without_ack_wait() -> None:
    consumer = make_consumer(timeout=600, in_progress_interval=10)
    with pytest.raises(ValueError, match="requires ack_wait"):
        JetStreamBroker._get_in_progress_interval(consumer, 600.0)


@pytest.mark.parametrize("ack_wait", [600, 900])
def test_ack_wait_not_lower_than_timeout(ack_wait: float) -> None:
    consumer = make_consumer(timeout=600, ack_wait=ack_wait)
    with pytest.raises(ValueError, match="must be lower than timeout"):
        JetStreamBroker._get_in_progress_interval(consumer, 600.0)


async def test_ack_and_nack_release_held_messages(
    jetstream_broker: JetStreamBroker,
) -> None:
    acked, nacked, kept = make_message("a"), make_message("b"), make_message("c")
    held: dict[str, Any] = {}
    for message in (acked, nacked, kept):
        jetstream_broker._hold(held, message)

    await jetstream_broker.ack(acked)
    await jetstream_broker.nack(nacked, delay=5)

    acked.ack.assert_awaited_once()
    nacked.nak.assert_awaited_once_with(delay=5)
    assert held == {"c": kept}
    assert jetstream_broker._held_by_reply == {"c": held}


async def test_failed_ack_and_nack_release_held_messages(
    jetstream_broker: JetStreamBroker,
) -> None:
    acked, nacked = make_message("a"), make_message("b")
    acked.ack.side_effect = RuntimeError("connection lost")
    nacked.nak.side_effect = RuntimeError("connection lost")
    held: dict[str, Any] = {}
    jetstream_broker._hold(held, acked)
    jetstream_broker._hold(held, nacked)

    with pytest.raises(RuntimeError):
        await jetstream_broker.ack(acked)
    with pytest.raises(RuntimeError):
        await jetstream_broker.nack(nacked)

    assert held == {}
    assert jetstream_broker._held_by_reply == {}


async def test_ack_and_nack_skip_settled_message(
    jetstream_broker: JetStreamBroker,
) -> None:
    message = make_message("a")
    message.is_acked = True

    await jetstream_broker.ack(message)
    await jetstream_broker.nack(message)

    message.ack.assert_not_awaited()
    message.nak.assert_not_awaited()


async def test_on_receive_sends_in_progress(
    jetstream_broker: JetStreamBroker,
) -> None:
    pending, settled, failing = make_message("a"), make_message("b"), make_message("c")
    settled.is_acked = True
    failing.in_progress.side_effect = RuntimeError("connection lost")

    for message in (pending, settled, failing):
        await jetstream_broker.on_receive(message)

    pending.in_progress.assert_awaited_once()
    settled.in_progress.assert_not_awaited()
    failing.in_progress.assert_awaited_once()


async def test_send_in_progress_for_unsettled_messages(
    jetstream_broker: JetStreamBroker,
) -> None:
    pending, settled = make_message("a"), make_message("b")
    settled.is_acked = True

    with anyio.move_on_after(0.25):
        await jetstream_broker._send_in_progress(
            "consumer", {"a": pending, "b": settled}, 0.1
        )

    assert pending.in_progress.await_count == 2
    settled.in_progress.assert_not_awaited()


async def test_send_in_progress_survives_failure(
    jetstream_broker: JetStreamBroker,
) -> None:
    failing, pending = make_message("a"), make_message("b")
    failing.in_progress.side_effect = RuntimeError("connection lost")

    with anyio.move_on_after(0.15):
        await jetstream_broker._send_in_progress(
            "consumer", {"a": failing, "b": pending}, 0.1
        )

    pending.in_progress.assert_awaited_once()


async def test_nack_buffered_messages(jetstream_broker: JetStreamBroker) -> None:
    failing, buffered = make_message("a"), make_message("b")
    failing.nak.side_effect = RuntimeError("connection lost")
    held: dict[str, Any] = {}
    jetstream_broker._hold(held, failing)
    jetstream_broker._hold(held, buffered)

    await jetstream_broker._nack_buffered("consumer", [failing, buffered])

    buffered.nak.assert_awaited_once_with(delay=None)
    assert held == {}
    assert jetstream_broker._held_by_reply == {}


@pytest.fixture
def js(jetstream_broker: JetStreamBroker) -> MagicMock:
    js = MagicMock()
    js.consumer_info = AsyncMock()
    js.add_consumer = AsyncMock()
    jetstream_broker.js = js
    return js


async def test_sync_consumer_config_updates_changed_fields(
    jetstream_broker: JetStreamBroker, js: MagicMock
) -> None:
    current = ConsumerConfig(
        name="group:consumer",
        durable_name="group:consumer",
        filter_subject="test.topic",
        ack_wait=28830.0,
        max_ack_pending=10_000,
        max_deliver=5,
    )
    js.consumer_info.return_value = MagicMock(config=current)
    config = ConsumerConfig(
        name="group:consumer", ack_wait=60, max_ack_pending=10_000, max_deliver=6
    )

    await jetstream_broker._sync_consumer_config(
        stream="stream",
        durable="group:consumer",
        config=config,
        fields={"ack_wait", "max_ack_pending", "max_deliver"},
    )

    js.add_consumer.assert_awaited_once_with(
        "stream", current.evolve(ack_wait=60, max_deliver=6)
    )


async def test_sync_consumer_config_without_changes(
    jetstream_broker: JetStreamBroker, js: MagicMock
) -> None:
    js.consumer_info.return_value = MagicMock(
        config=ConsumerConfig(ack_wait=60.0, max_ack_pending=10_000)
    )

    await jetstream_broker._sync_consumer_config(
        stream="stream",
        durable="group:consumer",
        config=ConsumerConfig(ack_wait=60, max_ack_pending=10_000),
        fields={"ack_wait", "max_ack_pending"},
    )

    js.add_consumer.assert_not_awaited()


async def test_sync_consumer_config_new_consumer(
    jetstream_broker: JetStreamBroker, js: MagicMock
) -> None:
    js.consumer_info.side_effect = NotFoundError()

    await jetstream_broker._sync_consumer_config(
        stream="stream",
        durable="group:consumer",
        config=ConsumerConfig(ack_wait=60),
        fields={"ack_wait"},
    )

    js.add_consumer.assert_not_awaited()


async def test_sync_consumer_config_rejected_update(
    jetstream_broker: JetStreamBroker,
    js: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    js.consumer_info.return_value = MagicMock(config=ConsumerConfig(ack_wait=600.0))
    js.add_consumer.side_effect = BadRequestError()

    await jetstream_broker._sync_consumer_config(
        stream="stream",
        durable="group:consumer",
        config=ConsumerConfig(ack_wait=60),
        fields={"ack_wait"},
    )

    assert "Could not update consumer group:consumer" in caplog.text
