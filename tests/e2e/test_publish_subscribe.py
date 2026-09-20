"""Core publish/subscribe roundtrips against a real broker."""

from __future__ import annotations

from uuid import uuid4

import anyio
import pytest

from eventiq import CloudEvent

from .utils import make_event, receive, wait_for_subscription


class OrderCreated(CloudEvent[str]):
    """A typed event subclass used by the typed-event tests.

    Defined at module level so handler annotations resolve at runtime.
    """


@pytest.mark.anyio
async def test_publish_subscribe_roundtrip(broker, service, topic):
    received = []

    @service.subscribe(topic=topic)
    async def handler(message: CloudEvent) -> None:
        received.append(message)

    event = CloudEvent.new(
        {"n": 42, "name": "eventiq", "tags": ["a", "b"]},
        type="Roundtrip",
        topic=topic,
    )

    async with service.context():
        await wait_for_subscription(broker)
        await service.publish(event, topic=topic)
        messages = await receive(received)

    assert len(messages) == 1
    msg = messages[0]
    assert msg.id == event.id
    assert msg.type == "Roundtrip"
    assert msg.data == {"n": 42, "name": "eventiq", "tags": ["a", "b"]}
    assert msg.topic == topic
    assert isinstance(msg, CloudEvent)


@pytest.mark.anyio
async def test_typed_event_subclass(broker, service, topic):
    received = []

    @service.subscribe(event_type=OrderCreated, topic=topic)
    async def handler(message: OrderCreated) -> None:
        received.append(message)

    event = OrderCreated.new("sku-123", id=uuid4(), topic=topic)

    async with service.context():
        await wait_for_subscription(broker)
        await service.publish(event, topic=topic)
        messages = await receive(received)

    assert len(messages) == 1
    assert isinstance(messages[0], OrderCreated)
    assert messages[0].data == "sku-123"


@pytest.mark.anyio
async def test_multiple_consumers_on_same_topic(broker, service, topic):
    """Redis pub/sub broadcasts to every subscriber of the pattern."""
    received_a = []
    received_b = []

    @service.subscribe(topic=topic, name="consumer_a")
    async def handler_a(message: CloudEvent) -> None:
        received_a.append(message)

    @service.subscribe(topic=topic, name="consumer_b")
    async def handler_b(message: CloudEvent) -> None:
        received_b.append(message)

    async with service.context():
        await wait_for_subscription(broker)
        await service.publish(make_event(topic, n=1))
        await receive(received_a)
        await receive(received_b)

    assert len(received_a) == 1
    assert len(received_b) == 1


@pytest.mark.anyio
async def test_wildcard_topic(broker, service, topic):
    """A consumer subscribed to a wildcard receives matching concrete topics."""
    wildcard = f"{topic}.*"
    received = []

    @service.subscribe(topic=wildcard)
    async def handler(message: CloudEvent) -> None:
        received.append(message)

    async with service.context():
        await wait_for_subscription(broker)
        await service.publish(make_event(f"{topic}.a", n=1))
        await service.publish(make_event(f"{topic}.b", n=2))
        messages = await receive(received, count=2)

    assert [m.data["n"] for m in messages] == [1, 2]


@pytest.mark.anyio
async def test_non_matching_topic_not_delivered(broker, service, topic):
    """A consumer never sees a message published to a different topic."""
    received = []

    @service.subscribe(topic=topic)
    async def handler(message: CloudEvent) -> None:
        received.append(message)

    async with service.context():
        await wait_for_subscription(broker)
        await service.publish(make_event(f"{topic}.other", n=1))
        # Give a stray delivery a chance to arrive before asserting it did not.
        await anyio.sleep(1.0)
        assert received == []


@pytest.mark.anyio
async def test_send_uses_topic_and_sets_source(broker, service, topic):
    """``Service.send`` builds, publishes and sources the event for us."""
    received = []

    @service.subscribe(topic=topic)
    async def handler(message: CloudEvent) -> None:
        received.append(message)

    async with service.context():
        await wait_for_subscription(broker)
        await service.send({"n": 7}, type="SentEvent", topic=topic)
        messages = await receive(received)

    assert len(messages) == 1
    assert messages[0].type == "SentEvent"
    assert messages[0].data == {"n": 7}
    assert messages[0].source == service.name
