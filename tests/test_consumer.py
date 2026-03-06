import anyio
import pytest

from eventiq import CloudEvent, GenericConsumer
from eventiq.consumer import ChannelConsumer, ConsumerGroup, FnConsumer

RET_VAL = 42


@pytest.mark.anyio
async def test_consumer_process(test_consumer, ce):
    res = await test_consumer.process(ce)
    assert res == RET_VAL


@pytest.mark.anyio
async def test_generic_consumer_process(generic_test_consumer, ce):
    res = await generic_test_consumer.process(ce)
    assert res == RET_VAL


def test_subscribe_invalid_type(service):
    with pytest.raises(TypeError, match="Expected function or GenericConsumer"):
        service.subscribe("not_a_function", topic="test")


def test_add_consumer_group_merges_consumers():
    group_a = ConsumerGroup()
    group_b = ConsumerGroup()

    async def handler_a(message: CloudEvent) -> None:
        pass

    async def handler_b(message: CloudEvent) -> None:
        pass

    group_a.subscribe(handler_a, topic="topic.a")
    group_b.subscribe(handler_b, topic="topic.b")
    group_a.add_consumer_group(group_b)

    assert "handler_a" in group_a.consumers
    assert "handler_b" in group_a.consumers


@pytest.mark.anyio
async def test_fn_consumer_wraps_sync_function(ce):
    results = []

    def sync_handler(message: CloudEvent) -> None:
        results.append(message)

    consumer = FnConsumer(fn=sync_handler, event_type=CloudEvent, topic="test")
    await consumer.process(ce)
    assert results == [ce]


def test_generic_consumer_publish_outside_context():
    """Accessing .publish with no service in context raises RuntimeError."""
    from eventiq.context import set_current_service

    class MyConsumer(GenericConsumer[CloudEvent]):
        async def process(self, message: CloudEvent) -> None:
            pass

    consumer = MyConsumer(name="x", topic="t")
    set_current_service(None)
    with pytest.raises(RuntimeError):
        _ = consumer.publish


def test_consumer_topic_from_event_type():
    class MyEvent(CloudEvent[str], topic="my.topic"):
        pass

    consumer = FnConsumer(fn=lambda _msg: None, event_type=MyEvent, name="test")
    assert consumer.topic == "my.topic"


def test_consumer_group_options_propagate():
    group = ConsumerGroup(concurrency=5)

    async def handler(message: CloudEvent) -> None:
        pass

    group.subscribe(handler, topic="test")
    assert group.consumers["handler"].concurrency == 5


def test_consumer_requires_event_type():
    with pytest.raises(ValueError, match="Event type is required"):
        FnConsumer(fn=lambda _msg: None, event_type=None, name="x", topic="t")


def test_consumer_requires_topic():
    # CloudEvent has no default topic → ValueError
    with pytest.raises(ValueError, match="Topic is required"):
        FnConsumer(fn=lambda _msg: None, event_type=CloudEvent, name="x")


def test_consumer_concurrency_must_be_positive():
    with pytest.raises(ValueError, match="Concurrency must be greater than 0"):
        FnConsumer(
            fn=lambda _msg: None,
            event_type=CloudEvent,
            topic="t",
            concurrency=0,
            name="x",
        )


def test_generic_consumer_with_preset_attrs():
    class MyConsumer(GenericConsumer[CloudEvent]):
        async def process(self, message: CloudEvent) -> None:
            pass

    # Passing name/description explicitly covers the "already in extra" branches
    c = MyConsumer(name="preset_name", description="preset desc", topic="t")
    assert c.name == "preset_name"
    assert c.description == "preset desc"


def test_channel_consumer_auto_name():
    send_stream, _ = anyio.create_memory_object_stream(1)
    consumer = ChannelConsumer(channel=send_stream, event_type=CloudEvent, topic="t")
    assert consumer.dynamic is True
    assert len(consumer.name) > 0


@pytest.mark.anyio
async def test_channel_consumer_process(ce):
    send_stream, recv_stream = anyio.create_memory_object_stream(1)
    consumer = ChannelConsumer(channel=send_stream, event_type=CloudEvent, topic="t")

    async def ack_task() -> None:
        _, ack = await recv_stream.receive()
        ack()

    async with anyio.create_task_group() as tg:
        tg.start_soon(ack_task)
        await consumer.process(ce)


def test_fn_consumer_with_explicit_description():
    """Passing description= explicitly skips auto-description branch (line 111->113)."""
    consumer = FnConsumer(
        fn=lambda _msg: None,
        event_type=CloudEvent,
        topic="t",
        name="x",
        description="custom desc",
    )
    assert consumer.description == "custom desc"


def test_generic_consumer_with_explicit_event_type():
    """Passing event_type= explicitly skips auto-detection branch (line 131->133)."""

    class MyConsumer(GenericConsumer[CloudEvent]):
        async def process(self, message: CloudEvent) -> None:
            pass

    c = MyConsumer(event_type=CloudEvent, name="x", topic="t")
    assert c.event_type is CloudEvent


def test_generic_consumer_publish_in_context(service):
    """Accessing .publish when service is in context returns service.publish (line 144)."""

    @service.subscribe(topic="test_topic")
    class MyConsumer(GenericConsumer[CloudEvent]):
        name = "publish_in_ctx_consumer"

        async def process(self, message: CloudEvent) -> None:
            pass

    consumer = service.consumers["publish_in_ctx_consumer"]
    assert consumer.publish == service.publish


def test_channel_consumer_with_explicit_name():
    """Passing name= explicitly skips auto-name branch (line 153->156)."""
    send_stream, _ = anyio.create_memory_object_stream(1)
    consumer = ChannelConsumer(
        channel=send_stream, event_type=CloudEvent, topic="t", name="my-consumer"
    )
    assert consumer.name == "my-consumer"
