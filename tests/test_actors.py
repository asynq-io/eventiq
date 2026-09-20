import logging
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import anyio
import pytest
from pydantic import ValidationError

from eventiq import CloudEvent, Service
from eventiq.actors import Actor, ActorMessage, Actors, ActorsMiddleware
from eventiq.actors.actor import _AsyncActorResult
from eventiq.backends.stub import StubBroker
from eventiq.context import reset_current_service, set_current_service

# --- type parameter resolution ---


def test_actor_subclass_resolves_type_args():
    class Greeter(Actor[str, str]):
        async def process(self, message: ActorMessage[str]) -> str:
            return f"hello {message.data}"

    assert Greeter.response_validator.validate_python("x") == "x"
    assert Greeter.actor_event_type is ActorMessage[str]


def test_actor_requires_type_parameters():
    with pytest.raises(TypeError, match="must be parametrized"):

        class Bare(Actor):
            async def process(self, message: ActorMessage) -> None:
                return None


def test_actor_type_args_found_behind_mixin():
    class Mixin:
        pass

    class Mixed(Mixin, Actor[int, bool]):
        async def process(self, message: ActorMessage[int]) -> bool:
            return True

    expected = True
    assert Mixed.response_validator.validate_python(expected) is expected
    assert Mixed.actor_event_type is ActorMessage[int]


def test_actor_topic_includes_namespace():
    class Ns(Actor[int, int]):
        namespace = "billing"

        async def process(self, message: ActorMessage[int]) -> int:
            return message.data

    assert Ns._get_actor_topic() == "actors.billing.Ns"


def test_actor_instance_uses_actor_message_event_type():
    class Sized(Actor[int, int]):
        async def process(self, message: ActorMessage[int]) -> int:
            return message.data

    consumer = Sized()
    assert consumer.event_type is ActorMessage[int]
    assert consumer.topic == "actors.Sized"


# --- ActorsMiddleware must not touch non-actor traffic ---


def test_actors_middleware_requires_actor_message():
    assert ActorsMiddleware.requires is ActorMessage


@pytest.mark.anyio
async def test_actors_middleware_skips_plain_cloud_event():
    """A plain CloudEvent must not reach the actor hooks, which would AttributeError."""
    broker = StubBroker(wait_on_publish=False)
    svc = Service(name="actors_skip", broker=broker)
    token = set_current_service(svc)
    try:
        svc.add_middleware(ActorsMiddleware)
        event = CloudEvent.new({"a": 1}, topic="plain.topic")
        await svc.dispatch_before("publish", message=event)
        await svc.dispatch_after(
            "process_message", consumer=None, message=event, result=None, exc=None
        )
    finally:
        reset_current_service(token)


# --- result registry lifecycle ---


def test_set_message_result_drops_unknown_conversation():
    """Replies with no waiter are discarded rather than accumulating forever."""
    Actor._results.clear()
    Actor.set_message_result(uuid4(), "orphan reply")
    assert Actor._results == {}


@pytest.mark.anyio
async def test_ask_cleans_up_registry_on_timeout():
    class Silent(Actor[int, int]):
        async def process(self, message: ActorMessage[int]) -> int:
            return message.data

    broker = StubBroker(wait_on_publish=False)
    svc = Service(name="actors_timeout", broker=broker)
    token = set_current_service(svc)
    Actor._results.clear()
    try:
        await svc.connect()
        with pytest.raises(TimeoutError):
            await Silent.ask(1, timeout=0.05)
    finally:
        await svc.disconnect()
        reset_current_service(token)

    assert Actor._results == {}


@pytest.mark.anyio
async def test_none_result_is_returned_not_rejected():
    """A handler returning None resolves to None instead of raising."""
    result: _AsyncActorResult[None] = _AsyncActorResult()
    result.set_result(None)
    with anyio.fail_after(1):
        assert await result.get() is None


@pytest.mark.anyio
async def test_unset_result_raises():
    result: _AsyncActorResult[int] = _AsyncActorResult()
    result._event.set()
    with pytest.raises(ValueError, match="Result not set"), anyio.fail_after(1):
        await result.get()


# --- request/reply correlation ---


@pytest.mark.anyio
async def test_tell_returns_conversation_id_reply_is_keyed_by():
    broker = StubBroker(wait_on_publish=False)
    svc = Service(name="actors_correlate", broker=broker)
    token = set_current_service(svc)
    Actor._results.clear()
    conversation_id = None
    try:
        await svc.connect()

        class Echo(Actor[str, str]):
            async def process(self, message: ActorMessage[str]) -> str:
                return message.data

        conversation_id = await Echo.tell("ping")

        # Mirror what the reply consumer installed by ActorsMiddleware does: the
        # reply carries a fresh id but the original conversation_id.
        pending: _AsyncActorResult[str] = _AsyncActorResult()
        Actor._results[str(conversation_id)] = pending
        reply = ActorMessage.new(
            "pong",
            topic="actors.replies.x",
            kind="response",
            conversation_id=conversation_id,
        )
        assert reply.id != conversation_id
        Actor.set_message_result(reply.conversation_id, reply.data)

        with anyio.fail_after(1):
            assert await pending.get() == "pong"
    finally:
        if conversation_id is not None:
            Actor._results.pop(str(conversation_id), None)
        await svc.disconnect()
        reset_current_service(token)


@pytest.mark.anyio
async def test_ask_receives_reply_end_to_end():
    """ask() resolves once a reply with the matching conversation_id arrives."""
    broker = StubBroker(wait_on_publish=False)
    svc = Service(name="actors_e2e", broker=broker)
    token = set_current_service(svc)
    Actor._results.clear()
    try:
        await svc.connect()

        class Doubler(Actor[int, int]):
            async def process(self, message: ActorMessage[int]) -> int:
                return message.data * 2

        async def reply_when_registered() -> None:
            while not Actor._results:
                await anyio.lowlevel.checkpoint()
            conversation_id = next(iter(Actor._results))
            Actor.set_message_result(conversation_id, 42)

        async with anyio.create_task_group() as tg:
            tg.start_soon(reply_when_registered)
            with anyio.fail_after(2):
                assert await Doubler.ask(21) == 42
    finally:
        await svc.disconnect()
        reset_current_service(token)

    assert Actor._results == {}


# --- Actors consumer group ---


def _actor_service(
    name: str, namespace: str, actor_cls, published: list[str], **options: Any
) -> Service:
    broker = StubBroker(wait_on_publish=False)
    broker.publish = AsyncMock(  # type: ignore[method-assign]
        side_effect=lambda topic, *_args, **_kwargs: published.append(topic),
    )
    svc = Service(name=name, broker=broker, id_generator=lambda: "test-id")
    group = Actors(namespace=namespace)
    group.actor(actor_cls, **options)
    svc.add_consumer_group(group)
    return svc


@pytest.mark.anyio
async def test_same_actor_in_two_namespaces_keeps_topics_independent():
    """Registering one actor class twice must not re-route the first registration."""

    class Charge(Actor[int, bool]):
        async def process(self, message: ActorMessage[int]) -> bool:
            return True

    published: list[str] = []
    svc_a = _actor_service("actors_ns_a", "a", Charge, published)
    svc_b = _actor_service("actors_ns_b", "b", Charge, published)

    assert svc_a.consumers["Charge"].topic == "actors.a.Charge"
    assert svc_b.consumers["Charge"].topic == "actors.b.Charge"
    assert Charge.namespace is None

    for svc in (svc_a, svc_b):
        token = set_current_service(svc)
        try:
            await Charge.tell(1)
        finally:
            reset_current_service(token)

    assert published == ["actors.a.Charge", "actors.b.Charge"]


@pytest.mark.anyio
async def test_tell_accepts_explicit_namespace():
    """An explicit namespace addresses an actor hosted by another service."""

    class Remote(Actor[int, int]):
        async def process(self, message: ActorMessage[int]) -> int:
            return message.data

    published: list[str] = []
    svc = _actor_service("actors_remote", "local", Remote, published)
    token = set_current_service(svc)
    try:
        await Remote.tell(1, namespace="elsewhere")
    finally:
        reset_current_service(token)

    assert published == ["actors.elsewhere.Remote"]


@pytest.mark.anyio
async def test_tell_validates_data_against_declared_type():
    """Wrong data fails in the caller instead of poisoning the actor's queue."""

    class Adder(Actor[int, int]):
        async def process(self, message: ActorMessage[int]) -> int:
            return message.data + 1

    published: list[str] = []
    svc = _actor_service("actors_validate", "v", Adder, published)
    token = set_current_service(svc)
    try:
        with pytest.raises(ValidationError):
            await Adder.tell("not-an-int")
    finally:
        reset_current_service(token)

    assert published == []


@pytest.mark.anyio
async def test_ask_resolves_with_string_conversation_id():
    """A str conversation_id is normalised, so the UUID coming back still matches."""

    class Echo(Actor[str, str]):
        async def process(self, message: ActorMessage[str]) -> str:
            return message.data

    broker = StubBroker(wait_on_publish=False)
    svc = Service(name="actors_str_cid", broker=broker)
    token = set_current_service(svc)
    Actor._results.clear()
    conversation_id = str(uuid4())
    try:
        await svc.connect()

        async def reply_when_registered() -> None:
            while not Actor._results:
                await anyio.lowlevel.checkpoint()
            # The wire round-trip coerces the id back to UUID
            Actor.set_message_result(UUID(conversation_id), "pong")

        async with anyio.create_task_group() as tg:
            tg.start_soon(reply_when_registered)
            with anyio.fail_after(2):
                assert await Echo.ask("ping", conversation_id=conversation_id) == "pong"
    finally:
        await svc.disconnect()
        reset_current_service(token)

    assert Actor._results == {}


# --- ActorsMiddleware reply path ---


@pytest.mark.anyio
@pytest.mark.parametrize("string_conversation_id", [False, True])
async def test_ask_replies_end_to_end_via_middleware(string_conversation_id):
    """Full round trip: ask → actor consumer → reply topic → resolved result."""
    broker = StubBroker(wait_on_publish=False)
    svc = Service(name="actors_mw", broker=broker, id_generator=lambda: "test-id")
    svc.add_middleware(ActorsMiddleware)
    group = Actors(namespace="mw")

    @group.actor
    class Greeter(Actor[str, str]):
        async def process(self, message: ActorMessage[str]) -> str:
            return f"hello {message.data}"

    svc.add_consumer_group(group)
    Actor._results.clear()
    kwargs = {"conversation_id": str(uuid4())} if string_conversation_id else {}
    token = set_current_service(svc)
    try:
        await svc.connect()
        async with anyio.create_task_group() as tg:
            await svc.start_consumers(tg)
            # The stub broker only routes to topics an already running sender
            # subscribed to
            while len(broker.topics) < len(svc.consumers):
                await anyio.lowlevel.checkpoint()
            with anyio.fail_after(2):
                assert await Greeter.ask("world", **kwargs) == "hello world"
            tg.cancel_scope.cancel()
    finally:
        await svc.disconnect()
        reset_current_service(token)

    assert Actor._results == {}


@pytest.mark.anyio
async def test_actors_middleware_sets_reply_topic_only_for_queries():
    broker = StubBroker(wait_on_publish=False)
    svc = Service(name="actors_reply_to", broker=broker, id_generator=lambda: "test-id")
    token = set_current_service(svc)
    try:
        svc.add_middleware(ActorsMiddleware)
        query = ActorMessage.new(
            1, topic="actors.Q", kind="query", conversation_id=uuid4()
        )
        command = ActorMessage.new(
            1, topic="actors.C", kind="command", conversation_id=uuid4()
        )
        await svc.dispatch_before("publish", message=query)
        await svc.dispatch_before("publish", message=command)
    finally:
        reset_current_service(token)

    assert query.reply_to == "actors.replies.actors_reply_to.test-id"
    assert command.reply_to is None


@pytest.mark.anyio
async def test_actors_middleware_does_not_reply_on_error():
    """A failed handler must not publish a reply the caller would take as a result."""
    broker = StubBroker(wait_on_publish=False)
    published: list[str] = []
    broker.publish = AsyncMock(
        side_effect=lambda topic, *_args, **_kwargs: published.append(topic),
    )
    svc = Service(name="actors_no_reply", broker=broker, id_generator=lambda: "test-id")
    token = set_current_service(svc)
    try:
        svc.add_middleware(ActorsMiddleware)
        message = ActorMessage.new(
            1,
            topic="actors.X",
            kind="query",
            conversation_id=uuid4(),
            reply_to="actors.replies.x",
        )
        await svc.dispatch_after(
            "process_message",
            consumer=MagicMock(options={}),
            message=message,
            result=1,
            exc=ValueError("boom"),
        )
    finally:
        reset_current_service(token)

    assert published == []


@pytest.mark.anyio
async def test_actors_middleware_logs_handler_failure_with_conversation_id(caplog):
    """The silently dropped reply must leave a correlatable trace for the operator."""
    broker = StubBroker(wait_on_publish=False)
    svc = Service(
        name="actors_log_error", broker=broker, id_generator=lambda: "test-id"
    )
    conversation_id = uuid4()
    consumer = MagicMock(options={})
    consumer.name = "Boom"
    token = set_current_service(svc)
    try:
        svc.add_middleware(ActorsMiddleware)
        message = ActorMessage.new(
            1,
            topic="actors.X",
            kind="query",
            conversation_id=conversation_id,
            reply_to="actors.replies.x",
        )
        with caplog.at_level(logging.ERROR):
            await svc.dispatch_after(
                "process_message",
                consumer=consumer,
                message=message,
                result=None,
                exc=ValueError("boom"),
            )
    finally:
        reset_current_service(token)

    record = next(r for r in caplog.records if "will time out" in r.message)
    assert record.consumer_name == "Boom"
    assert record.conversation_id == str(conversation_id)
    assert "boom" in caplog.text


@pytest.mark.anyio
async def test_ask_times_out_when_handler_raises(caplog):
    """A failing actor sends no reply, so ask fails with TimeoutError, not the error."""
    # default_on_exc="ack" keeps the failure to a single delivery instead of letting
    # the stub broker redeliver it for the rest of the test.
    broker = StubBroker(wait_on_publish=False, default_on_exc="ack")
    svc = Service(name="actors_boom", broker=broker, id_generator=lambda: "test-id")
    svc.add_middleware(ActorsMiddleware)
    group = Actors(namespace="boom")

    @group.actor
    class Boom(Actor[str, str]):
        async def process(self, message: ActorMessage[str]) -> str:
            msg = "actor exploded"
            raise RuntimeError(msg)

    svc.add_consumer_group(group)
    Actor._results.clear()
    token = set_current_service(svc)
    try:
        await svc.connect()
        async with anyio.create_task_group() as tg:
            await svc.start_consumers(tg)
            while len(broker.topics) < len(svc.consumers):
                await anyio.lowlevel.checkpoint()
            with caplog.at_level(logging.ERROR), pytest.raises(TimeoutError):
                await Boom.ask("world", timeout=0.5)
            tg.cancel_scope.cancel()
    finally:
        await svc.disconnect()
        reset_current_service(token)

    assert Actor._results == {}
    assert "actor exploded" in caplog.text


@pytest.mark.anyio
async def test_actor_explicit_topic_is_not_overwritten():
    """An explicit topic wins over the derived one, so legacy topics stay reachable."""

    class Legacy(Actor[int, int]):
        async def process(self, message: ActorMessage[int]) -> int:
            return message.data

    published: list[str] = []
    svc = _actor_service(
        "actors_legacy_topic",
        "payments",
        Legacy,
        published,
        topic="legacy.actors.Legacy",
    )

    assert svc.consumers["Legacy"].topic == "legacy.actors.Legacy"

    token = set_current_service(svc)
    try:
        await Legacy.tell(1)
    finally:
        reset_current_service(token)

    assert published == ["legacy.actors.Legacy"]


def test_actor_explicit_name_is_not_overwritten():
    """An explicit name wins over the class name, without affecting the topic."""

    class Renamed(Actor[int, int]):
        async def process(self, message: ActorMessage[int]) -> int:
            return message.data

    group = Actors(namespace="payments")
    group.actor(Renamed, name="legacy-name")

    assert "Renamed" not in group.consumers
    assert group.consumers["legacy-name"].topic == "actors.payments.Renamed"


def test_actors_group_applies_namespace_to_actor_class():
    broker = StubBroker(wait_on_publish=False)
    svc = Service(name="actors_group", broker=broker)
    token = set_current_service(svc)
    try:
        group = Actors(namespace="payments")

        @group.actor
        class Charge(Actor[int, bool]):
            async def process(self, message: ActorMessage[int]) -> bool:
                return True

        svc.add_consumer_group(group)
        consumer = next(iter(group.consumers.values()))
        assert consumer.topic == "actors.payments.Charge"
    finally:
        reset_current_service(token)
