import pytest
from pydantic import ValidationError

from eventiq import CloudEvent
from eventiq.models import Command, Event, Publishes, Query


@pytest.fixture
def test_event_cls():
    class TestEvent(CloudEvent[str], topic="events.{type}", validate_topic=True):
        pass

    return TestEvent


@pytest.fixture
def test_command_cls():
    class TestCommand(CloudEvent[str], topic="commands.command_a"):
        pass

    return TestCommand


def test_get_default_topic(test_event_cls):
    assert test_event_cls.get_default_topic() == "events.{type}"


@pytest.mark.parametrize(
    "topic",
    ["some_random_string", "events.type.subtype", "events."],
)
def test_event_incorrect_topic(test_event_cls, topic):
    with pytest.raises(ValidationError):
        test_event_cls(topic=topic, data="test_data")


@pytest.mark.parametrize("topic", ["events.type", "events.type2"])
def test_correct_topic(test_event_cls, topic):
    test_event_cls(topic=topic, data="test_data")


@pytest.mark.parametrize(
    "topic",
    ["some_random_string", "events.type.subtype", "events."],
)
def test_command_incorrect_topic(test_command_cls, topic):
    with pytest.raises(ValidationError):
        test_command_cls(topic=topic, data="test_data")


def test_command_correct_topic(test_command_cls):
    test_command_cls(data="test_data")
    test_command_cls(data="test_data", topic="commands.command_a")


def test_untyped_model():
    class UserEvent(CloudEvent): ...

    u1 = UserEvent.new(
        {"name": "John"}, type="UserCreated", topic="events.users.created"
    )
    assert isinstance(u1, UserEvent)
    assert u1.type == "UserCreated"
    u2 = UserEvent.new(
        {"name": "John"}, type="UserUpdated", topic="events.users.updated"
    )
    assert isinstance(u2, UserEvent)
    assert u2.type == "UserUpdated"
    u3 = UserEvent.new(
        {"name": "John"}, type="UserDeleted", topic="events.users.deleted"
    )
    assert isinstance(u3, UserEvent)
    assert u3.type == "UserDeleted"


def test_raw_raises_when_not_set():
    ce = CloudEvent.new({"x": 1}, type="Test", topic="t")
    with pytest.raises(ValueError, match="raw property accessible only for incoming"):
        _ = ce.raw


def test_raw_and_headers_after_set_raw():
    ce = CloudEvent.new({"x": 1}, type="Test", topic="t")
    ce.set_raw("raw_data", {"X-Header": "value"})
    assert ce.raw == "raw_data"
    assert ce.headers["X-Header"] == "value"


def test_age_is_nonnegative(ce):
    from datetime import timedelta

    assert isinstance(ce.age, timedelta)
    assert ce.age.total_seconds() >= 0


def test_equality_by_id(ce):
    ce2 = CloudEvent(id=ce.id, data={}, type="Test", topic="test_topic")
    assert ce == ce2


def test_inequality_different_id(ce):
    ce2 = CloudEvent.new({"x": 1}, type="Test", topic="t")
    assert ce != ce2
    assert ce != "not a cloud event"


def test_hash(ce):
    assert hash(ce) == hash(ce)


def test_requires_topic():
    with pytest.raises(ValidationError):
        CloudEvent(data="x", type="Test")


def test_source_set_from_service(service):
    from eventiq.context import set_current_service

    set_current_service(service)
    try:
        ce2 = CloudEvent.new({"x": 1}, type="Test", topic="t")
        assert ce2.source == service.name
    finally:
        set_current_service(None)


def test_abstract_subclasses():
    class MyEvent(Event[str], topic="my.topic"):
        pass

    class MyCommand(Command[dict], topic="my.command"):
        pass

    class MyQuery(Query[str], topic="my.query"):
        pass

    assert MyEvent(data="hello").topic == "my.topic"
    assert MyCommand(data={}).type == "MyCommand"
    assert MyQuery(data="?").topic == "my.query"


def test_publishes_requires_topic():
    with pytest.raises(ValidationError):
        Publishes(type=CloudEvent)


def test_publishes_inherits_topic_from_event():
    class MyEvent(CloudEvent[str], topic="my.topic"):
        pass

    p = Publishes(type=MyEvent)
    assert p.topic == "my.topic"


@pytest.mark.anyio
async def test_publish_instance_method(running_service, mock_consumer):
    ce = CloudEvent.new({"x": 1}, type="Test", topic="test_topic")
    await ce.publish()
    mock_consumer.assert_called_once_with(ce)


@pytest.mark.anyio
async def test_create_classmethod(running_service, mock_consumer):
    ce = await CloudEvent.create({"x": 1}, type="Test", topic="test_topic")
    assert isinstance(ce, CloudEvent)
    mock_consumer.assert_called_once_with(ce)


# --- CloudEvent __hash__ (line 98) ---


def test_cloud_event_hashable(ce):
    result = hash(ce)
    assert isinstance(result, int)
    s = {ce}
    assert ce in s


# --- validate_cloud_event sets topic from default (line 107) ---


def test_validate_cloud_event_uses_default_topic():
    """Passing topic='' triggers the default-topic assignment branch (line 107)."""

    class ParamEvent(CloudEvent[str], topic="events.{type}"):
        pass

    e = ParamEvent(data="hello", topic="")
    assert e.topic == "events.{type}"


# --- validate_cloud_event sets type from class name (line 109) ---


def test_validate_cloud_event_sets_type_from_class():
    class MyNamedEvent(CloudEvent[str], topic="events.named"):
        pass

    e = MyNamedEvent(data="x", type="")
    assert e.type == "MyNamedEvent"
