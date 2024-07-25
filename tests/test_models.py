import pytest
from pydantic import ValidationError

from eventiq import CloudEvent


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
    "topic", ["some_random_string", "events.type.subtype", "events."]
)
def test_event_incorrect_topic(test_event_cls, topic):
    with pytest.raises(ValidationError):
        test_event_cls(topic=topic, data="test_data")


@pytest.mark.parametrize("topic", ["events.type", "events.type2"])
def test_correct_topic(test_event_cls, topic):
    test_event_cls(topic=topic, data="test_data")


@pytest.mark.parametrize(
    "topic", ["some_random_string", "events.type.subtype", "events."]
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
    assert u2.type == "UserUpdated"
    assert isinstance(u2, UserEvent)
    u3 = UserEvent.new(
        {"name": "John"}, type="UserDeleted", topic="events.users.deleted"
    )
    assert isinstance(u3, UserEvent)
    assert u3.type == "UserDeleted"
