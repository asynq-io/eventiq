import inspect
from typing import Literal

import pytest
from pydantic import ValidationError

from eventiq import CloudEvent
from eventiq.context import (
    ServiceContext,
    reset_current_service,
    set_current_service,
)
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


def test_source_not_set_by_validation(service):
    """Validation must leave source alone: it also runs when decoding inbound messages."""
    set_current_service(service)
    try:
        ce2 = CloudEvent.new({"x": 1}, type="Test", topic="t")
        assert ce2.source is None
    finally:
        set_current_service(None)


def test_decoding_does_not_stamp_missing_source(service):
    """A foreign message without a source must not claim to originate here."""
    decoded = service.decoder.decode(
        b'{"data": {"x": 1}, "type": "Test", "subject": "t"}', CloudEvent
    )
    assert decoded.source is None


def test_decoding_keeps_foreign_source(service):
    decoded = service.decoder.decode(
        b'{"data": {"x": 1}, "type": "Test", "subject": "t", "source": "other_service"}',
        CloudEvent,
    )
    assert decoded.source == "other_service"


@pytest.mark.parametrize("ambient", [False, True])
def test_class_level_service_access_does_not_raise(service, ambient):
    """help(), inspect.getmembers and doc tooling read `service` off plain classes."""

    class ImportTimeEvent(CloudEvent[dict], topic="import.topic"): ...

    token = set_current_service(service if ambient else None)
    try:
        assert isinstance(ImportTimeEvent.service, ServiceContext)
        assert "service" in dict(inspect.getmembers(ImportTimeEvent))
    finally:
        reset_current_service(token)


def test_instance_service_access_resolves_ambient_service(service, ce):
    assert ce.service is service


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


def test_topic_not_leaked_between_siblings_of_same_parametrization():
    """Siblings share the cached `Event[dict]` base; `topic=` must not leak into it."""

    class SiblingWithTopic(Event[dict], topic="siblings.with_topic"): ...

    class SiblingWithoutTopic(Event[dict]): ...

    assert SiblingWithTopic.get_default_topic() == "siblings.with_topic"
    assert SiblingWithoutTopic.get_default_topic() == ""

    assert SiblingWithoutTopic(data={}, topic="anything.else").topic == "anything.else"

    with pytest.raises(ValidationError):
        SiblingWithTopic(data={}, topic="anything.else")


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


# --- explicit event type declaration ---


def test_type_class_kwarg_sets_default():
    class OrganizationCreatedEvent(
        CloudEvent[dict],
        topic="events.organization",
        type="events.organization.created",
    ):
        pass

    assert OrganizationCreatedEvent.get_default_type() == "events.organization.created"
    e = OrganizationCreatedEvent(data={})
    assert e.type == "events.organization.created"


def test_type_class_kwarg_rejects_other_value():
    class OrganizationCreatedEvent(
        CloudEvent[dict],
        topic="events.organization",
        type="events.organization.created",
    ):
        pass

    with pytest.raises(ValidationError):
        OrganizationCreatedEvent(data={}, type="events.organization.deleted")


def test_type_literal_annotation_sets_default():
    class OrganizationCreatedEvent(CloudEvent[dict], topic="events.organization"):
        type: Literal["events.organization.created"]

    assert OrganizationCreatedEvent.get_default_type() == "events.organization.created"
    e = OrganizationCreatedEvent(data={})
    assert e.type == "events.organization.created"
    assert e.model_dump()["type"] == "events.organization.created"

    with pytest.raises(ValidationError):
        OrganizationCreatedEvent(data={}, type="com.example.other")


def test_type_literal_annotation_inherited_from_abstract_base():
    class OrganizationEvent(CloudEvent[dict], abstract=True):
        type: Literal["events.organization.created"]

    class OrganizationCreatedEvent(OrganizationEvent, topic="events.organization"):
        pass

    assert OrganizationCreatedEvent(data={}).type == "events.organization.created"


def test_type_multi_value_literal_stays_required():
    class OrganizationEvent(CloudEvent[dict], topic="events.organization"):
        type: Literal["events.organization.created", "events.organization.deleted"]

    assert OrganizationEvent.get_default_type() is None
    with pytest.raises(ValidationError):
        OrganizationEvent(data={})
    assert (
        OrganizationEvent(data={}, type="events.organization.deleted").type
        == "events.organization.deleted"
    )


def test_type_not_leaked_between_siblings_of_same_parametrization():
    class SiblingWithType(Event[dict], topic="siblings.a", type="events.sibling.a"): ...

    class SiblingWithoutType(Event[dict], topic="siblings.b"): ...

    assert SiblingWithType.get_default_type() == "events.sibling.a"
    assert SiblingWithoutType.get_default_type() is None
    assert SiblingWithoutType(data={}).type == "SiblingWithoutType"


def test_default_type_of_plain_cloud_event():
    assert CloudEvent.get_default_type() is None


@pytest.mark.parametrize("value", [123, {"a": 1}])
def test_non_string_type_rejected(value):
    with pytest.raises(ValidationError, match="Type must be a non-empty string"):
        CloudEvent(data="x", topic="t", type=value)


def test_required_non_literal_type_stays_required():
    class OrganizationEvent(CloudEvent[dict], topic="events.organization"):
        type: str

    assert OrganizationEvent.get_default_type() is None
    with pytest.raises(ValidationError):
        OrganizationEvent(data={})


# --- field aliases ---


def test_topic_serialized_under_subject_alias():
    class AliasedEvent(CloudEvent[dict], topic="events.aliased"): ...

    e = AliasedEvent(data={})
    assert e.model_dump(by_alias=True)["subject"] == "events.aliased"
    assert e.model_dump()["topic"] == "events.aliased"


@pytest.mark.parametrize("key", ["topic", "subject"])
def test_topic_accepts_field_name_and_alias(key):
    class AliasedEvent(CloudEvent[dict], topic="events.aliased"): ...

    assert AliasedEvent.model_validate({key: "events.aliased", "data": {}}).topic == (
        "events.aliased"
    )


@pytest.mark.parametrize("key", ["content_type", "datacontenttype"])
def test_content_type_accepts_field_name_and_alias(key):
    ce = CloudEvent.model_validate(
        {"subject": "t", "data": {}, key: "application/json"}
    )
    assert ce.content_type == "application/json"
    assert ce.model_dump(by_alias=True)["datacontenttype"] == "application/json"
