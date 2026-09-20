import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import pytest

from eventiq import CloudEvent, Service
from eventiq.asyncapi import (
    generate_channel_id,
    generate_receive_operation,
    get_all_models_schema,
    get_async_api_spec,
    get_tag_list,
    get_topic_parameters,
    save_async_api_to_file,
    snake_case_to_title,
)
from eventiq.backends.stub import StubBroker
from eventiq.models import Publishes


@pytest.fixture
def rich_service():
    broker = StubBroker()
    svc = Service(
        name="test_asyncapi",
        broker=broker,
        version="1.0.0",
        tags_metadata=[{"name": "events", "description": "Event operations"}],
    )

    class UserCreated(CloudEvent[dict], topic="users.created"):
        """User was created."""

    class OrderPlaced(CloudEvent[dict], topic="orders.{order_id}.placed"):
        """Order was placed."""

    async def handle_user(message: UserCreated) -> None:
        pass

    async def handle_order(message: OrderPlaced) -> None:
        pass

    svc.subscribe(handle_user)
    svc.subscribe(handle_order, parameters={"order_id": {"description": "Order ID"}})
    svc.publishes.append(Publishes(type=UserCreated))
    return svc


def test_asyncapi_generation(service):
    spec = get_async_api_spec(service)
    assert spec.info.title == service.title


def test_generate_channel_id_simple():
    assert generate_channel_id("users.created") == "usersCreated"


def test_generate_channel_id_with_param():
    result = generate_channel_id("orders.{order_id}.placed")
    assert isinstance(result, str)
    assert len(result) > 0


def test_snake_case_to_title():
    assert snake_case_to_title("handle_user_event") == "Handle User Event"


def test_get_topic_parameters_with_param():
    params = get_topic_parameters("orders.{order_id}.placed", {})
    assert "order_id" in params


def test_get_topic_parameters_no_params():
    assert get_topic_parameters("users.created", {}) == {}


def test_get_topic_parameters_custom_description():
    params = get_topic_parameters(
        "events.{type}", {"type": {"description": "Event type"}}
    )
    assert params["type"].description == "Event type"


def test_get_all_models_schema(rich_service):
    schemas = get_all_models_schema(rich_service)
    assert isinstance(schemas, dict)


def test_spec_structure(rich_service):
    spec = get_async_api_spec(rich_service)
    assert spec.asyncapi == "3.0.0"
    assert spec.info.version == "1.0.0"
    assert spec.channels
    assert len(spec.channels) > 0
    assert spec.operations
    assert len(spec.operations) > 0
    assert rich_service.broker.name in spec.servers
    assert spec.components
    assert spec.components.schemas


def test_spec_receive_and_send_operations(rich_service):
    spec = get_async_api_spec(rich_service)
    actions = {op.action for op in spec.operations.values()}
    assert "receive" in actions
    assert "send" in actions


def test_get_tag_list_creates_tags():
    tags = {}
    result = get_tag_list(tags, ["events", "orders"])
    assert len(result) == 2
    assert tags["events"].name == "events"
    assert tags["orders"].name == "orders"


def test_get_tag_list_reuses_existing_tags():
    from pydantic_asyncapi.common import Tag

    existing = Tag(name="events")
    tags = {"events": existing}
    result = get_tag_list(tags, ["events"])
    assert result[0] is existing


def test_get_tag_list_empty():
    assert get_tag_list({}, None) == []


def test_generate_receive_operation_creates_components():
    """generate_receive_operation when spec.components is None → creates Components (line 123)."""
    from collections import defaultdict

    from pydantic_asyncapi.v3 import AsyncAPI, Info

    broker = StubBroker()
    svc = Service(name="comp_test", broker=broker)

    class MyEvent(CloudEvent[dict], topic="comp.topic"):
        pass

    async def handle_my(message: MyEvent) -> None:
        pass

    svc.subscribe(handle_my)
    consumer = svc.consumers["handle_my"]

    spec = AsyncAPI(
        asyncapi="3.0.0",
        info=Info(title="test", version="0.1.0"),
        components=None,
    )
    channels_params = defaultdict(dict)
    generate_receive_operation(consumer, svc, channels_params, spec, {})
    assert spec.components is not None
    assert spec.components.messages is not None


@pytest.fixture
def send_only_service():
    """Service that publishes (but does not subscribe to) a parameterized event."""
    broker = StubBroker()
    svc = Service(name="send_only", broker=broker)

    class ShippedEvent(CloudEvent[dict], topic="orders.{order_id}.shipped"):
        pass

    svc.publishes.append(Publishes(type=ShippedEvent))
    return svc


def test_generate_send_operation_new_channel(send_only_service):
    """generate_send_operation for a topic not yet in spec → creates channel (lines 167, 185-196)."""
    spec = get_async_api_spec(send_only_service)
    channel_ids = list(spec.channels.keys()) if spec.channels else []
    assert any(
        "shipped" in cid.lower() or "orders" in cid.lower() for cid in channel_ids
    )
    # send operations should be present
    actions = (
        {op.action for op in spec.operations.values()} if spec.operations else set()
    )
    assert "send" in actions


def test_save_async_api_to_file_json(rich_service):
    spec = get_async_api_spec(rich_service)
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        path = Path(f.name)
    save_async_api_to_file(spec, path, "json")
    data = json.loads(path.read_text())
    assert data["asyncapi"] == "3.0.0"
    path.unlink()


def test_save_async_api_to_file_yaml(rich_service):
    pytest.importorskip("yaml")
    spec = get_async_api_spec(rich_service)
    with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
        path = Path(f.name)
    save_async_api_to_file(spec, path, "yaml")
    import yaml

    data = yaml.safe_load(path.read_text())
    assert data["asyncapi"] == "3.0.0"
    path.unlink()


def test_save_async_api_to_file_serializes_extra_types(tmp_path):
    """`asyncapi_extra` is arbitrary user data that json/yaml cannot dump natively."""
    generated_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
    broker = StubBroker(asyncapi_extra={"x-generated-at": generated_at})
    svc = Service(name="extra_types", broker=broker, version="1.0.0")
    spec = get_async_api_spec(svc)

    json_path = tmp_path / "asyncapi.json"
    save_async_api_to_file(spec, json_path, "json")
    server = json.loads(json_path.read_text())["servers"][broker.name]
    assert server["x-generated-at"] == "2024-01-01T00:00:00Z"

    pytest.importorskip("yaml")
    import yaml

    yaml_path = tmp_path / "asyncapi.yaml"
    save_async_api_to_file(spec, yaml_path, "yaml")
    # `safe_load` rejects the `!!python/object` tags a python-mode dump emits
    loaded = yaml.safe_load(yaml_path.read_text())["servers"][broker.name]
    assert loaded["x-generated-at"] == "2024-01-01T00:00:00Z"


def test_save_async_api_to_file_missing_yaml_raises_import_error(
    rich_service, tmp_path
):
    """A missing extra must stay detectable with `except ImportError`."""
    spec = get_async_api_spec(rich_service)
    with (
        mock.patch.dict(sys.modules, {"yaml": None}),
        pytest.raises(ImportError, match="PyYAML"),
    ):
        save_async_api_to_file(spec, tmp_path / "asyncapi.yaml", "yaml")


def test_save_async_api_to_file_missing_yaml_keeps_existing_file(
    rich_service, tmp_path
):
    """A failed serialization must not destroy a previously generated spec."""
    spec = get_async_api_spec(rich_service)
    path = tmp_path / "asyncapi.yaml"
    path.write_text("PREVIOUS SPEC THAT MATTERS")

    with (
        mock.patch.dict(sys.modules, {"yaml": None}),
        pytest.raises(ImportError, match="PyYAML"),
    ):
        save_async_api_to_file(spec, path, "yaml")

    assert path.read_text() == "PREVIOUS SPEC THAT MATTERS"


def test_save_async_api_to_file_unsupported_format(rich_service, tmp_path):
    spec = get_async_api_spec(rich_service)
    with pytest.raises(ValueError, match="Unsupported format"):
        save_async_api_to_file(spec, tmp_path / "asyncapi.toml", "toml")


# --- reference integrity ---


def _iter_refs(node: object, acc: set[str]) -> set[str]:
    if isinstance(node, dict):
        for k, v in node.items():
            if k == "$ref" and isinstance(v, str):
                acc.add(v)
            else:
                _iter_refs(v, acc)
    elif isinstance(node, list):
        for item in node:
            _iter_refs(item, acc)
    return acc


def _resolve(doc: dict, ref: str) -> bool:
    node = doc
    for part in ref.lstrip("#/").split("/"):
        if not isinstance(node, dict) or part not in node:
            return False
        node = node[part]
    return True


def _spec_dict(svc: Service) -> dict:
    spec = get_async_api_spec(svc)
    return json.loads(spec.model_dump_json(by_alias=True, exclude_none=True))


def test_spec_has_no_dangling_references(rich_service):
    doc = _spec_dict(rich_service)
    unresolved = sorted(r for r in _iter_refs(doc, set()) if not _resolve(doc, r))
    assert unresolved == []


def test_channel_messages_are_not_self_referential(rich_service):
    """A channel message must point at the component message, not at itself."""
    doc = _spec_dict(rich_service)
    for channel_id, channel in (doc.get("channels") or {}).items():
        for message_id, message in (channel.get("messages") or {}).items():
            assert (
                message.get("$ref") != f"#/channels/{channel_id}/messages/{message_id}"
            )
            assert message.get("$ref") == f"#/components/messages/{message_id}"


def test_publish_only_event_gets_component_message():
    """An event that is published but never consumed still needs a message."""
    broker = StubBroker()

    class NotConsumed(CloudEvent[dict], topic="never.consumed"):
        """Published but never consumed."""

    svc = Service(
        name="publish_only",
        broker=broker,
        publishes=[Publishes(type=NotConsumed, topic="never.consumed")],
    )

    doc = _spec_dict(svc)

    assert "NotConsumed" in doc["components"]["messages"]
    unresolved = sorted(r for r in _iter_refs(doc, set()) if not _resolve(doc, r))
    assert unresolved == []


def _self_publishing_service(*, publishes_tags=None, consumer_tags=None) -> Service:
    """Consumer whose `publishes` targets the topic it subscribes to."""
    svc = Service(name="self_publish", broker=StubBroker())

    class OrderCreated(CloudEvent[dict], topic="orders"):
        """Order was created."""

    @svc.subscribe(
        topic="orders",
        publishes=[
            Publishes(type=OrderCreated, topic="orders", tags=publishes_tags or [])
        ],
        asyncapi_extra={"channel": {"title": "Orders Channel"}},
        tags=consumer_tags,
    )
    async def handle_order(message: OrderCreated) -> None:
        pass

    return svc


def test_self_publishing_consumer_keeps_channel_extra_and_tags():
    """The send operation must not shadow the consumer's channel extras and tags."""
    svc = _self_publishing_service(consumer_tags=["orders"])
    channel = _spec_dict(svc)["channels"][generate_channel_id("orders")]

    assert channel["title"] == "Orders Channel"
    assert [tag["name"] for tag in channel["tags"]] == ["orders"]


def test_self_publishing_consumer_does_not_duplicate_tags():
    svc = _self_publishing_service(
        publishes_tags=["orders"], consumer_tags=["orders", "internal"]
    )
    channel = _spec_dict(svc)["channels"][generate_channel_id("orders")]

    assert [tag["name"] for tag in channel["tags"]] == ["orders", "internal"]


def test_spec_reflects_consumers_registered_after_first_call(rich_service):
    """The spec must not be cached across consumer registration."""
    before = _spec_dict(rich_service)

    class LateEvent(CloudEvent[dict], topic="late.event"):
        """Registered after the first spec build."""

    async def handle_late(message: LateEvent) -> None:
        pass

    rich_service.subscribe(handle_late)
    after = _spec_dict(rich_service)

    assert "LateEvent" not in (before.get("components") or {}).get("messages", {})
    assert "LateEvent" in after["components"]["messages"]
