import json
import tempfile
from pathlib import Path

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
    get_async_api_spec.cache_clear()
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
    get_async_api_spec.cache_clear()
    schemas = get_all_models_schema(rich_service)
    assert isinstance(schemas, dict)


def test_spec_structure(rich_service):
    get_async_api_spec.cache_clear()
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
    get_async_api_spec.cache_clear()
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
    get_async_api_spec.cache_clear()
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
    get_async_api_spec.cache_clear()
    spec = get_async_api_spec(rich_service)
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        path = Path(f.name)
    save_async_api_to_file(spec, path, "json")
    data = json.loads(path.read_text())
    assert data["asyncapi"] == "3.0.0"
    path.unlink()


def test_save_async_api_to_file_yaml(rich_service):
    pytest.importorskip("yaml")
    get_async_api_spec.cache_clear()
    spec = get_async_api_spec(rich_service)
    with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
        path = Path(f.name)
    save_async_api_to_file(spec, path, "yaml")
    import yaml

    data = yaml.safe_load(path.read_text())
    assert data["asyncapi"] == "3.0.0"
    path.unlink()
