from __future__ import annotations

import json
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Literal

from pydantic.alias_generators import to_camel
from pydantic.json_schema import models_json_schema
from pydantic_asyncapi.common import Reference, Tag
from pydantic_asyncapi.v3 import (
    AsyncAPI,
    Channel,
    Components,
    Info,
    Message,
    Operation,
    Parameter,
    Server,
)

from eventiq.utils import TOPIC_PATTERN

TOPIC_TRANSLATION = str.maketrans({"{": "", "}": "", ".": "_", "*": "all"})
PREFIX = "#/components/schemas/"

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

    from pydantic import BaseModel

    from eventiq import Broker, CloudEvent, Service
    from eventiq.consumer import Consumer
    from eventiq.models import Publishes
    from eventiq.types import Parameter as ParamDict


def save_async_api_to_file(
    spec: BaseModel, path: Path, fmt: Literal["json", "yaml"]
) -> None:
    """Write `spec` to `path`, as JSON or YAML."""
    if fmt not in ("json", "yaml"):
        msg = f"Unsupported format {fmt!r}, expected 'json' or 'yaml'"
        raise ValueError(msg)
    # Import before opening `path`: opening it for writing truncates it, so a
    # missing extra would destroy a previously generated spec.
    if fmt == "yaml":
        try:
            import yaml
        except ImportError as e:
            msg = (
                "YAML output requires PyYAML. Install it with "
                "'pip install eventiq[yaml]' or 'pip install pyyaml'."
            )
            raise ImportError(msg) from e

    # JSON mode: every model allows extra fields, so user-supplied `asyncapi_extra`
    # may hold values (datetimes, enums) neither json nor yaml can dump natively.
    data = spec.model_dump(
        mode="json", by_alias=True, exclude_none=True, exclude_unset=True
    )

    with open(path, "w") as f:
        if fmt == "yaml":
            yaml.dump(data, f)
        else:
            json.dump(data, f)


def get_all_models_schema(service: Service) -> Any:
    all_models: list[tuple[type[CloudEvent], Literal["validation"]]] = [
        (m.type, "validation") for m in service.publishes
    ]
    for consumer in service.consumers.values():
        all_models.extend([(m.type, "validation") for m in consumer.publishes])
        all_models.append((consumer.event_type, "validation"))

    _, top_level_schema = models_json_schema(
        all_models,
        ref_template=f"{PREFIX}{{model}}",
    )
    return top_level_schema.get("$defs", {})


def snake_case_to_title(name: str) -> str:
    return name.replace("_", " ").title()


def get_tag_list(tags: dict[str, Tag], taggable: Iterable | None) -> list[Tag]:
    tag_list = []
    for t in taggable or []:
        if t not in tags:
            tags[t] = Tag(name=t)
        tag_list.append(tags[t])
    return tag_list


def generate_channel_id(topic: str) -> str:
    topic_snake = topic.translate(TOPIC_TRANSLATION)
    return to_camel(topic_snake)


def get_topic_parameters(
    topic: str,
    parameters: dict[str, ParamDict],
) -> dict[str, Parameter]:
    result_params = {}
    for k in topic.split("."):
        if TOPIC_PATTERN.fullmatch(k):
            param_name = k[1:-1]
            param = parameters.get(param_name, {"description": param_name})
            result_params[param_name] = Parameter(**param)
    return result_params


def generate_receive_operation(
    consumer: Consumer,
    service: Service,
    channels_params: dict[str, Any],
    spec: AsyncAPI,
    tags: dict[str, Tag],
) -> None:
    event_type: str = consumer.event_type.__name__
    channel_id = generate_channel_id(consumer.topic)
    params = get_topic_parameters(consumer.topic, consumer.parameters)
    for k, v in params.items():
        channels_params[channel_id].setdefault(k, v)
    content_type = (
        consumer.decoder.CONTENT_TYPE
        if consumer.decoder
        else service.decoder.CONTENT_TYPE
    )
    message = Message(
        name=event_type,
        title=event_type,
        description=consumer.event_type.__doc__,
        contentType=content_type,
        payload=Reference(ref=f"#/components/schemas/{event_type}"),
        **consumer.asyncapi_extra.get("message", {}),
    )
    register_component_message(spec, event_type, message, overwrite=True)

    if spec.channels is None:
        spec.channels = {}
    existing = spec.channels.get(channel_id)
    if isinstance(existing, Channel):
        # A send operation may have created this channel already; keep its messages.
        add_channel_message(existing, event_type)
        existing.parameters = channels_params[channel_id]
        add_channel_tags(existing, get_tag_list(tags, consumer.tags))
        for key, value in consumer.asyncapi_extra.get("channel", {}).items():
            setattr(existing, key, value)
    else:
        spec.channels[channel_id] = Channel(
            address=consumer.topic,
            servers=[Reference(ref=f"#/servers/{service.broker.name}")],
            messages={
                event_type: Reference(ref=f"#/components/messages/{event_type}"),
            },
            parameters=channels_params[channel_id],
            tags=get_tag_list(tags, consumer.tags),
            **consumer.asyncapi_extra.get("channel", {}),
        )

    operation_id = f"{to_camel(consumer.name)}Receive"
    operation = Operation(
        action="receive",
        title=f"{snake_case_to_title(consumer.name)} Receive",
        summary=consumer.description,
        channel=Reference(ref=f"#/channels/{channel_id}"),
        **consumer.asyncapi_extra.get("operation", {}),
    )
    if spec.operations is None:
        spec.operations = {}
    spec.operations[operation_id] = operation


def register_component_message(
    spec: AsyncAPI, event_type: str, message: Message, *, overwrite: bool = False
) -> None:
    """Store a message under `#/components/messages` so refs to it resolve.

    Consumers describe a message more precisely than a publisher can (content type,
    docstring), so they overwrite the placeholder a send operation may have left.
    """
    if spec.components is None:
        spec.components = Components()
    if spec.components.messages is None:
        spec.components.messages = {}
    if overwrite:
        spec.components.messages[event_type] = message
    else:
        spec.components.messages.setdefault(event_type, message)


def add_channel_tags(channel: Channel, new_tags: list[Tag]) -> None:
    """Append tags to a channel, skipping names it already carries."""
    current = channel.tags or []
    known = {tag.name for tag in current}
    channel.tags = [*current, *(t for t in new_tags if t.name not in known)]


def add_channel_message(channel: Channel, event_type: str) -> None:
    """Point a channel at a component message without dropping existing ones."""
    if channel.messages is None:
        channel.messages = {}
    channel.messages.setdefault(
        event_type, Reference(ref=f"#/components/messages/{event_type}")
    )


def generate_send_operation(
    publish_list: list[Publishes],
    broker: Broker,
    spec: AsyncAPI,
    channels_params: dict[str, dict[str, Any]],
    tags: dict[str, Tag],
) -> None:
    for publishes in publish_list:
        event_type = publishes.type.__name__
        params = get_topic_parameters(publishes.topic, publishes.parameters)
        channel_id = generate_channel_id(publishes.topic)
        for k, v in params.items():
            channels_params[channel_id].setdefault(k, v)

        # A published-only event has no consumer to describe it, so register the
        # component message here; otherwise the channel ref would dangle.
        register_component_message(
            spec,
            event_type,
            Message(
                name=event_type,
                title=event_type,
                description=publishes.type.__doc__,
                payload=Reference(ref=f"#/components/schemas/{event_type}"),
                **publishes.asyncapi_extra.get("message", {}),
            ),
        )

        operation_id = f"send{event_type}"
        operation = Operation(
            action="send",
            title=f"Send {event_type}",
            messages=[Reference(ref=f"#/channels/{channel_id}/messages/{event_type}")],
            channel=Reference(ref=f"#/channels/{channel_id}"),
            tags=get_tag_list(tags, publishes.tags),
            summary=publishes.summary,
            **publishes.asyncapi_extra.get("operation", {}),
        )
        if spec.operations is None:
            spec.operations = {}
        spec.operations[operation_id] = operation
        if spec.channels is None:
            spec.channels = {}
        existing = spec.channels.get(channel_id)
        if isinstance(existing, Channel):
            add_channel_message(existing, event_type)
        else:
            spec.channels[channel_id] = Channel(
                address=publishes.topic,
                servers=[Reference(ref=f"#/servers/{broker.name}")],
                messages={
                    event_type: Reference(ref=f"#/components/messages/{event_type}"),
                },
                parameters=channels_params[channel_id],
                tags=get_tag_list(tags, publishes.tags),
                summary=publishes.summary,
                **publishes.asyncapi_extra.get("channel", {}),
            )


def populate_spec(service: Service, spec: AsyncAPI) -> None:
    tags = {t["name"]: Tag.model_validate(t) for t in service.tags_metadata}
    channels_params: dict[str, dict[str, Parameter]] = defaultdict(dict)
    for consumer in service.consumers.values():
        generate_send_operation(
            consumer.publishes,
            service.broker,
            spec,
            channels_params,
            tags,
        )
        generate_receive_operation(
            consumer,
            service,
            channels_params,
            spec,
            tags,
        )

    generate_send_operation(
        service.publishes,
        service.broker,
        spec,
        channels_params,
        tags,
    )


def get_async_api_spec(service: Service) -> AsyncAPI:
    """Build the AsyncAPI document describing `service`.

    Not cached: consumers may be registered after the first call, and caching would
    keep the service (and its broker) alive for the process lifetime.
    """
    schemas = get_all_models_schema(service)
    spec = AsyncAPI(
        asyncapi="3.0.0",
        info=Info(
            title=service.title,
            version=service.version,
            **service.async_api_extra,
        ),
        defaultContentType=service.encoder.CONTENT_TYPE,
        servers={
            service.broker.name: Server(
                protocol=service.broker.protocol,
                **service.broker.get_info(),
                **service.broker.async_api_extra,
            ),
        },
        components=Components(schemas=schemas),
    )
    populate_spec(service, spec)

    return spec
