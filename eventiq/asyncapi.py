from __future__ import annotations

import functools
import json
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel
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

from eventiq.consumer import Consumer
from eventiq.models import Publishes
from eventiq.types import Parameter as ParamDict
from eventiq.utils import TOPIC_PATTERN

TOPIC_TRANSLATION = str.maketrans({"{": "", "}": "", ".": "_", "*": "all"})
PREFIX = "#/components/schemas/"

if TYPE_CHECKING:
    from eventiq import Broker, CloudEvent, Service


def save_async_api_to_file(spec: BaseModel, path: Path, fmt: str) -> None:
    data = spec.model_dump(by_alias=True, exclude_none=True, exclude_unset=True)

    with open(path, "w") as f:
        if fmt == "yaml":
            import yaml

            yaml.dump(data, f)
        else:
            json.dump(data, f)


def get_all_models_schema(service: Service):
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


def get_tag_list(tags: dict[str, Tag], taggable: Iterable | None):
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
    topic: str, parameters: dict[str, ParamDict]
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
    broker: Broker,
    channels_params: dict[str, Any],
    spec: AsyncAPI,
    tags: dict[str, Tag],
):
    event_type: str = consumer.event_type.__name__
    channel_id = generate_channel_id(consumer.topic)
    params = get_topic_parameters(consumer.topic, consumer.parameters)
    for k, v in params.items():
        channels_params[channel_id].setdefault(k, v)
    message = Message(
        name=event_type,
        title=event_type,
        description=consumer.event_type.__doc__,
        contentType=broker.encoder.CONTENT_TYPE,
        payload=Reference(ref=f"#/components/schemas/{event_type}"),
        **consumer.asyncapi_extra.get("message", {}),
    )
    if spec.components is None:
        spec.components = Components()
    if spec.components.messages is None:
        spec.components.messages = {}
    spec.components.messages[event_type] = message

    channel = Channel(
        address=consumer.topic,
        servers=[Reference(ref=f"#/servers/{broker.name}")],
        messages={
            event_type: Reference(ref=f"#/channels/{channel_id}/messages/{event_type}")
        },
        parameters=channels_params[channel_id],
        tags=get_tag_list(tags, consumer.tags),
        **consumer.asyncapi_extra.get("channel", {}),
    )
    if spec.channels is None:
        spec.channels = {}
    spec.channels[channel_id] = channel

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


def generate_send_operation(
    publish_list: list[Publishes],
    broker: Broker,
    spec: AsyncAPI,
    channels_params: dict[str, dict[str, Any]],
    tags,
):
    for publishes in publish_list:
        event_type = publishes.type.__name__
        params = get_topic_parameters(publishes.topic, publishes.parameters)
        channel_id = generate_channel_id(publishes.topic)
        for k, v in params.items():
            channels_params[channel_id].setdefault(k, v)

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
        if channel_id not in spec.channels:
            channel = Channel(
                address=publishes.topic,
                servers=[Reference(ref=f"#/servers/{broker.name}")],
                messages={
                    event_type: Reference(ref=f"#/components/messages/{event_type}")
                },
                parameters=channels_params[channel_id],
                tags=get_tag_list(tags, publishes.tags),
                summary=publishes.summary,
                **publishes.asyncapi_extra.get("channel", {}),
            )
            spec.channels[channel_id] = channel


def populate_spec(service: Service, spec: AsyncAPI):
    tags = {t["name"]: Tag.model_validate(t) for t in service.tags_metadata}
    channels_params: dict[str, dict[str, Parameter]] = defaultdict(dict)
    for consumer in service.consumers.values():
        generate_send_operation(
            consumer.publishes, service.broker, spec, channels_params, tags
        )
        generate_receive_operation(
            consumer, service.broker, channels_params, spec, tags
        )

    generate_send_operation(
        service.publishes, service.broker, spec, channels_params, tags
    )
    return spec


@functools.lru_cache
def get_async_api_spec(service: Service) -> AsyncAPI:
    schemas = get_all_models_schema(service)
    spec = AsyncAPI(
        asyncapi="3.0.0",
        info=Info(
            title=service.title, version=service.version, **service.async_api_extra
        ),
        servers={
            service.broker.name: Server(
                protocol=service.broker.protocol,
                **service.broker.get_info(),
                **service.broker.async_api_extra,
            )
        },
        components=Components(schemas=schemas),
    )
    populate_spec(service, spec)

    return spec
