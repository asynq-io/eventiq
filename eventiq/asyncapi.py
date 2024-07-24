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
from pydantic_asyncapi.common import Tag
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

from eventiq.models import Publishes
from eventiq.utils import TOPIC_PATTERN

TOPIC_TRANSLATION = str.maketrans({"{": "", "}": "", ".": "_", "*": "all"})
PREFIX = "#/components/schemas/"

if TYPE_CHECKING:
    from eventiq import CloudEvent, Service


def save_async_api_to_file(spec: BaseModel, path: Path, fmt: str) -> None:
    data = spec.model_dump(by_alias=True, exclude_none=True)

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
    topic: str, parameters: dict[str, str] | None = None
) -> dict[str, Parameter]:
    if parameters is None:
        parameters = {}
    params = {}
    for k in topic.split("."):
        if TOPIC_PATTERN.fullmatch(k):
            param_name = k[1:-1]
            params[param_name] = parameters.get(
                param_name, Parameter(description=param_name)
            )
    return params


def generate_receive_operation(consumer, broker_name, channels_params, spec, tags):
    event_type = consumer.event_type.__name__
    channel_id = generate_channel_id(consumer.topic)
    params = get_topic_parameters(consumer.topic, **(consumer.parameters or {}))
    for k, v in params.items():
        channels_params[channel_id].setdefault(k, v)
    message = {
        "name": event_type,
        "title": event_type,
        "description": consumer.event_type.__doc__,
        "contentType": consumer.event_type.get_default_content_type(),  # This is different per broker
        "payload": {"$ref": f"#/components/schemas/{event_type}"},
    }
    spec["components"]["messages"][event_type] = Message(**message)

    channel = {
        "address": consumer.topic,
        "servers": [{"$ref": f"#/servers/{broker_name}"}],
        "messages": {
            event_type: {"$ref": f"#/channels/{channel_id}/messages/{event_type}"}
        },
        "parameters": channels_params[channel_id],
        "tags": get_tag_list(tags, consumer.tags),
    }
    spec["channels"][channel_id] = Channel(**channel)

    operation_id = f"{to_camel(consumer.name)}Receive"
    operation = {
        "action": "receive",
        "title": f"{snake_case_to_title(consumer.name)} Receive",
        "summary": consumer.description,
        "channel": {"$ref": f"#/channels/{channel_id}"},
    }
    spec["operations"][operation_id] = Operation(**operation)


def generate_send_operation(
    publish_list: list[Publishes], broker_name, spec, channels_params, tags
):
    for publishes in publish_list:
        event_type = publishes.type.__name__
        params = get_topic_parameters(publishes.topic, publishes.parameters)
        channel_id = generate_channel_id(publishes.topic)
        for k, v in params.items():
            channels_params[channel_id].setdefault(k, v)

        operation = {
            "action": "send",
            "title": f"Send {event_type}",
            "messages": [{"$ref": f"#/channels/{channel_id}/messages/{event_type}"}],
            "channel": {"$ref": f"#/channels/{channel_id}"},
            "tags": get_tag_list(tags, publishes.tags),
            "summary": publishes.summary,
        }
        operation_id = f"send{event_type}"
        spec["operations"][operation_id] = Operation(**operation)

        if channel_id not in spec["channels"]:
            channel = {
                "address": publishes.topic,
                "servers": [{"$ref": f"#/servers/{broker_name}"}],
                "messages": {
                    event_type: {"$ref": f"#/components/messages/{event_type}"}
                },
                "parameters": channels_params[channel_id],
                "tags": get_tag_list(tags, publishes.tags),
                "summary": publishes.summary,
            }
            spec["channels"][channel_id] = Channel(**channel)


def generate_spec(service: Service) -> dict[str, Any]:
    spec = {
        "channels": {},
        "operations": {},
        "components": {"messages": {}},
    }
    channels_params: dict[str, dict[str, Parameter]] = defaultdict(dict)
    tags = {t["name"]: Tag.model_validate(t) for t in service.tags_metadata}

    for consumer in service.consumers.values():
        generate_send_operation(
            consumer.publishes, service.broker.name, spec, channels_params, tags
        )

    generate_send_operation(
        service.publishes, service.broker.name, spec, channels_params, tags
    )
    return spec


@functools.lru_cache
def get_async_api_spec(service: Service) -> AsyncAPI:
    schemas = get_all_models_schema(service)
    spec = generate_spec(service)
    components = Components(**{"schemas": schemas, **spec["components"]})
    return AsyncAPI(
        info=Info(
            **{
                "title": service.title,
                "version": service.version,
                **service.async_api_extra,
            }
        ),
        servers={
            service.broker.name: Server(
                title=service.broker.name.title(),
                protocol=service.broker.protocol,
                protocolVersion=service.broker.protocol_version,
                **service.broker.get_info(),
                **service.broker.async_api_extra,
            )
        },
        defaultContentType=service.broker.encoder.CONTENT_TYPE,
        channels=spec["channels"],
        operations=spec["operations"],
        components=components,
    )
