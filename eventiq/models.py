from datetime import datetime, timedelta
from typing import Any, ClassVar, Generic, Literal, TypeVar, get_args, get_origin
from uuid import UUID, uuid4

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    model_validator,
)
from pydantic.fields import FieldInfo, _FieldInfoInputs
from typing_extensions import Self

from .context import ServiceContext
from .types import Encoder, Parameter
from .utils import TOPIC_SPECIAL_CHARS, get_annotation, get_topic_regex, utc_now

D = TypeVar("D", bound=Any)


def _get_topic_field(topic: str, *, validate_topic: bool) -> FieldInfo:
    kw: _FieldInfoInputs = {
        "validation_alias": "subject",
        "serialization_alias": "subject",
        "description": "Message subject",
        "validate_default": True,
        "default": topic,
    }
    if any(k in topic for k in TOPIC_SPECIAL_CHARS):
        kw["annotation"] = str
        if validate_topic:
            kw["pattern"] = get_topic_regex(topic)
    else:
        kw["annotation"] = get_annotation(topic)
    return FieldInfo(**kw)


def _get_type_field(type: str) -> FieldInfo:
    kw: _FieldInfoInputs = {
        "annotation": get_annotation(type),
        "default": type,
        "description": "Event type",
        "validate_default": True,
    }
    return FieldInfo(**kw)


def _get_literal_value(annotation: Any) -> str | None:
    """Return the only allowed value of a single-valued `Literal[...]` annotation."""
    if get_origin(annotation) is Literal:
        args = get_args(annotation)
        if len(args) == 1 and isinstance(args[0], str):
            return args[0]
    return None


class CloudEvent(BaseModel, Generic[D]):
    """Base Schema for all messages."""

    model_config = ConfigDict(
        use_enum_values=True,
        populate_by_name=True,
        extra="allow",
        arbitrary_types_allowed=True,
    )

    service: ClassVar[ServiceContext] = ServiceContext()

    specversion: str = Field(
        default="1.0", description="CloudEvents specification version"
    )
    content_type: str | None = Field(
        default=None,
        validation_alias="datacontenttype",
        serialization_alias="datacontenttype",
        description="Message content type",
    )
    id: UUID = Field(default_factory=uuid4, description="Event ID", repr=True)
    time: datetime = Field(default_factory=utc_now, description="Event created time")
    topic: str = Field(
        default="",
        validation_alias="subject",
        serialization_alias="subject",
        description="Message subject (topic)",
    )
    # Annotated as `Any` so subclasses can narrow it to a `Literal`; the JSON schema
    # keeps declaring a string, as required by the CloudEvents spec.
    type: Any = Field(
        default="",
        description="Event type",
        json_schema_extra={"type": "string"},
    )
    source: str | None = Field(default=None, description="Event source (app)")
    data: D = Field(description="Event payload")
    dataschema: str | None = Field(default=None, description="Data schema URI")

    _raw: Any | None = PrivateAttr(None)
    _headers: dict[str, str] = PrivateAttr({})

    def __init_subclass__(
        cls,
        *,
        abstract: bool = False,
        topic: str | None = None,
        type: str | None = None,
        validate_topic: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init_subclass__(**kwargs)

    @classmethod
    def __pydantic_init_subclass__(
        cls,
        *,
        abstract: bool = False,
        topic: str | None = None,
        type: str | None = None,
        validate_topic: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        if abstract:
            return

        fields: dict[str, FieldInfo] = {}
        if topic:
            fields["topic"] = _get_topic_field(topic, validate_topic=validate_topic)
        if type:
            fields["type"] = _get_type_field(type)
        else:
            # `type: Literal["events.organization.created"]` declared in the class
            # body is a complete definition, so fill in the implied default.
            type_field = cls.model_fields["type"]
            if type_field.is_required():
                value = _get_literal_value(type_field.annotation)
                if value is not None:
                    fields["type"] = FieldInfo.merge_field_infos(
                        type_field,
                        default=value,
                    )

        if fields:
            cls.model_fields.update(fields)
            cls.model_rebuild(force=True)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CloudEvent):
            return False
        return self.id == other.id

    def __hash__(self) -> int:
        return hash(self.id)

    @model_validator(mode="after")
    def validate_cloud_event(self: Self) -> Self:
        if not self.topic:
            topic = self.get_default_topic()
            if not topic:
                msg = "Topic is required"
                raise ValueError(msg)
            self.topic = topic
        if not self.type:
            self.type = type(self).__name__
        elif not isinstance(self.type, str):
            msg = "Type must be a non-empty string"
            raise ValueError(msg)
        return self

    @classmethod
    def get_default_topic(cls) -> str | None:
        return cls.model_fields["topic"].get_default()

    @classmethod
    def get_default_type(cls) -> str | None:
        default = cls.model_fields["type"].get_default()
        return default if isinstance(default, str) and default else None

    @property
    def raw(self) -> Any:
        if self._raw is None:
            msg = "raw property accessible only for incoming messages"
            raise ValueError(msg)
        return self._raw

    def set_raw(self, raw: Any, headers: dict[str, str]) -> None:
        self._raw = raw
        self._headers = headers

    @classmethod
    def new(
        cls,
        data: D,
        *,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> Self:
        self = cls(data=data, **kwargs)
        if headers:
            self._headers.update(headers)
        return self

    @property
    def age(self) -> timedelta:
        return utc_now() - self.time

    @property
    def headers(self) -> dict[str, str]:
        return self._headers

    async def publish(
        self,
        topic: str | None = None,
        headers: dict[str, Any] | None = None,
        encoder: Encoder | None = None,
        **kwargs: Any,
    ) -> Any:
        return await self.service.publish(
            self,
            topic=topic,
            headers=headers,
            encoder=encoder,
            **kwargs,
        )

    @classmethod
    async def create(
        cls,
        data: D,
        *,
        headers: dict[str, str] | None = None,
        publish_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Self:
        self = cls.new(data, headers=headers, **kwargs)
        await self.publish(**(publish_options or {}))
        return self


class Publishes(BaseModel):
    type: type[CloudEvent]
    topic: str = ""
    parameters: dict[str, Parameter] = {}
    tags: list[str] = []
    summary: str = ""
    asyncapi_extra: dict[str, Any] = {}

    @model_validator(mode="after")
    def validate_topic(self) -> Self:
        topic = self.topic or self.type.get_default_topic()
        if not topic:
            msg = "Topic is required"
            raise ValueError(msg)
        self.topic = topic
        return self

    model_config = ConfigDict(
        use_enum_values=True,
        populate_by_name=True,
        extra="allow",
        arbitrary_types_allowed=True,
    )


class Event(CloudEvent[D], abstract=True):
    """
    Semantic convention for messages that represent events
    """


class Command(CloudEvent[D], abstract=True):
    """
    Semantic convention for messages that represent commands
    """


class Query(CloudEvent[D], abstract=True):
    """
    Semantic convention for messages that represent queries
    """
