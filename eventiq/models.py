import contextlib
from datetime import datetime, timedelta
from typing import Any, ClassVar, Generic, TypeVar
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


class CloudEvent(BaseModel, Generic[D]):
    """Base Schema for all messages."""

    model_config = ConfigDict(
        use_enum_values=True,
        populate_by_name=True,
        extra="allow",
        arbitrary_types_allowed=True,
    )
    service: ClassVar[ServiceContext] = ServiceContext()

    specversion: str = Field("1.0", description="CloudEvents specification version")
    content_type: str | None = Field(
        None,
        alias="datacontenttype",
        description="Message content type",
    )
    id: UUID = Field(default_factory=uuid4, description="Event ID", repr=True)
    time: datetime = Field(default_factory=utc_now, description="Event created time")
    topic: str = Field(
        "",
        alias="subject",
        description="Message subject (topic)",
    )
    type: str = Field("", description="Event type")
    source: str | None = Field(None, description="Event source (app)")
    data: D = Field(description="Event payload")
    dataschema: str | None = Field(None, description="Data schema URI")

    _raw: Any | None = PrivateAttr(None)
    _headers: dict[str, str] = PrivateAttr({})

    def __init_subclass__(
        cls,
        *,
        abstract: bool = False,
        topic: str | None = None,
        validate_topic: bool = False,
        **kwargs: Any,
    ) -> None:
        if not abstract and topic:
            kw: _FieldInfoInputs = {
                "alias": "subject",
                "description": "Message subject",
                "validate_default": True,
            }
            if any(k in topic for k in TOPIC_SPECIAL_CHARS):
                kw.update(
                    {
                        "annotation": str,
                        "default": topic,
                    },
                )
                if validate_topic:
                    kw["pattern"] = get_topic_regex(topic)
            else:
                kw.update(
                    {
                        "annotation": get_annotation(topic),
                        "default": topic,
                    },
                )

            cls.model_fields["topic"] = FieldInfo(**kw)
        super().__init_subclass__(**kwargs)

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
        if self.source is None:
            with contextlib.suppress(AttributeError, RuntimeError):
                self.source = self.service.name
        return self

    @classmethod
    def get_default_topic(cls) -> str | None:
        return cls.model_fields["topic"].get_default()

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

    model_config = {
        "populate_by_name": True,
        "extra": "allow",
        "arbitrary_types_allowed": True,
    }


class Event(CloudEvent[D], abstract=True):
    pass


class Command(CloudEvent[D], abstract=True):
    pass


class Query(CloudEvent[D], abstract=True):
    pass
