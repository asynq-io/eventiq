import contextlib
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Optional, TypeVar
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

from .types import Encoder, Parameter
from .utils import TOPIC_SPECIAL_CHARS, get_annotation, get_topic_regex, utc_now

if TYPE_CHECKING:
    from .service import Service

D = TypeVar("D", bound=Any)


class CloudEvent(BaseModel, Generic[D]):
    """Base Schema for all messages."""

    model_config = ConfigDict(
        use_enum_values=True,
        populate_by_name=True,
        extra="allow",
        arbitrary_types_allowed=True,
    )

    service: ClassVar["Service"]

    specversion: str = Field("1.0", description="CloudEvents specification version")
    content_type: Optional[str] = Field(
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
    source: Optional[str] = Field(None, description="Event source (app)")
    data: D = Field(description="Event payload")
    dataschema: Optional[str] = Field(None, description="Data schema URI")

    _raw: Optional[Any] = PrivateAttr(None)
    _headers: dict[str, str] = PrivateAttr({})

    def __init_subclass__(
        cls,
        abstract: bool = False,
        topic: Optional[str] = None,
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
            with contextlib.suppress(AttributeError):
                self.source = self.service.name
        return self

    @classmethod
    def get_default_topic(cls) -> Optional[str]:
        return cls.model_fields["topic"].get_default()

    @property
    def raw(self) -> Any:
        if self._raw is None:
            msg = "raw property accessible only for incoming messages"
            raise ValueError(msg)
        return self._raw

    def set_context(self, raw: Any, headers: dict[str, str]) -> None:
        self._raw = raw
        self._headers = headers

    def model_dump(self, by_alias: bool = True, **kwargs: Any) -> dict[str, Any]:
        return super().model_dump(by_alias=by_alias, **kwargs)

    @classmethod
    def new(
        cls, data: D, *, headers: Optional[dict[str, str]] = None, **kwargs: Any
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
        topic: Optional[str] = None,
        headers: Optional[dict[str, Any]] = None,
        encoder: Optional[Encoder] = None,
        **kwargs: Any,
    ) -> Any:
        return await self.service.publish(
            self, topic=topic, headers=headers, encoder=encoder, **kwargs
        )

    @classmethod
    async def create(
        cls,
        data: D,
        *,
        headers: Optional[dict[str, str]] = None,
        publish_options: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Self:
        self = cls.new(data, headers=headers, **kwargs)
        await cls.service.publish(self, **(publish_options or {}))
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
