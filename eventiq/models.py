from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Generic, Optional, TypeVar
from uuid import UUID, uuid4

from pydantic import (
    AnyUrl,
    BaseModel,
    Field,
    PrivateAttr,
    field_validator,
    model_validator,
)
from pydantic.fields import FieldInfo, _FieldInfoInputs
from pydantic_core.core_schema import ValidationInfo

from .types import Parameter
from .utils import TOPIC_SPECIAL_CHARS, get_annotation, get_topic_regex, utc_now

if TYPE_CHECKING:
    from .service import Service

D = TypeVar("D", bound=Any)


class CloudEvent(BaseModel, Generic[D]):
    """Base Schema for all messages"""

    specversion: str = Field("1.0", description="CloudEvents specification version")
    content_type: str = Field(
        "application/json", alias="datacontenttype", description="Message content type"
    )
    id: UUID = Field(default_factory=uuid4, description="Event ID", repr=True)
    time: datetime = Field(default_factory=utc_now, description="Event created time")
    topic: str = Field(
        None,
        alias="subject",
        description="Message subject (topic)",
        validate_default=True,
    )
    type: str = Field(None, description="Event type", validate_default=True)
    source: Optional[str] = Field(None, description="Event source (app)")
    data: D = Field(..., description="Event payload")
    dataschema: Optional[AnyUrl] = Field(None, description="Data schema URI")

    _raw: Optional[Any] = PrivateAttr(None)
    _headers: dict[str, str] = PrivateAttr({})
    _service: Optional["Service"] = PrivateAttr(None)

    def __init_subclass__(
        cls,
        abstract: bool = False,
        topic: Optional[str] = None,
        validate_topic: bool = False,
        **kwargs: Any,
    ):
        if not abstract:
            if topic:
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
                        }
                    )
                    if validate_topic:
                        kw["pattern"] = get_topic_regex(topic)
                else:
                    kw.update(
                        {
                            "annotation": get_annotation(topic),
                            "default": topic,
                        }
                    )

                cls.model_fields["topic"] = FieldInfo(**kw)
            super().__init_subclass__(**kwargs)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, CloudEvent):
            return False
        return self.id == other.id

    def __hash__(self) -> int:
        return hash(self.id)

    def __str__(self) -> str:
        return (
            f"{type(self).__name__}(type={self.type}, topic={self.topic}, id={self.id})"
        )

    def __repr__(self) -> str:
        return str(self)

    @classmethod
    def get_default_topic(cls) -> Optional[str]:
        return cls.model_fields["topic"].get_default()

    @field_validator("type", mode="before")
    @classmethod
    def get_default_type(cls, value, info: ValidationInfo):
        if value is None:
            return cls.__name__
        return value

    @property
    def raw(self) -> Any:
        if self._raw is None:
            raise ValueError("raw property accessible only for incoming messages")
        return self._raw

    @property
    def service(self) -> "Service":
        if self._service is None:
            raise ValueError("Service not set")
        return self._service

    def set_context(self, service: "Service", raw: Any) -> None:
        self._service = service
        self._raw = raw

    def model_dump(self, **kwargs: Any) -> dict[str, Any]:
        kwargs.setdefault("by_alias", True)
        kwargs.setdefault("exclude_none", True)
        return super().model_dump(**kwargs)

    @classmethod
    def new(cls, obj: D, *, headers: Optional[dict[str, str]] = None, **kwargs: Any):
        self = cls(data=obj, **kwargs)
        if headers:
            self._headers.update(headers)
        return self

    @property
    def extra_span_attributes(self) -> dict[str, str]:
        return {}

    @property
    def age(self) -> timedelta:
        return utc_now() - self.time

    @property
    def headers(self) -> dict[str, str]:
        if self._raw:
            return getattr(self.raw, "headers", {})
        return self._headers

    model_config = {
        "use_enum_values": True,
        "populate_by_name": True,
        "extra": "allow",
        "arbitrary_types_allowed": True,
    }


class Publishes(BaseModel):
    type: type[CloudEvent]
    topic: str = Field(None)
    parameters: dict[str, Parameter] = {}
    tags: list[str] = []
    summary: str = ""
    asyncapi_extra: dict[str, Any] = {}

    @model_validator(mode="after")
    def validate_topic(self):
        if self.topic is None:
            topic = self.type.get_default_topic()
            if topic is None:
                raise ValueError("Topic is required")
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
