from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from datetime import timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Literal,
    Optional,
    Protocol,
    TypeVar,
    Union,
    overload,
    runtime_checkable,
)
from uuid import UUID

from pydantic import BaseModel, TypeAdapter
from typing_extensions import Concatenate, ParamSpec, TypedDict

if TYPE_CHECKING:
    from .consumer import GenericConsumer
    from .middleware import Middleware
    from .models import CloudEvent, Publishes
    from .service import Service


Undefinded: Any = object()

ID = Union[UUID, str]

Message = TypeVar("Message", bound=Any)
DefaultAction = Literal["ack", "nack"]
DecodedMessage = tuple[bytes, Optional[dict[str, str]]]

T = TypeVar("T", bound=BaseModel)
Seconds = Union[int, float]
Timeout = Union[Seconds, timedelta]
RawData = Union[str, bytes, bytearray]

CloudEventType = TypeVar("CloudEventType", bound="CloudEvent")
AnyType: TypeAdapter = TypeAdapter(Any)
State = dict[Union[type, str], Any]

PreparedMessage = tuple[str, bytes, dict[str, Any]]
Lifespan = Callable[["Service"], AbstractAsyncContextManager[Optional[State]]]


P = ParamSpec("P")

MessageHandler = Union[
    type["GenericConsumer"], Callable[Concatenate[CloudEventType, P], Any]
]


class MiddlewareType(Protocol[P]):
    def __call__(
        self, service: Service, *args: P.args, **kwargs: P.kwargs
    ) -> Middleware: ...


class Publisher(Protocol):
    async def __call__(
        self,
        message: CloudEvent,
        topic: str | None = None,
        headers: dict[str, str] | None = None,
        encoder: Encoder | None = None,
        **kwargs: Any,
    ) -> Any: ...


@runtime_checkable
class Encoder(Protocol):
    CONTENT_TYPE: str

    def encode(self, data: BaseModel) -> bytes: ...


@runtime_checkable
class Decoder(Protocol[T]):
    CONTENT_TYPE: str

    @overload
    def decode(self, data: RawData, as_type: type[T]) -> T: ...

    @overload
    def decode(self, data: RawData, as_type: None = None) -> Any: ...


class Parameter(TypedDict, total=False):
    enum: list[str]
    default: str
    description: str
    examples: list[str]
    location: str


class RetryStrategy(Protocol):
    def maybe_retry(
        self,
        service: Service,
        message: CloudEvent,
        exc: Exception,
    ) -> None: ...


class ConsumerGroupOptions(TypedDict, total=False):
    topic: str
    timeout: Timeout
    dynamic: bool
    tags: list[str]
    encoder: Encoder
    decoder: Decoder
    description: str
    concurrency: int
    retry_strategy: RetryStrategy
    publishes: list[Publishes]
    parameters: dict[str, Parameter]
    asyncapi_extra: dict[str, Any]
