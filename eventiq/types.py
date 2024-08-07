from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from datetime import timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Protocol,
    TypeVar,
    Union,
    overload,
    runtime_checkable,
)
from uuid import UUID

from pydantic import BaseModel, TypeAdapter
from typing_extensions import ParamSpec, TypedDict

if TYPE_CHECKING:
    from .middleware import Middleware
    from .models import CloudEvent
    from .service import Service

ID = Union[UUID, str]

Message = TypeVar("Message")

T = TypeVar("T", bound=BaseModel)
Seconds = Union[int, float]
Timeout = Union[Seconds, timedelta]
RawData = Union[str, bytes, bytearray]

CloudEventType = TypeVar("CloudEventType", bound="CloudEvent")
AnyType: TypeAdapter = TypeAdapter(Any)
State = dict[Union[type, str], Any]


Lifespan = Callable[["Service"], AbstractAsyncContextManager[State | None]]

P = ParamSpec("P")


class MiddlewareType(Protocol[P]):
    def __call__(
        self, service: Service, *args: P.args, **kwargs: P.kwargs
    ) -> Middleware: ...


@runtime_checkable
class Encoder(Protocol):
    CONTENT_TYPE: str

    def encode(self, data: BaseModel) -> bytes: ...


@runtime_checkable
class Decoder(Protocol[T]):
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
