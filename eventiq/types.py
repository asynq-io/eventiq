from datetime import timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Protocol,
    TypeVar,
    Union,
    overload,
    runtime_checkable,
)
from uuid import UUID

from pydantic import BaseModel

if TYPE_CHECKING:
    from .models import CloudEvent as _CloudEvent

ID = Union[UUID, str]

Message = TypeVar("Message")

T = TypeVar("T", bound=BaseModel)
Seconds = Union[int, float]
Timeout = Union[Seconds, timedelta]
RawData = Union[str, bytes, bytearray]

CloudEvent = TypeVar("CloudEvent", bound="_CloudEvent")


@runtime_checkable
class Encoder(Protocol):
    CONTENT_TYPE: str

    def encode(self, data: BaseModel) -> bytes:
        ...


@runtime_checkable
class Decoder(Protocol[T]):
    @overload
    def decode(self, data: RawData, as_type: type[T]) -> T:
        ...

    @overload
    def decode(self, data: RawData, as_type: None = None) -> Any:
        ...
