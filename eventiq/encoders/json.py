from __future__ import annotations

from typing import TYPE_CHECKING, Any

from typing_extensions import Unpack

from eventiq.exceptions import DecodeError, EncodeError
from eventiq.types import AnyType

if TYPE_CHECKING:
    from pydantic import BaseModel

    from eventiq.types import DecodeOptions, EncodeOptions, RawData, T


class JsonEncoder:
    CONTENT_TYPE: str = "application/json"

    def __init__(
        self, *, indent: int | None = None, **options: Unpack[EncodeOptions]
    ) -> None:
        self.indent = indent
        self.options = options

    def encode(self, data: BaseModel) -> bytes:
        try:
            return data.model_dump_json(indent=self.indent, **self.options).encode(
                "utf-8"
            )
        except Exception as e:
            raise EncodeError from e


class JsonDecoder:
    CONTENT_TYPE: str = "application/json"

    def __init__(self, **options: Unpack[DecodeOptions]) -> None:
        self.options = options

    def decode(self, data: RawData, as_type: type[T] | None = None) -> T | Any:
        try:
            if as_type:
                return as_type.model_validate_json(data, **self.options)
            return AnyType.validate_json(data, **self.options)
        except Exception as e:
            raise DecodeError from e
