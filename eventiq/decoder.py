from __future__ import annotations

from typing import Any

from pydantic import TypeAdapter

from .exceptions import DecodeError
from .types import Decoder, RawData, T

AnyType: TypeAdapter = TypeAdapter(Any)


class JsonDecoder:
    def __init__(self, strict: bool = False, context: Any = None) -> None:
        self.strict = strict
        self.context = context

    def decode(self, data: RawData, as_type: type[T] | None = None) -> T | Any:
        try:
            if as_type:
                return as_type.model_validate_json(
                    data, strict=self.strict, context=self.context
                )
            return AnyType.validate_json(data, strict=self.strict, context=self.context)
        except Exception as e:
            raise DecodeError from e


DEFAULT_DECODER: Decoder = JsonDecoder()
