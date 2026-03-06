from __future__ import annotations

from typing import TYPE_CHECKING, Any

import ormsgpack
from typing_extensions import Unpack

from eventiq.exceptions import DecodeError, EncodeError

if TYPE_CHECKING:
    from pydantic import BaseModel

    from eventiq.types import DecodeOptions, EncodeOptions, RawData, T


class MsgPackEncoder:
    CONTENT_TYPE = "application/x-msgpack"

    def __init__(
        self, option: int | None = None, **options: Unpack[EncodeOptions]
    ) -> None:
        self.option = option
        self.options = options

    def encode(self, data: BaseModel) -> bytes:
        try:
            return ormsgpack.packb(data.model_dump(**self.options), option=self.option)
        except ormsgpack.MsgpackEncodeError as e:
            raise EncodeError from e


class MsgPackDecoder:
    def __init__(
        self,
        *,
        option: int | None = None,
        from_attributes: bool | None = None,
        **options: Unpack[DecodeOptions],
    ) -> None:
        self.option = option
        self.from_attributes = from_attributes
        self.options = options

    def decode(self, data: RawData, as_type: type[T] | None = None) -> T | Any:
        try:
            if isinstance(data, str):
                data = data.encode("utf-8")
            unpacked = ormsgpack.unpackb(data, option=self.option)
            if as_type is None:
                return unpacked
            return as_type.model_validate(
                unpacked, from_attributes=self.from_attributes, **self.options
            )
        except Exception as e:
            raise DecodeError from e
