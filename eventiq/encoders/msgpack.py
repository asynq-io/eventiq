from __future__ import annotations

from typing import TYPE_CHECKING, Any

import ormsgpack
from pydantic_core import to_json
from typing_extensions import Unpack

from eventiq.exceptions import DecodeError, EncodeError

if TYPE_CHECKING:
    from pydantic import BaseModel

    from eventiq.types import DecodeOptions, EncodeOptions, RawData, T


class MsgPackEncoder:
    """Encodes messages as MessagePack."""

    CONTENT_TYPE: str = "application/x-msgpack"

    def __init__(
        self, *, option: int | None = None, **options: Unpack[EncodeOptions]
    ) -> None:
        self.option = option
        # CloudEvents-spec keys on the wire, like the default `JsonEncoder`: field
        # names would emit `topic`/`content_type` instead of `subject`/`datacontenttype`.
        self.options: EncodeOptions = {"by_alias": True, **options}

    def encode(self, data: BaseModel) -> bytes:
        try:
            return ormsgpack.packb(
                data.model_dump(mode="json", **self.options), option=self.option
            )
        except Exception as e:
            raise EncodeError from e


class MsgPackDecoder:
    """Decodes MessagePack payloads into events."""

    CONTENT_TYPE: str = "application/x-msgpack"

    def __init__(
        self, *, option: int | None = None, **options: Unpack[DecodeOptions]
    ) -> None:
        self.option = option
        self.options = options

    def decode(self, data: RawData, as_type: type[T] | None = None) -> T | Any:
        try:
            if isinstance(data, str):
                data = data.encode("utf-8")
            unpacked = ormsgpack.unpackb(data, option=self.option)
            if as_type is None:
                return unpacked
            # `MsgPackEncoder` packs `mode="json"` output and msgpack has no native
            # UUID/datetime types, so validation must happen in JSON mode.
            return as_type.model_validate_json(to_json(unpacked), **self.options)
        except Exception as e:
            raise DecodeError from e
