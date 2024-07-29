from __future__ import annotations

from typing import Any, Literal, TypedDict

from pydantic import BaseModel
from pydantic.main import IncEx
from typing_extensions import Unpack

from .exceptions import EncodeError
from .types import Encoder


class Options(TypedDict, total=False):
    indent: int | None
    include: IncEx
    exclude: IncEx
    context: Any | None
    by_alias: bool
    exclude_unset: bool
    exclude_defaults: bool
    exclude_none: bool
    round_trip: bool
    warnings: bool | Literal["none", "warn", "error"]
    serialize_as_any: bool


class JsonEncoder:
    CONTENT_TYPE: str = "application/json"

    def __init__(self, **options: Unpack[Options]) -> None:
        self.options = options

    def encode(self, data: BaseModel) -> bytes:
        try:
            return data.model_dump_json(**self.options).encode("utf-8")
        except Exception as e:
            raise EncodeError from e


DEFAULT_ENCODER: Encoder = JsonEncoder(by_alias=True)
