from __future__ import annotations

from typing import TYPE_CHECKING

from .json import JsonDecoder, JsonEncoder

if TYPE_CHECKING:
    from eventiq.types import Decoder, Encoder

DEFAULT_ENCODER: Encoder = JsonEncoder(by_alias=True)
DEFAULT_DECODER: Decoder = JsonDecoder()
