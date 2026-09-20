from __future__ import annotations

import gzip

from eventiq.middleware import Middleware

GZIP_MAGIC = b"\x1f\x8b"


class GzipMiddleware(Middleware):
    """Compresses published payloads with gzip and decompresses incoming ones."""

    async def encode_payload(
        self,
        payload: bytes,
        headers: dict[str, str],
    ) -> tuple[bytes, dict[str, str]]:
        return gzip.compress(payload), {**headers, "Content-Encoding": "gzip"}

    async def decode_payload(
        self,
        payload: bytes,
        headers: dict[str, str],
    ) -> bytes:
        _ = headers
        # Headers are not a reliable signal: several brokers (redis pub/sub among
        # them) carry no headers at all, which would leave the payload compressed
        # and turn every message into a poison message. Detect the gzip member
        # header on the payload itself instead.
        if payload[:2] == GZIP_MAGIC:
            return gzip.decompress(payload)
        return payload
