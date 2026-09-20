from datetime import datetime, timedelta
from decimal import Decimal
from ipaddress import IPv4Address
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from eventiq import CloudEvent
from eventiq.encoders import DEFAULT_DECODER, DEFAULT_ENCODER
from eventiq.encoders.json import JsonDecoder, JsonEncoder
from eventiq.encoders.msgpack import MsgPackDecoder, MsgPackEncoder
from eventiq.exceptions import DecodeError, EncodeError


def test_default_encoder_decoder(ce):
    data = DEFAULT_ENCODER.encode(ce)
    parsed = DEFAULT_DECODER.decode(data, as_type=type(ce))
    assert parsed == ce


def test_json_encode_error():
    encoder = JsonEncoder()
    model = MagicMock()
    model.model_dump_json.side_effect = ValueError("bad serialization")
    with pytest.raises(EncodeError):
        encoder.encode(model)


def test_json_decode_error():
    decoder = JsonDecoder()
    with pytest.raises(DecodeError):
        decoder.decode(b"{{invalid json!!!")


def test_json_decode_no_type():
    decoder = JsonDecoder()
    result = decoder.decode(b'{"x": 1}')
    assert result is not None


# --- MsgPackEncoder ---


def test_msgpack_encode_decode_roundtrip(ce):
    encoder = MsgPackEncoder()
    decoder = MsgPackDecoder()
    data = encoder.encode(ce)
    assert isinstance(data, bytes)
    parsed = decoder.decode(data, as_type=type(ce))
    assert parsed == ce


@pytest.mark.parametrize("strict", [False, True])
def test_msgpack_roundtrip_preserves_native_types(ce, strict):
    """The decoder must validate in JSON mode to parse the encoder's own output."""
    parsed = MsgPackDecoder(strict=strict).decode(
        MsgPackEncoder().encode(ce), as_type=type(ce)
    )

    assert isinstance(parsed.id, UUID)
    assert isinstance(parsed.time, datetime)
    assert parsed == ce


def test_msgpack_encode_error():
    encoder = MsgPackEncoder()
    model = MagicMock()
    import ormsgpack

    model.model_dump.side_effect = ormsgpack.MsgpackEncodeError("bad")
    with pytest.raises(EncodeError):
        encoder.encode(model)


def test_msgpack_decode_no_type():
    import ormsgpack

    data = ormsgpack.packb({"x": 1})
    decoder = MsgPackDecoder()
    result = decoder.decode(data)
    assert result == {"x": 1}


def test_msgpack_decode_str_input():
    decoder = MsgPackDecoder()
    # "\x2a" encodes to UTF-8 as b"\x2a" which is msgpack fixint 42
    result = decoder.decode("\x2a")
    assert result == 42


def test_msgpack_decode_error():
    decoder = MsgPackDecoder()
    with pytest.raises(DecodeError):
        decoder.decode(b"\xc1")  # reserved byte → invalid msgpack


def test_msgpack_encode_non_native_types():
    """Payload types msgpack cannot represent must be serialized in json mode."""
    ce = CloudEvent.new(
        {
            "amount": Decimal("1.5"),
            "ttl": timedelta(seconds=30),
            "tags": {"a"},
            "ip": IPv4Address("127.0.0.1"),
        },
        type="TestEvent",
        topic="test_topic",
    )
    ce.set_raw(None, {})

    data = MsgPackEncoder().encode(ce)

    assert MsgPackDecoder().decode(data)["data"] == {
        "amount": "1.5",
        "ttl": "PT30S",
        "tags": ["a"],
        "ip": "127.0.0.1",
    }


def test_msgpack_content_type():
    assert MsgPackEncoder.CONTENT_TYPE == "application/x-msgpack"
