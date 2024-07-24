from eventiq.decoder import DEFAULT_DECODER
from eventiq.encoder import DEFAULT_ENCODER


def test_default_encoder_decoder(ce):
    data = DEFAULT_ENCODER.encode(ce)
    parsed = DEFAULT_DECODER.decode(data, as_type=type(ce))
    assert parsed == ce
