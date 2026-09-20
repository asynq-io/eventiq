from eventiq.exceptions import (
    BrokerConnectionError,
    BrokerError,
    DecodeError,
    EncodeError,
    EventiqError,
    Fail,
    MessageError,
    Retry,
    Skip,
)


def test_hierarchy():
    assert issubclass(BrokerError, EventiqError)
    assert issubclass(BrokerConnectionError, BrokerError)
    assert issubclass(DecodeError, EventiqError)
    assert issubclass(EncodeError, EventiqError)
    assert issubclass(MessageError, EventiqError)
    assert issubclass(Skip, MessageError)
    assert issubclass(Fail, MessageError)
    assert issubclass(Retry, MessageError)


def test_message_error_str():
    e = Fail("some reason")
    assert str(e) == "Fail: some reason"
    assert e.reason == "some reason"


def test_skip_str():
    s = Skip("skipping")
    assert "skipping" in str(s)
    assert s.reason == "skipping"


def test_retry_no_args():
    r = Retry()
    assert r.delay is None
    assert r.reason == "unknown"


def test_retry_with_delay():
    r = Retry(delay=30)
    assert r.delay == 30


def test_retry_with_reason():
    r = Retry(reason="too slow")
    assert r.reason == "too slow"
    assert r.delay is None


def test_retry_str():
    r = Retry(reason="overloaded")
    assert "overloaded" in str(r)
