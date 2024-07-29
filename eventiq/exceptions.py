from __future__ import annotations


class EventiqError(Exception):
    """Base exception for Eventiq"""


class BrokerError(EventiqError):
    """Base Exception for broker related errors"""


class EncodeError(EventiqError):
    """Error encoding message"""


class DecodeError(EventiqError):
    """Error decoding message"""


class MessageError(EventiqError):
    """Base message processing error"""

    def __init__(self, reason: str):
        self.reason = reason

    def __str__(self):
        return f"{self.__class__.__name__}: {self.reason}"


class Skip(MessageError):
    """Raise exception to skip message without processing and/or retrying"""


class Fail(MessageError):
    """Fail message without retrying"""


class Retry(MessageError):
    """
    Utility exception for retrying message.
    RetryMiddleware must be added
    """

    def __init__(self, reason: str | None = None, delay: int | None = None):
        super().__init__(reason or "unknown")
        self.delay = delay
