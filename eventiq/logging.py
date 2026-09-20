from __future__ import annotations

import inspect
import logging
from typing import Any


def get_logger(module: str, name: str | type) -> logging.Logger:
    if inspect.isclass(name):
        name = name.__name__

    return logging.getLogger(f"{module}.{name}")


_STANDARD_RECORD_ATTRS = frozenset(
    logging.LogRecord("", 0, "", 0, "", None, None).__dict__
) | {"message", "asctime", "taskName"}


class KeyValueFormatter(logging.Formatter):
    """Appends the fields passed through `extra` as a trailing `key=value` list.

    The standard formatter interpolates only the names its format string
    mentions, so fields logged through `extra` never reach a plain console.
    Naming them in the format string is not an alternative: the first record
    without one of them raises `ValueError` and takes the handler down. This
    renders whatever non-standard attributes a record happens to carry instead.
    """

    def formatMessage(self, record: logging.LogRecord) -> str:  # noqa: N802
        message = super().formatMessage(record)
        extras = " ".join(
            f"{key}={value!r}"
            for key, value in record.__dict__.items()
            if key not in _STANDARD_RECORD_ATTRS
        )
        return f"{message} {extras}" if extras else message


class LoggerMixin:
    logger: logging.Logger

    def __init_subclass__(cls, **kwargs: Any) -> None:
        cls.logger = get_logger(cls.__module__, cls)
        super().__init_subclass__(**kwargs)
