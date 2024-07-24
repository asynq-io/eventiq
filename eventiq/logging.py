from __future__ import annotations

import inspect
import logging


def get_logger(module: str, name: str | type) -> logging.Logger:
    if inspect.isclass(name):
        name = name.__name__

    return logging.getLogger(f"{module}.{name}")


class LoggerMixin:
    logger: logging.Logger

    def __init_subclass__(cls, **kwargs):
        cls.logger = get_logger(__name__, cls)
