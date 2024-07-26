from __future__ import annotations

import functools
import re
from collections.abc import Awaitable
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Literal, TypeVar, cast, get_type_hints, overload
from urllib.parse import urlparse

from anyio import to_thread
from typing_extensions import ParamSpec

from eventiq.types import Timeout

P = ParamSpec("P")
R = TypeVar("R", bound=Any)

TOPIC_PATTERN = re.compile(r"{\w+}")
TOPIC_SPECIAL_CHARS = {"{", "}", "*", ">"}


def utc_now():
    return datetime.now(tz=timezone.utc)


def to_async(func: Callable[P, R]) -> Callable[P, Awaitable[R]]:
    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> Awaitable[R]:
        if not kwargs:
            return to_thread.run_sync(func, *args)
        return to_thread.run_sync(functools.partial(func, *args, **kwargs))

    return wrapper


def get_safe_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.username and parsed.password:
        parsed = parsed._replace(
            netloc="{}:{}@{}:{}".format(
                parsed.username or "", "*****", parsed.hostname, parsed.port
            )
        )
    return parsed.geturl()


def resolve_message_type_hint(func) -> type[Any] | None:
    try:
        return func.__annotations__["message"]
    except (AttributeError, KeyError):
        pass
    hints = get_type_hints(func)
    if "message" in hints:
        return hints["message"]
    hints.pop("return", None)
    try:
        return next(iter(hints.values()))
    except StopIteration:
        return None


def format_topic(topic: str, wildcard_one: str, wildcard_many: str) -> str:
    result = []

    for k in topic.split("."):
        if re.fullmatch(TOPIC_PATTERN, k):
            result.append(wildcard_one)
        elif k in {"*", ">"}:
            result.append(wildcard_many)
        else:
            result.append(k)
    return ".".join(filter(None, result))


def get_topic_regex(topic: str) -> str:
    result = []

    for k in topic.split("."):
        if re.fullmatch(TOPIC_PATTERN, k):
            result.append(r"\w+")

        elif k in {"*", ">"}:
            result.append(r"*")
        else:
            result.append(k)
    return r"^{}$".format(r"\.".join(result))


@overload
def to_float(timeout: Timeout) -> float: ...


@overload
def to_float(timeout: Timeout | None) -> float | None: ...


def to_float(timeout: Timeout | None) -> float | None:
    # TODO: for some reason type narrowing doesnt work here
    if timeout is None:
        return None
    if isinstance(timeout, timedelta):
        return timeout.total_seconds()
    return float(timeout)


def get_annotation(value: str) -> type:
    return cast(type, Literal[value])
