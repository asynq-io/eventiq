from __future__ import annotations

import functools
import inspect
import re
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    Concatenate,
    Literal,
    TypeGuard,
    TypeVar,
    cast,
    get_type_hints,
    overload,
)
from urllib.parse import urlparse

from anyio import to_thread
from typing_extensions import ParamSpec

if TYPE_CHECKING:
    from eventiq.types import Timeout

P = ParamSpec("P")
R = TypeVar("R", bound=Any)
T = TypeVar("T")


TOPIC_PATTERN = re.compile(r"{\w+}")
TOPIC_SPECIAL_CHARS = {"{", "}", "*", ">"}


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


@overload
def to_async(
    func: Callable[Concatenate[T, P], R],
) -> Callable[Concatenate[T, P], Awaitable[R]]: ...


@overload
def to_async(func: Callable[P, R]) -> Callable[P, Awaitable[R]]: ...


def to_async(func: Callable[P, R]) -> Callable[P, Awaitable[R]]:
    """Run `func` in a worker thread, preserving its (possibly `Concatenate`d) signature."""

    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> Awaitable[R]:
        if args or kwargs:
            return to_thread.run_sync(functools.partial(func, *args, **kwargs))
        return to_thread.run_sync(func)

    return wrapper


def get_safe_url(url: str) -> str:
    """Return `url` with its password redacted, preserving the rest verbatim."""
    parsed = urlparse(url)
    if parsed.password:
        host = parsed.hostname or ""
        if parsed.port is not None:
            host = f"{host}:{parsed.port}"
        parsed = parsed._replace(
            netloc=f"{parsed.username or ''}:*****@{host}",
        )
    return parsed.geturl()


def resolve_message_type_hint(func: Callable) -> type[Any] | None:
    """Resolve the event type a handler accepts.

    Annotations are always evaluated, so handlers defined in modules using
    `from __future__ import annotations` resolve to types rather than strings.
    Returns `None` for an unannotated handler, but raises `TypeError` when an
    annotation exists and cannot be resolved, rather than reporting it later as
    a missing event type.
    """
    if not getattr(func, "__annotations__", None):
        return None
    try:
        hints = get_type_hints(func)
    except (NameError, TypeError) as e:
        name = getattr(func, "__qualname__", None) or repr(func)
        msg = (
            f"Could not resolve annotations of {name}: {e}. Types used in handler "
            "signatures must be importable at runtime, not only under "
            "`if TYPE_CHECKING`."
        )
        raise TypeError(msg) from e
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
    """Build a regex matching `topic`, expanding `{param}`/`*`/`>` wildcards."""
    result = []

    for k in topic.split("."):
        if re.fullmatch(TOPIC_PATTERN, k) or k == "*":
            result.append(r"[^.]+")
        elif k == ">":
            result.append(r".+")
        else:
            result.append(re.escape(k))
    return r"^{}$".format(r"\.".join(result))


@overload
def to_float(timeout: Timeout) -> float: ...


@overload
def to_float(timeout: None) -> None: ...


def to_float(timeout: Timeout | None) -> float | None:
    if timeout is None:
        return None
    if isinstance(timeout, timedelta):
        return timeout.total_seconds()
    return float(timeout)


def get_annotation(value: str) -> type:
    return cast("type", Literal[value])


AwaitableCallable = Callable[..., Awaitable[T]]


@overload
def is_async_callable(obj: AwaitableCallable[T]) -> TypeGuard[AwaitableCallable[T]]: ...


@overload
def is_async_callable(obj: Any) -> TypeGuard[AwaitableCallable[Any]]: ...


def is_async_callable(obj: Any) -> Any:
    while isinstance(obj, functools.partial):
        obj = obj.func

    return inspect.iscoroutinefunction(obj) or (
        callable(obj) and inspect.iscoroutinefunction(obj.__call__)
    )
