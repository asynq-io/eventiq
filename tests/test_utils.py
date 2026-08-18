from datetime import timedelta, timezone

import pytest

from eventiq import CloudEvent
from eventiq.utils import (
    format_topic,
    get_safe_url,
    get_topic_regex,
    is_async_callable,
    resolve_message_type_hint,
    to_async,
    to_float,
    utc_now,
)

# --- to_float ---


def test_to_float_none():
    assert to_float(None) is None


def test_to_float_int():
    assert to_float(5) == 5.0


def test_to_float_float():
    assert to_float(2.5) == 2.5


def test_to_float_timedelta():
    assert to_float(timedelta(seconds=30)) == 30.0


# --- utc_now ---


def test_utc_now_is_utc():
    now = utc_now()
    assert now.tzinfo == timezone.utc


# --- format_topic ---


def test_format_topic_single_wildcard():
    assert format_topic("events.{type}.created", "*", "#") == "events.*.created"


def test_format_topic_many_wildcard():
    result = format_topic("events.>", "*", "#")
    assert result == "events.#"


def test_format_topic_no_wildcards():
    assert format_topic("events.user.created", "*", "#") == "events.user.created"


def test_format_topic_multiple_params():
    result = format_topic("events.{type}.{action}", "*", "#")
    assert result in {"events.*.#", "events.*.*"}


# --- get_topic_regex ---


def test_get_topic_regex_matches_param():
    import re

    pattern = get_topic_regex("events.{type}.created")
    assert re.match(pattern, "events.user.created")
    assert not re.match(pattern, "events.created")
    assert not re.match(pattern, "events.user.updated")


def test_get_topic_regex_static():
    import re

    pattern = get_topic_regex("events.user.created")
    assert re.match(pattern, "events.user.created")
    assert not re.match(pattern, "events.user.updated")


# --- get_safe_url ---


def test_get_safe_url_redacts_password():
    url = get_safe_url("nats://user:s3cr3t@localhost:4222")
    assert "s3cr3t" not in url
    assert "*****" in url
    assert "user" in url


def test_get_safe_url_no_credentials():
    url = get_safe_url("nats://localhost:4222")
    assert url == "nats://localhost:4222"


def test_get_safe_url_without_port_keeps_host_intact():
    url = get_safe_url("amqp://user:s3cr3t@rabbitmq/vhost")
    assert url == "amqp://user:*****@rabbitmq/vhost"
    assert "None" not in url


def test_get_safe_url_preserves_explicit_port():
    url = get_safe_url("amqp://user:s3cr3t@rabbitmq:5672/vhost")
    assert url == "amqp://user:*****@rabbitmq:5672/vhost"


def test_get_safe_url_username_only_is_untouched():
    url = get_safe_url("nats://user@localhost:4222")
    assert url == "nats://user@localhost:4222"


# --- resolve_message_type_hint under PEP 563 ---


def test_resolve_message_type_hint_evaluates_postponed_annotations():
    from .postponed_handlers import PostponedEvent, handler

    resolved = resolve_message_type_hint(handler)
    assert resolved is PostponedEvent
    assert not isinstance(resolved, str)


# --- is_async_callable ---


def test_is_async_callable_sync_function():
    def sync_fn() -> None:
        pass

    assert not is_async_callable(sync_fn)


def test_is_async_callable_async_function():
    async def async_fn() -> None:
        pass

    assert is_async_callable(async_fn)


def test_is_async_callable_async_class():
    class AsyncCallable:
        async def __call__(self) -> None:
            pass

    assert is_async_callable(AsyncCallable())


# --- to_async ---


@pytest.mark.anyio
async def test_to_async_no_args():
    """to_async with a zero-argument function (no args branch, line 43)."""

    def get_answer() -> int:
        return 42

    wrapped = to_async(get_answer)
    result = await wrapped()
    assert result == 42


@pytest.mark.anyio
async def test_to_async_wraps_sync():
    def double(x: int) -> int:
        return x * 2

    wrapped = to_async(double)
    result = await wrapped(5)
    assert result == 10


@pytest.mark.anyio
async def test_to_async_wraps_sync_with_kwargs():
    def add(x: int, y: int = 0) -> int:
        return x + y

    wrapped = to_async(add)
    result = await wrapped(3, y=7)
    assert result == 10


# --- resolve_message_type_hint ---


def test_resolve_message_type_hint_annotation():
    async def handler(message: CloudEvent) -> None:
        pass

    result = resolve_message_type_hint(handler)
    assert result is CloudEvent


def test_resolve_message_type_hint_first_arg():
    async def handler(msg: CloudEvent) -> None:
        pass

    result = resolve_message_type_hint(handler)
    assert result is CloudEvent


def test_resolve_message_type_hint_no_hints():
    async def handler(msg) -> None:
        pass

    result = resolve_message_type_hint(handler)
    assert result is None


def test_resolve_message_type_hint_no_annotations_at_all():
    assert resolve_message_type_hint(lambda _msg: None) is None


def test_resolve_message_type_hint_reports_unresolvable_annotation():
    """A TYPE_CHECKING-only annotation must name itself, not look like a missing type."""

    async def handler(message) -> None:
        pass

    # What `if TYPE_CHECKING: from x import TypeCheckingOnlyEvent` plus postponed
    # annotations leaves behind: a string no runtime namespace can resolve.
    handler.__annotations__ = {"message": "TypeCheckingOnlyEvent", "return": None}

    with pytest.raises(TypeError, match="TypeCheckingOnlyEvent") as exc_info:
        resolve_message_type_hint(handler)

    assert "Could not resolve annotations" in str(exc_info.value)
    assert isinstance(exc_info.value.__cause__, NameError)


# --- get_topic_regex wildcard segment (line 98) ---


def test_get_topic_regex_wildcard_star_matches_single_segment():
    import re

    pattern = get_topic_regex("events.*.created")
    assert re.fullmatch(pattern, "events.user.created")
    assert not re.fullmatch(pattern, "events.created")
    assert not re.fullmatch(pattern, "events.user.admin.created")


def test_get_topic_regex_wildcard_greater_matches_remaining_segments():
    import re

    pattern = get_topic_regex("events.>")
    assert re.fullmatch(pattern, "events.user")
    assert re.fullmatch(pattern, "events.user.created")
    assert not re.fullmatch(pattern, "events")


def test_get_topic_regex_bare_star_is_valid_pattern():
    import re

    pattern = get_topic_regex("*")
    assert re.fullmatch(pattern, "events")
    assert not re.fullmatch(pattern, "events.user")


def test_get_topic_regex_escapes_dots():
    import re

    pattern = get_topic_regex("events.user.created")
    assert not re.fullmatch(pattern, "eventsXuserXcreated")


# --- is_async_callable with functools.partial (line 140) ---


def test_is_async_callable_partial_async():
    import functools

    async def async_fn(x: int) -> int:
        return x

    wrapped = functools.partial(async_fn, 42)
    assert is_async_callable(wrapped)


def test_is_async_callable_partial_sync():
    import functools

    def sync_fn(x: int) -> int:
        return x

    wrapped = functools.partial(sync_fn, 42)
    assert not is_async_callable(wrapped)
