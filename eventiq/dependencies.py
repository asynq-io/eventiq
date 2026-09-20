from __future__ import annotations

import functools
import logging
from inspect import Parameter, signature
from typing import TYPE_CHECKING, Any, Concatenate, Final, Protocol

import anyio
from typing_extensions import ParamSpec

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from .models import CloudEvent
    from .types import CloudEventType

P = ParamSpec("P")

logger = logging.getLogger(__name__)


class _Unresolved:
    """Sentinel distinguishing "no value" from a legitimate `None` dependency."""

    def __repr__(self) -> str:
        return "UNRESOLVED"


UNRESOLVED: Final = _Unresolved()

CLOSE_TIMEOUT: Final = 3


class DependencyResolver(Protocol):
    """Provides the values of handler parameters other than `message`."""

    async def resolve(self, message: CloudEvent, annotation: Any) -> Any:
        """Return the value of a parameter typed `annotation`, or `UNRESOLVED`."""

    async def close(self) -> None:
        """Release whatever resolving the message just handled allocated."""


class DefaultDependencyResolver:
    """Resolves parameters from `service.state`, matched by their annotation."""

    async def resolve(self, message: CloudEvent, annotation: Any) -> Any:
        return message.service.state.get(annotation, UNRESOLVED)

    async def close(self) -> None:
        """Nothing is held per message, so there is nothing to release."""


def resolved_func(
    func: Callable[Concatenate[CloudEventType, P], Awaitable[Any]],
) -> Callable[Concatenate[CloudEventType, P], Awaitable[Any]]:
    """Wrap `func` so its annotated parameters are injected at call time.

    Parameters are resolved by the service's `dependency_resolver`, which reads
    `service.state` unless replaced - that is how external containers (see
    `eventiq.integrations`) plug in per-message scopes. The resolver is closed
    once `func` has returned, releasing that scope.

    Annotations are evaluated, so handlers in modules using
    `from __future__ import annotations` still get their dependencies injected.
    An annotation which cannot be evaluated raises `TypeError` here, rather than
    degrading to a string which no resolver can ever resolve.
    """
    try:
        sig = signature(func, eval_str=True)
    except (NameError, TypeError) as e:
        name = getattr(func, "__qualname__", None) or repr(func)
        msg = (
            f"Could not resolve annotations of {name}: {e}. Types used in handler "
            "signatures must be importable at runtime, not only under "
            "`if TYPE_CHECKING`."
        )
        raise TypeError(msg) from e
    params = {
        k: v.annotation
        for k, v in sig.parameters.items()
        if k != "message"
        and v.annotation is not Parameter.empty
        and v.kind
        not in {
            Parameter.POSITIONAL_ONLY,
            Parameter.VAR_POSITIONAL,
            Parameter.VAR_KEYWORD,
        }
    }

    if not params:
        return func

    @functools.wraps(func)
    async def wrapped(
        message: CloudEventType,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Any:
        service = message.service
        resolver = service.dependency_resolver
        try:
            for k, annotation in params.items():
                value = await resolver.resolve(message, annotation)
                if value is not UNRESOLVED:
                    kwargs[k] = value

            return await func(message, *args, **kwargs)
        finally:
            with anyio.move_on_after(delay=CLOSE_TIMEOUT, shield=True) as scope:
                await resolver.close()
            if scope.cancelled_caught:
                logger.error(
                    "Timed out closing dependency resolver",
                    extra={"resolver": repr(resolver)},
                )

    return wrapped
