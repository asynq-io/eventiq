from __future__ import annotations

import functools
from inspect import Parameter, signature
from typing import TYPE_CHECKING, Any, Callable

from typing_extensions import Concatenate, ParamSpec

from .exceptions import DependencyError

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from .types import CloudEventType

P = ParamSpec("P")


def resolved_func(
    func: Callable[Concatenate[CloudEventType, P], Awaitable[Any]],
) -> Callable[Concatenate[CloudEventType, P], Awaitable[Any]]:
    sig = signature(func)
    params = {
        k: (v.annotation, v.default)
        for k, v in sig.parameters.items()
        if k not in {"message", "args", "kwargs"}
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
        message: CloudEventType, *args: P.args, **kwargs: P.kwargs
    ) -> Any:
        state = message.service.state

        for k, v in params.items():
            annotation, default = v
            if annotation in state:
                kwargs[k] = state[annotation]
            elif default is Parameter.empty:
                err = f"Missing dependency {k}: {annotation}"
                raise DependencyError(err)

        return await func(message, *args, **kwargs)

    return wrapped
