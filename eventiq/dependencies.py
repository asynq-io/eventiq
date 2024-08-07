from __future__ import annotations

import functools
import inspect
from typing import TYPE_CHECKING, Any, Callable

from typing_extensions import Concatenate

from .exceptions import DependencyError

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from .types import CloudEventType


def resolved_func(
    func: Callable[Concatenate[CloudEventType, ...], Awaitable[Any]],
) -> Callable[Concatenate[CloudEventType, ...], Awaitable[Any]]:
    signature = inspect.signature(func)
    params = {
        k: (v.annotation, v.default)
        for k, v in signature.parameters.items()
        if k not in {"message", "args", "kwargs", "_"}
        and v.kind
        not in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}
    }

    if not params:
        return func

    @functools.wraps(func)
    async def wrapped(message: CloudEventType, **kwargs: Any) -> Any:
        state = message.service.state

        for k, v in params.items():
            annotation, default = v
            if annotation in state:
                kwargs[k] = state[annotation]
            elif default is inspect.Parameter.empty:
                err = f"Missing dependency {k}: {annotation}"
                raise DependencyError(err)

        return await func(message, **kwargs)

    return wrapped
