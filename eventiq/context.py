from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import TYPE_CHECKING, overload

if TYPE_CHECKING:
    from collections.abc import Generator

    from .service import Service


_current_service: ContextVar[Service | None] = ContextVar(
    "_current_service", default=None
)


_default_services: list[Service] = []


def find_current_service() -> Service | None:
    """Return the ambient service, or `None` when no service is running.

    The current task's service takes precedence over the process-wide default.
    """
    service = _current_service.get()
    if service is not None:
        return service
    return _default_services[-1] if _default_services else None


def get_current_service() -> Service:
    """Return the ambient service, raising when none is running."""
    service = find_current_service()
    if service is None:
        msg = "Service object accessed outside of context"
        raise RuntimeError(msg)
    return service


def set_current_service(service: Service | None) -> Token[Service | None]:
    """Set the ambient service for the current task, returning a restore token."""
    return _current_service.set(service)


def reset_current_service(token: Token[Service | None]) -> None:
    """Restore the service that was ambient before the matching `set` call."""
    _current_service.reset(token)


def _remove_default_service(service: Service) -> None:
    for index in reversed(range(len(_default_services))):
        if _default_services[index] is service:
            del _default_services[index]
            return


@contextmanager
def current_service(service: Service) -> Generator[None]:
    """Expose `service` as the ambient service while the block runs.

    The service is set both for the current task and, as a fallback, for every
    other task in the process: an ASGI app starts its service in the lifespan
    task, while requests are served from sibling tasks a contextvar never reaches.

    The fallback is a stack, so services running concurrently in one process may
    stop in any order without clobbering each other's fallback.
    """
    token = set_current_service(service)
    _default_services.append(service)
    try:
        yield
    finally:
        _remove_default_service(service)
        reset_current_service(token)


class ServiceContext:
    """Descriptor resolving the ambient `Service` on attribute access.

    Class-level access yields the descriptor instead of resolving a service, so
    that importing and introspecting a class (`help`, `inspect.getmembers`,
    documentation tooling, debuggers) never raises. Use `get_current_service()`
    where a service is needed outside of an instance.
    """

    @overload
    def __get__(self, instance: None, owner: type | None = None) -> ServiceContext: ...

    @overload
    def __get__(self, instance: object, owner: type | None = None) -> Service: ...

    def __get__(
        self, instance: object | None, owner: type | None = None
    ) -> Service | ServiceContext:
        if instance is None:
            return self
        return get_current_service()
