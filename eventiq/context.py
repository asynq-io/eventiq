from __future__ import annotations

from contextvars import ContextVar
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .service import Service


_current_service: ContextVar[Service | None] = ContextVar(
    "_current_service", default=None
)


def get_current_service() -> Service:
    service = _current_service.get()
    if service is None:
        msg = "Service object accessed outside of context"
        raise RuntimeError(msg)
    return service


def set_current_service(service: Service | None) -> None:
    _current_service.set(service)


class ServiceContext:
    def __get__(self, instance: object, owner: type | None = None) -> Service:
        return get_current_service()
