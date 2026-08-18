from __future__ import annotations

import typing
from contextvars import ContextVar
from typing import TYPE_CHECKING, Annotated, Any, get_args, get_origin

from modern_di import Scope, integrations, providers

from eventiq.dependencies import UNRESOLVED, DefaultDependencyResolver
from eventiq.middleware import CloudEventType, Middleware
from eventiq.models import CloudEvent

if TYPE_CHECKING:
    from modern_di import Container

    from eventiq.service import Service

DI_CONTAINER_STATE_KEY = "di_container"

eventiq_message_provider = providers.ContextProvider(CloudEvent, scope=Scope.REQUEST)

_CONNECTION_PROVIDERS = (eventiq_message_provider,)

# `resolved_func` resolves and closes within one call, so the container in
# flight is task state - exactly like the ambient service in `eventiq.context`.
# A dict keyed by the message would alias on recycled `id()`s, and one shared
# attribute would be overwritten by every concurrently consumed message.
_request_container: ContextVar[Container | None] = ContextVar(
    "_request_container", default=None
)

FromDI = integrations.from_di

__all__ = [
    "DI_CONTAINER_STATE_KEY",
    "FromDI",
    "ModernDIMiddleware",
    "ModernDIResolver",
    "eventiq_message_provider",
    "fetch_di_container",
    "setup_di",
]


def setup_di(service: Service, container: Container) -> Container:
    """Attach `container` to `service` and wire it into the message lifecycle.

    Call `container.validate()` after this, never before: the message's
    `ContextProvider` is only registered here.
    """
    service.state[DI_CONTAINER_STATE_KEY] = container
    container.add_providers(*_CONNECTION_PROVIDERS)
    service.add_middleware(ModernDIMiddleware)
    service.dependency_resolver = ModernDIResolver(container)
    return container


def fetch_di_container(service: Service) -> Container:
    """Return the root container attached to `service` by `setup_di`."""
    return typing.cast("Container", service.state[DI_CONTAINER_STATE_KEY])


def _unwrap(annotation: Any) -> tuple[integrations.Marker[Any] | None, Any]:
    """Split an annotation into its `FromDI` marker, if any, and its bare type."""
    if get_origin(annotation) is not Annotated:
        return None, annotation
    args = get_args(annotation)
    for meta in args[1:]:
        if isinstance(meta, integrations.Marker):
            return meta, args[0]
    return None, args[0]


class ModernDIMiddleware(Middleware[CloudEventType]):
    """Owns the root container's lifecycle.

    It is opened when the broker connects and closed when it disconnects, so
    that `Scope.APP` dependencies live exactly as long as the service does.
    Per-message scopes belong to `ModernDIResolver`.
    """

    async def before_broker_connect(self) -> None:
        fetch_di_container(self.service).open()

    async def after_broker_disconnect(self) -> None:
        await fetch_di_container(self.service).close_async()


class ModernDIResolver(DefaultDependencyResolver):
    """Resolves consumer dependencies from `service.state`, then from `container`.

    The first dependency the container has to provide opens a `Scope.REQUEST`
    child container carrying the message itself as context; `close` runs its
    finalizers once the consumer has returned. A handler needing nothing from
    the container never opens one.
    """

    def __init__(self, container: Container) -> None:
        self.container = container

    def _request_scope(self, message: CloudEvent) -> Container:
        container = _request_container.get()
        if container is None:
            match = integrations.bind(eventiq_message_provider, message)
            container = self.container.build_child_container(
                scope=match.scope,
                context=match.context,
            )
            _request_container.set(container)
        return container

    async def resolve(self, message: CloudEvent, annotation: Any) -> Any:
        value = await super().resolve(message, annotation)
        if value is not UNRESOLVED:
            return value

        marker, dependency_type = _unwrap(annotation)
        if marker is not None:
            return marker.resolve(self._request_scope(message))

        if (
            isinstance(dependency_type, type)
            and self.container.providers_registry.find_provider(dependency_type)
            is not None
        ):
            return self._request_scope(message).resolve(dependency_type)

        return UNRESOLVED

    async def close(self) -> None:
        container = _request_container.get()
        if container is None:
            return
        _request_container.set(None)
        await container.close_async()
