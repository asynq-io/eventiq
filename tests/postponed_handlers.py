"""Handlers defined under PEP 563 postponed annotations.

Kept in a separate module because `from __future__ import annotations` must apply
to the whole file: it turns annotations into strings, which is exactly the
condition `resolve_message_type_hint` and dependency injection must cope with.
"""

from __future__ import annotations

from eventiq import CloudEvent


class Dependency:
    def __init__(self, value: str = "injected") -> None:
        self.value = value


class PostponedEvent(CloudEvent[int], topic="postponed.topic"):
    pass


async def handler(message: PostponedEvent) -> None:
    pass


NOT_INJECTED = Dependency("not-injected")


async def handler_with_dependency(
    message: PostponedEvent, dep: Dependency = NOT_INJECTED
) -> Dependency:
    return dep
