"""Handlers whose parameter annotations cannot be evaluated at runtime.

Kept in a separate module because `from __future__ import annotations` must apply
to the whole file: combined with the `if TYPE_CHECKING` import below it produces a
dependency annotation that only a type checker can resolve, which is exactly the
condition dependency injection must reject loudly instead of silently degrading.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from eventiq import CloudEvent, GenericConsumer

if TYPE_CHECKING:
    from decimal import Decimal


class UnresolvableEvent(CloudEvent[int], topic="unresolvable.topic"):
    pass


async def handler_with_unresolvable_dependency(
    message: UnresolvableEvent, db: Decimal
) -> None:
    pass


class UnresolvableConsumer(GenericConsumer[UnresolvableEvent]):
    name = "unresolvable_consumer"

    async def process(self, message: UnresolvableEvent, db: Decimal) -> None:
        pass
