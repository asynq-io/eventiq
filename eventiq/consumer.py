from __future__ import annotations

import asyncio
import inspect
from abc import ABC, abstractmethod
from collections.abc import Awaitable
from typing import TYPE_CHECKING, Any, Callable, Generic, Union, overload

from .logging import get_logger
from .types import CloudEvent, Decoder, Timeout
from .utils import resolve_message_type_hint, to_async

if TYPE_CHECKING:
    from .models import Publishes

Fn = Callable[[CloudEvent], Awaitable[Any]]


class Consumer(ABC, Generic[CloudEvent]):
    """Base consumer class"""

    def __init__(
        self,
        *,
        name: str,
        event_type: type[CloudEvent],
        topic: str | None = None,
        timeout: Timeout | None = None,
        dynamic: bool = False,
        tags: list[str] | None = None,
        decoder: Decoder | None = None,
        description: str | None = None,
        concurrency: int = 1,
        publishes: list[Publishes] | None = None,
        **options: Any,
    ):
        if event_type is None:
            raise ValueError("Event type is required")
        topic = topic or event_type.get_default_topic()
        if not topic:
            raise ValueError("Topic is required")
        if concurrency < 1:
            raise ValueError("Concurrency must be greater than 0")
        self.name = name
        self.event_type = event_type
        self.topic = topic
        self.timeout = timeout
        self.dynamic = dynamic
        self.tags = tags
        self.decoder = decoder
        self.concurrency = concurrency
        # self.parameters = parameters
        self.description = description
        self.publishes = publishes or []
        self.options: dict[str, Any] = options
        self.logger = get_logger(__name__, self.name)

    @abstractmethod
    async def process(self, message: CloudEvent) -> Any:
        raise NotImplementedError


class FnConsumer(Consumer[CloudEvent]):
    def __init__(
        self,
        *,
        fn: Fn,
        **extra: Any,
    ) -> None:
        if "name" not in extra:
            extra["name"] = fn.__name__
        if "event_type" not in extra:
            extra["event_type"] = resolve_message_type_hint(fn)
        if "description" not in extra:
            extra["description"] = fn.__doc__ or ""
        if not asyncio.iscoroutinefunction(fn):
            fn = to_async(fn)
        self.fn = fn
        super().__init__(**extra)

    async def process(self, message: CloudEvent) -> Any:
        return await self.fn(message)


class GenericConsumer(Consumer[CloudEvent], ABC):
    def __init__(self, **extra: Any) -> None:
        if "name" not in extra:
            extra["name"] = getattr(type(self), "name", type(self).__name__)

        if "event_type" not in extra:
            extra["event_type"] = type(self).__orig_bases__[0].__args__[0]  # type: ignore
        if "description" not in extra:
            extra["description"] = type(self).__doc__ or ""

        super().__init__(**extra)


MessageHandlerT = Union[type[GenericConsumer], Fn]


class ConsumerGroup:
    def __init__(self, **options: Any):
        self.consumers: dict[str, Consumer] = {}
        self.options = options

    def add_consumer(self, consumer: Consumer) -> None:
        self.consumers[consumer.name] = consumer

    def add_consumer_group(self, other: ConsumerGroup) -> None:
        self.consumers.update(other.consumers)

    @overload
    def subscribe(self, func_or_cls: MessageHandlerT) -> MessageHandlerT: ...

    @overload
    def subscribe(
        self,
        func_or_cls: None = None,
        **options: Any,
    ) -> Callable[[MessageHandlerT], MessageHandlerT]: ...

    def subscribe(
        self,
        func_or_cls: MessageHandlerT | None = None,
        **options: Any,
    ) -> MessageHandlerT | Callable[[MessageHandlerT], MessageHandlerT]:
        def decorator(func_or_cls: MessageHandlerT) -> MessageHandlerT:
            cls: type[Consumer] = FnConsumer
            if inspect.isfunction(func_or_cls):
                options["fn"] = func_or_cls

            elif isinstance(func_or_cls, type) and issubclass(
                func_or_cls, GenericConsumer
            ):
                cls = func_or_cls
            else:
                raise TypeError(
                    f"Expected function or GenericConsumer got {type(func_or_cls)}"
                )
            for k, v in self.options.items():
                options.setdefault(k, v)
            consumer = cls(**options)

            self.add_consumer(consumer)
            return func_or_cls

        if func_or_cls is None:
            return decorator

        return decorator(func_or_cls)
