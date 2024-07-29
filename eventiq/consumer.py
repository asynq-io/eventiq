from __future__ import annotations

import asyncio
import inspect
from abc import ABC, abstractmethod
from collections.abc import Awaitable
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Union,
    overload,
)

from typing_extensions import TypedDict, Unpack

from .logging import get_logger
from .types import CloudEventType, Decoder, Parameter, Timeout
from .utils import resolve_message_type_hint, to_async

if TYPE_CHECKING:
    from .models import Publishes

Fn = Callable[[CloudEventType], Awaitable[Any]]


class Consumer(ABC, Generic[CloudEventType]):
    """Base consumer class"""

    def __init__(
        self,
        *,
        name: str,
        event_type: type[CloudEventType],
        topic: str | None = None,
        timeout: Timeout | None = None,
        dynamic: bool = False,
        tags: list[str] | None = None,
        decoder: Decoder | None = None,
        description: str | None = None,
        concurrency: int = 1,
        publishes: list[Publishes] | None = None,
        parameters: dict[str, Parameter] | None = None,
        asyncapi_extra: dict[str, Any] | None = None,
        **options: Any,
    ) -> None:
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
        self.parameters = parameters or {}
        self.description = description
        self.publishes = publishes or []
        self.asyncapi_extra = asyncapi_extra or {}
        self.options: dict[str, Any] = options
        self.logger = get_logger(__name__, self.name)

    @abstractmethod
    async def process(self, message: CloudEventType) -> Any:
        raise NotImplementedError


class FnConsumer(Consumer[CloudEventType]):
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

    async def process(self, message: CloudEventType) -> Any:
        return await self.fn(message)


class GenericConsumer(Consumer[CloudEventType], ABC):
    def __init__(self, **extra: Any) -> None:
        if "name" not in extra:
            extra["name"] = getattr(type(self), "name", type(self).__name__)
        if "event_type" not in extra:
            extra["event_type"] = type(self).__orig_bases__[0].__args__[0]  # type: ignore
        if "description" not in extra:
            extra["description"] = type(self).__doc__ or ""

        super().__init__(**extra)


MessageHandlerT = Union[type[GenericConsumer], Fn]


class ConsumerGroupOptions(TypedDict, total=False):
    topic: str
    timeout: Timeout
    dynamic: bool
    tags: list[str]
    decoder: Decoder
    description: str
    concurrency: int
    publishes: list[Publishes]
    parameters: dict[str, Parameter]
    asyncapi_extra: dict[str, Any]


class ConsumerOptions(ConsumerGroupOptions, total=False):
    name: str


class ConsumerGroup:
    def __init__(self, **options: Unpack[ConsumerGroupOptions]) -> None:
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
        **options: Unpack[ConsumerOptions],
    ) -> Callable[[MessageHandlerT], MessageHandlerT]: ...

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
