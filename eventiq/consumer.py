from __future__ import annotations

import inspect
import socket
from abc import ABC, abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    overload,
)
from uuid import uuid4

import anyio
from typing_extensions import Concatenate, Unpack

from .dependencies import resolved_func
from .logging import get_logger
from .types import CloudEventType, P, RetryStrategy
from .utils import is_async_callable, resolve_message_type_hint, to_async, to_float

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from anyio.streams.memory import MemoryObjectSendStream

    from .models import Publishes
    from .types import (
        ConsumerGroupOptions,
        Decoder,
        Encoder,
        MessageHandler,
        Parameter,
        Publisher,
        Timeout,
    )


class Consumer(ABC, Generic[CloudEventType]):
    """Base consumer class."""

    def __init__(
        self,
        *,
        name: str,
        event_type: type[CloudEventType],
        topic: str | None = None,
        concurrency: int = 1,
        timeout: Timeout | None = None,
        description: str | None = None,
        encoder: Encoder | None = None,
        decoder: Decoder | None = None,
        retry_strategy: RetryStrategy | None = None,
        dynamic: bool = False,
        store_results: bool = False,
        tags: list[str] | None = None,
        publishes: list[Publishes] | None = None,
        parameters: dict[str, Parameter] | None = None,
        asyncapi_extra: dict[str, Any] | None = None,
        **options: Any,
    ) -> None:
        if event_type is None:
            msg = "Event type is required"
            raise ValueError(msg)
        topic = topic or event_type.get_default_topic()
        if not topic:
            msg = "Topic is required"
            raise ValueError(msg)
        if concurrency < 1:
            msg = "Concurrency must be greater than 0"
            raise ValueError(msg)
        self.name = name
        self.event_type = event_type
        self.topic = topic
        self.timeout = timeout
        self.tags = tags
        self.encoder = encoder
        self.decoder = decoder
        self.dynamic = dynamic
        self.concurrency = concurrency
        self.retry_strategy = retry_strategy
        self.store_results = store_results
        self.parameters = parameters or {}
        self.description = description
        self.publishes = publishes or []
        self.asyncapi_extra = asyncapi_extra or {}
        self.options = options
        self.logger = get_logger(__name__, self.name)

    def maybe_set_publisher(self, publisher: Publisher) -> None:
        pass

    if TYPE_CHECKING:
        process: Callable[Concatenate[CloudEventType, ...], Awaitable[Any]]
    else:

        @abstractmethod
        async def process(self, message: CloudEventType) -> Any:
            raise NotImplementedError


class FnConsumer(Consumer[CloudEventType], Generic[CloudEventType, P]):
    def __init__(
        self,
        *,
        fn: Callable[Concatenate[CloudEventType, P], Awaitable[Any]],
        **extra: Any,
    ) -> None:
        if "name" not in extra:
            extra["name"] = fn.__name__
        if "event_type" not in extra:
            extra["event_type"] = resolve_message_type_hint(fn)
        if "description" not in extra:
            extra["description"] = fn.__doc__ or ""
        if not is_async_callable(fn):
            fn = to_async(fn)
        self.fn = resolved_func(fn)
        super().__init__(**extra)

    async def process(
        self, message: CloudEventType, *args: P.args, **kwargs: P.kwargs
    ) -> Any:
        return await self.fn(message, *args, **kwargs)


class GenericConsumer(Consumer[CloudEventType], ABC):
    def __init__(self, **extra: Any) -> None:
        if "name" not in extra:
            extra["name"] = getattr(type(self), "name", type(self).__name__)
        if "event_type" not in extra:
            extra["event_type"] = type(self).__orig_bases__[0].__args__[0]  # type: ignore[attr-defined]
        if "description" not in extra:
            extra["description"] = type(self).__doc__ or ""
        super().__init__(**extra)
        self._publish: Publisher | None = None
        self.process = resolved_func(self.process)

    def maybe_set_publisher(self, publisher: Publisher) -> None:
        self._publish = publisher

    @property
    def publish(self) -> Publisher:
        if self._publish is None:
            err = "Publisher is not set"
            raise RuntimeError(err)
        return self._publish


class ChannelConsumer(Consumer[CloudEventType]):
    def __init__(
        self,
        channel: MemoryObjectSendStream[tuple[CloudEventType, Callable[[], None]]],
        **extra: Any,
    ) -> None:
        if "name" not in extra:
            extra.setdefault("dynamic", True)
            extra["name"] = f"{socket.gethostname()}:{uuid4()}"
        super().__init__(**extra)
        self.channel = channel
        self._timeout = to_float(self.timeout) or 10.0

    async def process(self, message: CloudEventType) -> Any:
        event = anyio.Event()
        await self.channel.send((message, event.set))
        with anyio.fail_after(self._timeout):
            await event.wait()


class ConsumerGroup:
    def __init__(self, **options: Unpack[ConsumerGroupOptions]) -> None:
        self.options = options
        self.consumers: dict[str, Consumer] = {}

    def add_consumer(self, consumer: Consumer) -> None:
        self.consumers[consumer.name] = consumer

    def add_consumer_group(self, other: ConsumerGroup) -> None:
        self.consumers.update(other.consumers)

    @overload
    def subscribe(self, func_or_cls: MessageHandler) -> MessageHandler: ...

    @overload
    def subscribe(
        self,
        func_or_cls: None = None,
        name: str | None = None,
        event_type: type[CloudEventType] | None = None,
        topic: str | None = None,
        concurrency: int | None = None,
        timeout: Timeout | None = None,
        description: str | None = None,
        encoder: Encoder | None = None,
        decoder: Decoder | None = None,
        dynamic: bool | None = None,
        tags: list[str] | None = None,
        publishes: list[Publishes] | None = None,
        parameters: dict[str, Parameter] | None = None,
        asyncapi_extra: dict[str, Any] | None = None,
        **options: Any,
    ) -> Callable[[MessageHandler], MessageHandler]: ...

    def subscribe(
        self,
        func_or_cls: MessageHandler | None = None,
        name: str | None = None,
        event_type: type[CloudEventType] | None = None,
        topic: str | None = None,
        concurrency: int | None = None,
        timeout: Timeout | None = None,
        description: str | None = None,
        encoder: Encoder | None = None,
        decoder: Decoder | None = None,
        dynamic: bool | None = None,
        tags: list[str] | None = None,
        publishes: list[Publishes] | None = None,
        parameters: dict[str, Parameter] | None = None,
        asyncapi_extra: dict[str, Any] | None = None,
        **options: Any,
    ) -> MessageHandler | Callable[[MessageHandler], MessageHandler]:
        def decorator(func_or_cls: MessageHandler) -> MessageHandler:
            cls: type[Consumer] = FnConsumer
            if inspect.isfunction(func_or_cls):
                options["fn"] = func_or_cls

            elif isinstance(func_or_cls, type) and issubclass(
                func_or_cls,
                GenericConsumer,
            ):
                cls = func_or_cls
            else:
                msg = f"Expected function or GenericConsumer got {type(func_or_cls)}"
                raise TypeError(
                    msg,
                )
            options.update(
                {
                    "name": name,
                    "event_type": event_type,
                    "topic": topic,
                    "concurrency": concurrency,
                    "timeout": timeout,
                    "description": description,
                    "encoder": encoder,
                    "dynamic": dynamic,
                    "tags": tags,
                    "publishes": publishes,
                    "parameters": parameters,
                    "asyncapi_extra": asyncapi_extra,
                }
            )
            filtered_options = {k: v for k, v in options.items() if v is not None}
            for k, v in self.options.items():
                filtered_options.setdefault(k, v)
            consumer = cls(**filtered_options)
            self.add_consumer(consumer)
            return func_or_cls

        if func_or_cls is None:
            return decorator

        return decorator(func_or_cls)
