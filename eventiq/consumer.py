from __future__ import annotations

import inspect
import socket
from abc import ABC, abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    Concatenate,
    Generic,
    get_args,
    get_origin,
    overload,
)
from uuid import uuid4

import anyio
from typing_extensions import Unpack

from .context import ServiceContext
from .dependencies import resolved_func
from .logging import get_logger
from .types import CloudEventType, P, RetryStrategy
from .utils import is_async_callable, resolve_message_type_hint, to_async, to_float

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

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
        tags: list[str] | None = None,
        publishes: list[Publishes] | None = None,
        parameters: dict[str, Parameter] | None = None,
        asyncapi_extra: dict[str, Any] | None = None,
        **options: Any,
    ) -> None:
        if concurrency < 1:
            msg = "Concurrency must be greater than 0"
            raise ValueError(msg)
        if event_type is None:
            msg = "Event type is required"
            raise ValueError(msg)
        self.name = name
        self.event_type = event_type
        topic = topic or event_type.get_default_topic()
        if not topic:
            msg = "Topic is required"
            raise ValueError(msg)
        self._topic = topic
        self.timeout = timeout
        self.tags = tags
        self.encoder = encoder
        self.decoder = decoder
        self.dynamic = dynamic
        self.concurrency = concurrency
        self.retry_strategy = retry_strategy
        self.parameters = parameters or {}
        self.description = description
        self.publishes = publishes or []
        self.asyncapi_extra = asyncapi_extra or {}
        self.options = options
        self.logger = get_logger(__name__, self.name)

    @property
    def topic(self) -> str:
        return self._topic

    if TYPE_CHECKING:
        process: Callable[Concatenate[CloudEventType, ...], Awaitable[Any]]
    else:

        @abstractmethod
        async def process(self, message: CloudEventType) -> Any:
            raise NotImplementedError


_CONSUMER_INIT_KWARGS: frozenset[str] = frozenset(
    inspect.signature(Consumer.__init__).parameters,
) - {"self", "options"}


class FnConsumer(Consumer[CloudEventType], Generic[CloudEventType, P]):
    """
    Function consumer. This class should not be used directly, the object is created
    by the framework when `@service.subscribe` decorator is used.
    """

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
        async_fn: Callable[Concatenate[CloudEventType, P], Awaitable[Any]] = (
            fn if is_async_callable(fn) else to_async(fn)
        )
        self.fn = resolved_func(async_fn)
        super().__init__(**extra)

    async def process(
        self,
        message: CloudEventType,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Any:
        return await self.fn(message, *args, **kwargs)


class GenericConsumer(Consumer[CloudEventType], ABC):
    """
    Class based consumer
    """

    service: ServiceContext = ServiceContext()

    @classmethod
    def _resolve_event_type(cls) -> type[Any]:
        """Infer the event type from the parametrized `GenericConsumer[...]` base."""
        for base in getattr(cls, "__orig_bases__", ()):
            origin = get_origin(base)
            if isinstance(origin, type) and issubclass(origin, GenericConsumer):
                args = get_args(base)
                if args:
                    return args[0]
        msg = (
            f"Could not infer event_type for {cls.__name__}: parametrize the "
            f"consumer (e.g. class {cls.__name__}(GenericConsumer[MyEvent])) "
            f"or pass event_type explicitly"
        )
        raise ValueError(msg)

    def __init__(self, **extra: Any) -> None:
        if "name" not in extra:
            extra["name"] = getattr(type(self), "name", type(self).__name__)
        if "event_type" not in extra:
            extra["event_type"] = self._resolve_event_type()
        if "description" not in extra:
            extra["description"] = type(self).__doc__ or ""
        super().__init__(**extra)
        self.process = resolved_func(self.process)

    @property
    def publish(self) -> Publisher:
        return self.service.publish


class ChannelConsumer(Consumer[CloudEventType]):
    """
    Short-lived, ephemeral consumer, to be created in runtime
    """

    def __init__(
        self,
        channel: MemoryObjectSendStream[tuple[CloudEventType, Callable[[], None]]],
        **extra: Any,
    ) -> None:
        extra.setdefault("dynamic", True)
        if "name" not in extra:
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
    """
    Consumer group. Similar to how FastAPI groups paths using APIRouter.
    :param attrs: Extra keyword arguments passed to the constructor of every
        consumer registered in this group, below explicit `subscribe` arguments.
    :param options: Default options to set for each consumer in this group.
    """

    def __init__(
        self,
        *,
        attrs: dict[str, Any] | None = None,
        **options: Unpack[ConsumerGroupOptions],
    ) -> None:
        self.attrs = attrs or {}
        self.options = options
        self.consumers: dict[str, Consumer] = {}

    def _validate_attrs(self, cls: type[Consumer]) -> None:
        """Reject attrs which cannot be applied to `cls` instead of failing obscurely."""
        for key in self.attrs:
            if key in _CONSUMER_INIT_KWARGS:
                continue
            attr = inspect.getattr_static(cls, key, None)
            if isinstance(attr, property) and attr.fset is None:
                msg = (
                    f"Attribute {key!r} is a read-only property of "
                    f"{cls.__name__} and cannot be set via consumer group attrs"
                )
                raise ValueError(msg)

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
        *,
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
        *,
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
            self._validate_attrs(cls)
            options.update(
                {
                    "name": name,
                    "event_type": event_type,
                    "topic": topic,
                    "concurrency": concurrency,
                    "timeout": timeout,
                    "description": description,
                    "encoder": encoder,
                    "decoder": decoder,
                    "dynamic": dynamic,
                    "tags": tags,
                    "publishes": publishes,
                    "parameters": parameters,
                    "asyncapi_extra": asyncapi_extra,
                },
            )
            filtered_options = {k: v for k, v in options.items() if v is not None}
            # Attrs and group options are passed to the constructor rather than set
            # on the class or the instance afterwards: the class object is shared by
            # every group and service, and post-construction attributes cannot
            # influence __init__ (nor overwrite read-only properties like `topic`).
            for k, v in self.attrs.items():
                filtered_options.setdefault(k, v)
            for k, v in self.options.items():
                filtered_options.setdefault(k, v)
            consumer = cls(**filtered_options)
            self.add_consumer(consumer)
            return func_or_cls

        if func_or_cls is None:
            return decorator

        return decorator(func_or_cls)
