from collections.abc import Callable
from typing import (
    Any,
    ClassVar,
    Generic,
    TypeVar,
    cast,
    get_args,
    get_origin,
    overload,
)
from uuid import uuid4

import anyio
from anyio import Event
from pydantic import TypeAdapter
from typing_extensions import Unpack

from eventiq.consumer import ConsumerGroup, GenericConsumer
from eventiq.context import get_current_service
from eventiq.logging import LoggerMixin
from eventiq.models import Publishes
from eventiq.types import ID, Decoder, Encoder, Parameter, Timeout

from .models import ActorMessage
from .types import ActorConsumerGroupOptions

D = TypeVar("D")
R = TypeVar("R")

_TYPE_ARG_COUNT = 2


class _Unset:
    """Sentinel distinguishing a missing result from a legitimate `None` result."""


class _AsyncActorResult(Generic[R]):
    def __init__(self) -> None:
        self._event = Event()
        self._result: Any = _Unset

    def set_result(self, value: Any) -> None:
        self._result = value
        self._event.set()

    async def get(self) -> R:
        await self._event.wait()
        if self._result is _Unset:
            msg = "Result not set"
            raise ValueError(msg)
        return self._result


class Actor(GenericConsumer[ActorMessage[D]], LoggerMixin, Generic[D, R]):
    """Base class for request/reply actors.

    Subclasses must parametrize the accepted data type and the response type,
    e.g. `class Greeter(Actor[str, str])`.
    """

    # Class level default, overridable per instance (e.g. by `Actors(namespace=...)`).
    namespace: str | None = None
    actor_name: ClassVar[str | None] = None

    response_validator: ClassVar[TypeAdapter[Any]]
    actor_event_type: ClassVar[type[ActorMessage[Any]]]

    # Keyed by `str(conversation_id)`: `ID` allows both `UUID` and `str`, and the
    # wire round-trip may return either of them for the very same conversation.
    _results: ClassVar[dict[str, _AsyncActorResult[Any]]] = {}

    def __init_subclass__(cls) -> None:
        data_type, response_type = cls._get_type_args()
        cls.response_validator = TypeAdapter(response_type)
        cls.actor_event_type = ActorMessage[data_type]  # type: ignore[valid-type]
        return super().__init_subclass__()

    @classmethod
    def _get_type_args(cls) -> tuple[Any, Any]:
        for base in getattr(cls, "__orig_bases__", ()):
            if isinstance(get_origin(base), type) and issubclass(
                get_origin(base), Actor
            ):
                args = get_args(base)
                if len(args) == _TYPE_ARG_COUNT:
                    return args[0], args[1]
        msg = (
            f"{cls.__name__} must be parametrized with a data and a response type, "
            f"e.g. class {cls.__name__}(Actor[MyData, MyResponse])"
        )
        raise TypeError(msg)

    @classmethod
    def _get_actor_topic(cls, namespace: str | None = None) -> str:
        return ".".join(
            filter(None, ["actors", namespace or cls.namespace, cls._get_actor_name()]),
        )

    @classmethod
    def _get_actor_name(cls) -> str:
        return cls.actor_name or cls.__name__

    @classmethod
    def _resolve_send_topic(cls, namespace: str | None = None) -> str:
        """Resolve the topic to send to for this actor.

        Without an explicit `namespace` the topic of this actor's consumer in the
        current service wins, so actors registered under `Actors(namespace=...)`
        are reachable without repeating the namespace on every call. Actors hosted
        by another service fall back to the class level namespace.
        """
        if namespace is None:
            for consumer in get_current_service().consumers.values():
                if type(consumer) is cls:
                    return consumer.topic
        return cls._get_actor_topic(namespace)

    def __init__(self, *, namespace: str | None = None, **extra: Any) -> None:
        self.namespace = namespace or type(self).namespace
        extra.setdefault("name", self._get_actor_name())
        extra.setdefault("topic", self._get_actor_topic(self.namespace))
        extra.setdefault("event_type", type(self).actor_event_type)
        super().__init__(**extra)

    @classmethod
    def set_message_result(cls, conversation_id: ID, result: Any) -> None:
        """Fulfil a pending `ask` waiting on `conversation_id`.

        Replies nobody is waiting for (e.g. arriving after a timeout) are dropped
        so the registry cannot grow unboundedly.
        """
        async_result = cls._results.get(str(conversation_id))
        if async_result is None:
            return
        async_result.set_result(result)

    @classmethod
    async def ask(
        cls,
        data: D,
        timeout: float = 30,
        *,
        namespace: str | None = None,
        **kwargs: Any,
    ) -> R:
        """Send `data` to the actor and wait up to `timeout` seconds for its reply.

        Only a successful run is replied to. If the actor's handler raises, nothing
        is sent back and this call fails with `TimeoutError` after `timeout` seconds:
        the actor's own exception is logged by the service hosting it, keyed by the
        conversation id, and never propagated to the caller. A handler retried by a
        `RetryStrategy` may therefore reply long after this call gave up, in which
        case the late reply is discarded.
        """
        kwargs["kind"] = "query"
        conversation_id: ID = kwargs.setdefault("conversation_id", uuid4())
        key = str(conversation_id)
        async_result: _AsyncActorResult[Any] = _AsyncActorResult()
        cls._results[key] = async_result
        try:
            await cls.tell(data, namespace=namespace, **kwargs)
            with anyio.fail_after(timeout):
                res = await async_result.get()
        finally:
            cls._results.pop(key, None)
        return cls.response_validator.validate_python(res)

    @classmethod
    async def tell(cls, data: D, *, namespace: str | None = None, **kwargs: Any) -> ID:
        """Send `data` to the actor without waiting for a reply.

        `namespace` addresses an actor registered under a namespace other than the
        class level one. The message is validated against the actor's declared data
        type, so a bad payload fails here instead of poisoning the actor's queue.
        Returns the conversation id correlating the message with its reply.
        """
        kwargs.setdefault("topic", cls._resolve_send_topic(namespace))
        kwargs.setdefault("kind", "command")
        kwargs.setdefault("conversation_id", uuid4())
        msg = cls.actor_event_type.new(data, **kwargs)
        await get_current_service().publish(msg)
        return msg.conversation_id


ActorT = TypeVar("ActorT", bound=type[Actor])


class Actors(ConsumerGroup):
    """Consumer group registering `Actor` subclasses under a common namespace.

    The namespace is applied to each registered consumer instance, so the same
    actor class may be registered in several namespaces without them interfering.
    """

    def __init__(
        self, namespace: str | None = None, **options: Unpack[ActorConsumerGroupOptions]
    ) -> None:
        attrs = {"namespace": namespace} if namespace else {}
        super().__init__(attrs=attrs, **options)

    @overload
    def actor(
        self,
        actor: ActorT,
        *,
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
    ) -> ActorT: ...

    @overload
    def actor(
        self,
        actor: None = None,
        *,
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
    ) -> Callable[[ActorT], ActorT]: ...

    def actor(
        self,
        actor: ActorT | None = None,
        *,
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
    ) -> ActorT | Callable[[ActorT], ActorT]:
        decorator = self.subscribe(
            None,
            concurrency=concurrency,
            timeout=timeout,
            description=description,
            encoder=encoder,
            decoder=decoder,
            dynamic=dynamic,
            tags=tags,
            publishes=publishes,
            parameters=parameters,
            asyncapi_extra=asyncapi_extra,
            **options,
        )
        return cast(
            "ActorT | Callable[[ActorT], ActorT]",
            decorator if actor is None else decorator(actor),
        )
