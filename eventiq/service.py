from __future__ import annotations

import functools
import os
import signal
import socket
import sys
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Generic

import anyio
from anyio import CancelScope, create_memory_object_stream, from_thread
from pydantic import ValidationError

from .broker import Broker, BulkMessage, R
from .consumer import ChannelConsumer, Consumer, ConsumerGroup
from .context import current_service, reset_current_service, set_current_service
from .dependencies import DefaultDependencyResolver, DependencyResolver
from .encoders import DEFAULT_DECODER, DEFAULT_ENCODER
from .exceptions import ConsumerCancelledError, DecodeError, Fail, Retry, Skip
from .logging import LoggerMixin
from .models import CloudEvent, Publishes
from .types import (
    Decoder,
    Encoder,
    Lifespan,
    Message,
    MiddlewareType,
    P,
    PreparedMessage,
    Publisher,
)
from .utils import to_float

if sys.version_info < (3, 11):
    from exceptiongroup import BaseExceptionGroup

CONTROL_FLOW_EXCEPTIONS: tuple[type[Exception], ...] = (Retry, Skip, Fail)

DEFAULT_MIDDLEWARE_TIMEOUT: int = int(
    os.getenv("EVENTIQ_DEFAULT_MIDDLEWARE_TIMEOUT", "10")
)

DEFAULT_HANDLE_MESSAGE_FINALIZATION_DELAY: int = int(
    os.getenv("EVENTIQ_HANDLE_MESSAGE_FINALIZATION_DELAY", "60")
)

DEFAULT_FINALIZATION_TIMEOUT: int = int(os.getenv("EVENTIQ_FINALIZATION_TIMEOUT", "10"))

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

    from anyio.abc import TaskGroup
    from anyio.streams.memory import MemoryObjectReceiveStream

    from .middleware import MiddlewareProtocol


@asynccontextmanager
async def nullcontext(_: Service) -> AsyncGenerator[None]:
    yield


def find_control_flow_exception(exc: BaseException) -> Exception | None:
    """Return the single `Retry`/`Skip`/`Fail` carried by `exc`, if there is one.

    anyio always wraps exceptions raised inside a task group in a
    `BaseExceptionGroup`, so a control-flow exception raised from a child task
    would otherwise be routed as an ordinary failure. Groups carrying more than
    one of them state no single outcome and stay ordinary failures.
    """
    if isinstance(exc, BaseExceptionGroup):
        found = [
            nested
            for nested in map(find_control_flow_exception, exc.exceptions)
            if nested is not None
        ]
        return found[0] if len(found) == 1 else None
    return exc if isinstance(exc, CONTROL_FLOW_EXCEPTIONS) else None


class Service(LoggerMixin, Generic[Message, R]):
    """Logical group of consumers. Provides group (queue) name and handles versioning."""

    default_middlewares: ClassVar[list[MiddlewareType]] = []

    def __init__(
        self,
        name: str,
        *,
        broker: Broker[Message, R],
        encoder: Encoder = DEFAULT_ENCODER,
        decoder: Decoder = DEFAULT_DECODER,
        title: str | None = None,
        version: str = "0.1.0",
        description: str = "",
        lifespan: Lifespan = nullcontext,
        tags_metadata: list[dict[str, Any]] | None = None,
        publishes: list[Publishes] | None = None,
        async_api_extra: dict[str, Any] | None = None,
        state: dict[type | str, Any] | None = None,
        dependency_resolver: DependencyResolver = DefaultDependencyResolver(),
        middleware_timeout: int = DEFAULT_MIDDLEWARE_TIMEOUT,
        handle_message_finalization_delay: int = DEFAULT_HANDLE_MESSAGE_FINALIZATION_DELAY,
        finalization_timeout: int = DEFAULT_FINALIZATION_TIMEOUT,
        id_generator: Callable[[], str] = socket.gethostname,
        **options: Any,
    ) -> None:
        self.id = id_generator()
        self.broker = broker
        self.name = name
        self.encoder = encoder
        self.decoder = decoder
        self.title = title or name.title()
        self.version = version
        self.description = description
        self.tags_metadata = tags_metadata or []
        self.consumer_group = ConsumerGroup()
        self.subscribe = self.consumer_group.subscribe
        self.middlewares: list[MiddlewareProtocol] = []
        for m in self.default_middlewares:
            self.add_middleware(m)
        self.lifespan = lifespan
        self.publishes = publishes or []
        self.async_api_extra = async_api_extra or {}
        self.state = state or {}
        self.state[Publisher] = self.publish
        self.dependency_resolver = dependency_resolver
        self.middleware_timeout = middleware_timeout
        self.handle_message_finalization_delay = handle_message_finalization_delay
        self.finalization_timeout = finalization_timeout
        self.options = options
        self.default_action = getattr(self, self.broker.default_on_exc)
        self._background_tg: TaskGroup | None = None

    def start_background_task(
        self,
        func: Callable[..., Awaitable[Any]],
        *args: Any,
        name: str | None = None,
    ) -> None:
        """Schedule `func` to run for as long as the service does.

        Callable from any middleware hook while the service is running. The task
        is cancelled once the consumers have stopped and before the broker
        disconnects, so `after_broker_disconnect` observes it already finished.

        Exceptions propagate and tear the service down, as for any task group
        child; a task that should survive its own failures must handle them.
        """
        if self._background_tg is None:
            msg = "Background tasks can only be started while the service is running"
            raise RuntimeError(msg)
        self._background_tg.start_soon(func, *args, name=name)

    def add_middleware(
        self,
        middleware: MiddlewareType[P],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.middlewares.append(middleware(self, *args, **kwargs))

    @property
    def add_consumer_group(self) -> Callable[[ConsumerGroup], None]:
        return self.consumer_group.add_consumer_group

    @property
    def consumers(self) -> dict[str, Consumer]:
        return self.consumer_group.consumers

    async def send(
        self,
        data: Any,
        type: type[CloudEvent] | str = CloudEvent,
        headers: dict[str, str] | None = None,
        encoder: Encoder | None = None,
        **kwargs: Any,
    ) -> R:
        """Build an event of `type` around `data` and publish it."""
        if isinstance(type, str):
            kwargs["type"] = type
            event_cls = CloudEvent
        else:
            event_cls = type
        ce = event_cls.new(data, source=self.name, headers=headers, **kwargs)
        return await self.publish(ce, encoder=encoder)

    def prepare_message(
        self,
        message: CloudEvent,
        topic: str | None = None,
        encoder: Encoder | None = None,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> PreparedMessage:
        message_topic = topic or message.topic
        encoder = encoder or self.encoder
        # The publishing service is the only party that knows the origin, and
        # `message_source` is a required broker argument: an event built outside
        # any service context would otherwise reach the broker without it.
        if message.source is None:
            message.source = self.name
        message.content_type = encoder.CONTENT_TYPE
        message.headers["Content-Type"] = encoder.CONTENT_TYPE
        if topic:
            message.headers["Destination"] = topic
        if headers:
            message.headers.update(headers)
        body = encoder.encode(message)
        message_kwargs = {
            f"message_{k}": v
            for k, v in message.model_dump(
                exclude_none=True,
                by_alias=False,
                exclude={"topic", "data"},
            ).items()
        }
        message_kwargs.update(kwargs)
        return message_topic, body, message_kwargs

    def publish_sync(
        self,
        message: CloudEvent,
        topic: str | None = None,
        headers: dict[str, str] | None = None,
        encoder: Encoder | None = None,
        **kwargs: Any,
    ) -> R:
        """Publish `message` from a synchronous caller.

        Must be called from a worker thread started by anyio (e.g. inside
        `anyio.to_thread.run_sync`); `from_thread.run` raises otherwise.
        """
        fn = functools.partial(
            self.publish,
            message,
            topic=topic,
            headers=headers,
            encoder=encoder,
            **kwargs,
        )
        return from_thread.run(fn)

    async def publish(
        self,
        message: CloudEvent,
        topic: str | None = None,
        headers: dict[str, str] | None = None,
        encoder: Encoder | None = None,
        **kwargs: Any,
    ) -> R:
        """Encode and publish `message`, returning the broker's response."""
        await self.dispatch_before("publish", message=message, **kwargs)
        message_topic, body, message_kwargs = self.prepare_message(
            message,
            topic,
            encoder,
            headers=headers,
            **kwargs,
        )
        body, encoded_headers = await self._encode_payload(body, message.headers)
        message.headers.update(encoded_headers)
        res = await self.broker.publish(
            message_topic,
            body,
            headers=message.headers,
            **message_kwargs,
        )
        await self.dispatch_after("publish", message=message, **kwargs)
        return res

    def bulk_publish_sync(
        self,
        messages: Sequence[CloudEvent],
        *,
        topic: str | None = None,
        headers: dict[str, str] | None = None,
        encoder: Encoder | None = None,
        **kwargs: Any,
    ) -> None:
        """Publish `messages` from a synchronous caller.

        Must be called from a worker thread started by anyio (e.g. inside
        `anyio.to_thread.run_sync`); `from_thread.run` raises otherwise.
        """
        fn = functools.partial(
            self.bulk_publish,
            messages,
            topic=topic,
            headers=headers,
            encoder=encoder,
            **kwargs,
        )
        from_thread.run(fn)

    async def bulk_publish(
        self,
        messages: Sequence[CloudEvent],
        *,
        topic: str | None = None,
        headers: dict[str, str] | None = None,
        encoder: Encoder | None = None,
        **kwargs: Any,
    ) -> None:
        bulk_messages: list[BulkMessage] = []
        for message in messages:
            await self.dispatch_before("publish", message=message, **kwargs)
            message_topic, body, message_kwargs = self.prepare_message(
                message,
                topic,
                encoder,
                headers=headers,
                **kwargs,
            )
            body, encoded_headers = await self._encode_payload(body, message.headers)
            message.headers.update(encoded_headers)
            msg = BulkMessage(message_topic, body, message.headers, message_kwargs)
            bulk_messages.append(msg)

        await self.broker.bulk_publish(bulk_messages, topic=topic)

        for message in messages:
            await self.dispatch_after("publish", message=message, **kwargs)

    async def connect(self) -> None:
        await self.dispatch_before("broker_connect")
        await self.broker.connect()
        await self.dispatch_after("broker_connect")

    async def disconnect(self) -> None:
        await self.dispatch_before("broker_disconnect")
        await self.broker.disconnect()
        await self.dispatch_after("broker_disconnect")

    async def start_consumers(self, tg: TaskGroup) -> None:
        for consumer in self.consumers.values():
            await self.dispatch_before("consumer_start", consumer=consumer)
            send_stream, receive_stream = create_memory_object_stream[Message](
                consumer.concurrency,
            )

            tg.start_soon(self.broker.sender, self.name, consumer, send_stream)

            # Each receiver owns a clone; closing the original keeps the stream's
            # reference count equal to the number of running receivers.
            with receive_stream:
                for i in range(consumer.concurrency):
                    self.logger.info(
                        "Starting consumer task",
                        extra={"consumer_name": consumer.name, "task_index": i},
                    )
                    tg.start_soon(
                        self.receiver,
                        consumer,
                        receive_stream.clone(),
                        name=f"{consumer.name}:{i + 1}",
                    )
            await self.dispatch_after("consumer_start", consumer=consumer)

    async def run(self, *, enable_signal_handler: bool = True) -> None:
        """Connect, start every consumer, and block until cancelled or signalled."""
        # Ambient before the lifespan runs and until after disconnect, so that
        # `run` and `context` expose an identical window to user code.
        with current_service(self):
            async with self.lifespan(self) as state:
                if state:
                    self.state.update(state)

                try:
                    # Opened before `connect`, so that `after_broker_connect` can
                    # already schedule background tasks, and left before
                    # `disconnect`, so they are all reaped by the time the broker
                    # goes away.
                    async with anyio.create_task_group() as background_tg:
                        self._background_tg = background_tg
                        await self.connect()
                        try:
                            async with anyio.create_task_group() as tg:
                                if enable_signal_handler:
                                    tg.start_soon(
                                        self.watch_for_signals, tg.cancel_scope
                                    )
                                await self.start_consumers(tg)
                        finally:
                            background_tg.cancel_scope.cancel()
                finally:
                    self._background_tg = None
                    with anyio.move_on_after(5, shield=True):
                        await self.disconnect()

    @asynccontextmanager
    async def context(
        self, *, enable_signal_handler: bool = False
    ) -> AsyncGenerator[None]:
        """Run the service for the duration of the block, then shut it down."""
        with current_service(self):
            async with self.lifespan(self) as state:
                if state:
                    self.state.update(state)
                try:
                    async with anyio.create_task_group() as background_tg:
                        self._background_tg = background_tg
                        await self.connect()
                        try:
                            async with anyio.create_task_group() as tg:
                                if enable_signal_handler:
                                    tg.start_soon(
                                        self.watch_for_signals, tg.cancel_scope
                                    )
                                await self.start_consumers(tg)
                                try:
                                    yield
                                finally:
                                    # Sender/receiver tasks never return on their
                                    # own, so leaving the task group would block
                                    # forever and `disconnect` would never run.
                                    # Announce the shutdown first, exactly as
                                    # `watch_for_signals` does - unless that is who
                                    # cancelled us, and already announced it.
                                    if not tg.cancel_scope.cancel_called:
                                        await self.dispatch_before("close_consumers")
                                    tg.cancel_scope.cancel()
                        finally:
                            background_tg.cancel_scope.cancel()
                finally:
                    self._background_tg = None
                    with anyio.move_on_after(5, shield=True):
                        await self.disconnect()

    async def watch_for_signals(self, scope: CancelScope) -> None:
        with anyio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
            async for signum in signals:
                self.logger.info("Received signal", extra={"signal": signum.name})
                await self.dispatch_before("close_consumers")
                scope.cancel()

    async def _dispatch(self, event: str, **kwargs: Any) -> None:
        message = kwargs.get("message")
        middlewares = (
            reversed(self.middlewares)
            if event.startswith("after_")
            else self.middlewares
        )
        with anyio.fail_after(self.middleware_timeout, shield=True):
            for middleware in middlewares:
                if message and (
                    middleware.requires is not None
                    and not isinstance(message, middleware.requires)
                ):
                    self.logger.debug(
                        "Skipping event for middleware",
                        extra={
                            "event": event,
                            "middleware": type(middleware).__name__,
                        },
                    )
                    continue
                method = getattr(middleware, event, None)
                if method is None:
                    self.logger.debug(
                        "Method not found in middleware",
                        extra={
                            "event": event,
                            "middleware": type(middleware).__name__,
                        },
                    )
                    continue

                await method(**kwargs)

    async def dispatch_before(self, event: str, **kwargs: Any) -> None:
        await self._dispatch(f"before_{event}", **kwargs)

    async def dispatch_after(self, event: str, **kwargs: Any) -> None:
        await self._dispatch(f"after_{event}", **kwargs)

    async def _encode_payload(
        self, payload: bytes, headers: dict[str, str]
    ) -> tuple[bytes, dict[str, str]]:
        for middleware in self.middlewares:
            method = getattr(middleware, "encode_payload", None)
            if method is not None:
                payload, headers = await method(payload, headers)
        return payload, headers

    async def _decode_payload(self, payload: bytes, headers: dict[str, str]) -> bytes:
        for middleware in reversed(self.middlewares):
            method = getattr(middleware, "decode_payload", None)
            if method is not None:
                payload = await method(payload, headers)
        return payload

    async def receiver(
        self,
        consumer: Consumer,
        receive_stream: MemoryObjectReceiveStream[Message],
    ) -> None:
        consumer_timeout = to_float(
            consumer.timeout or self.broker.default_consumer_timeout,
        )
        decoder = consumer.decoder or self.decoder

        async with receive_stream:
            async for raw_message in receive_stream:
                await self._process(
                    consumer,
                    raw_message,
                    decoder,
                    consumer_timeout,
                )

    async def ack(self, consumer: Consumer, message: Message) -> None:
        """Acknowledge a message, shielded so shutdown cannot turn it into a nack."""
        await self.dispatch_before("ack", consumer=consumer, raw_message=message)
        with anyio.move_on_after(self.finalization_timeout, shield=True) as scope:
            await self.broker.ack(message)
        if scope.cancelled_caught:
            # The message was not acknowledged and will be redelivered. Reporting
            # `after_ack` would make health and metrics middlewares record a
            # completion the broker never saw.
            self.logger.error(
                "Timed out acknowledging message",
                extra={
                    "consumer_name": consumer.name,
                    "raw_message": str(message),
                },
            )
            return
        try:
            await self.dispatch_after("ack", consumer=consumer, raw_message=message)
        except Exception:
            # The broker has already settled this delivery: letting the failure
            # reach the finalization fallback would nack an acknowledged message,
            # which brokers answer with a channel error or a redelivery.
            self.logger.exception("Error dispatching after ack.")

    async def nack(
        self,
        consumer: Consumer,
        message: Message,
        delay: int | None = None,
    ) -> None:
        await self.dispatch_before(
            "nack",
            consumer=consumer,
            raw_message=message,
        )
        with anyio.move_on_after(self.finalization_timeout, shield=True) as scope:
            await self.broker.nack(message, delay)
        if scope.cancelled_caught:
            # As in `ack`: the broker never rejected the message, so middlewares
            # must not be told that it did.
            self.logger.error(
                "Timed out rejecting message",
                extra={
                    "consumer_name": consumer.name,
                    "raw_message": str(message),
                    "delay": delay,
                },
            )
            return
        try:
            await self.dispatch_after(
                "nack",
                consumer=consumer,
                raw_message=message,
            )
        except Exception:
            # As in `ack`: the delivery is already settled, so the finalization
            # fallback must not settle it a second time.
            self.logger.exception("Error dispatching after nack.")

    async def _process(
        self,
        consumer: Consumer,
        raw_message: Message,
        decoder: Decoder,
        timeout: float,
    ) -> None:
        exc: Exception | None = None
        result = None
        # Balanced set/reset: receiver tasks own a private context copy, but
        # `_process` may also be driven from a task shared with unrelated code,
        # which must not be left publishing through this service.
        token = set_current_service(self)
        try:
            try:
                data, headers = self.broker.decode_message(raw_message)
                data = await self._decode_payload(data, headers)
                message = decoder.decode(data, consumer.event_type)
                message.set_raw(raw_message, headers)
            except (DecodeError, ValidationError) as e:
                # A poison message: redelivering it can only fail the same way, so
                # it is dropped (or parked by brokers that can nack it with a delay).
                self.logger.exception(
                    "Failed to validate message",
                    extra={
                        "consumer_name": consumer.name,
                        "raw_message": str(raw_message),
                    },
                    exc_info=e,
                )
                if self.broker.should_nack(raw_message):
                    await self.nack(
                        consumer,
                        raw_message,
                        delay=self.broker.validate_error_delay,
                    )
                else:
                    await self.ack(consumer, raw_message)
                return
            except Exception as e:
                # Anything else (broker decoding, a `decode_payload` middleware) may
                # well be a transient or local fault, so the message is nacked for
                # redelivery rather than discarded. It must still not escape: that
                # would kill the receiver task and with it the whole consumer.
                self.logger.exception(
                    "Unexpected decode failure",
                    extra={
                        "consumer_name": consumer.name,
                        "raw_message": str(raw_message),
                    },
                    exc_info=e,
                )
                await self.nack(
                    consumer,
                    raw_message,
                    delay=self.handle_message_finalization_delay,
                )
                return

            try:
                await self.dispatch_before(
                    "process_message",
                    consumer=consumer,
                    message=message,
                )
                self.logger.info(
                    "Running consumer",
                    extra={
                        "consumer_name": consumer.name,
                        "message_id": str(message.id),
                    },
                )
                with anyio.fail_after(timeout):
                    result = await consumer.process(message)
            except Exception as e:
                exc = e
            except anyio.get_cancelled_exc_class() as e:
                exc = ConsumerCancelledError("Consumer cancelled")
                exc.__cause__ = e
                raise
            finally:
                await self._handle_message_finalization_with_fallback(
                    consumer, message, result, exc
                )
        finally:
            reset_current_service(token)

    async def _handle_message_finalization_with_fallback(
        self,
        consumer: Consumer,
        message: CloudEvent,
        result: Any,
        exc: Exception | None,
    ) -> None:
        try:
            await self._handle_message_finalization(consumer, message, result, exc)
        except Exception:
            self.logger.exception("Error handling message finalization.")
            with anyio.CancelScope(shield=True):
                await self.nack(
                    consumer,
                    message.raw,
                    delay=self.handle_message_finalization_delay,
                )
        except anyio.get_cancelled_exc_class():
            self.logger.warning("Message finalization cancelled, nacking message.")
            with anyio.CancelScope(shield=True):
                await self.nack(
                    consumer,
                    message.raw,
                    delay=self.handle_message_finalization_delay,
                )
            raise
        finally:
            with anyio.CancelScope(shield=True):
                await self.dispatch_after(
                    "message_finalized",
                    consumer=consumer,
                    message=message,
                    result=result,
                    exc=exc,
                )

    async def _handle_message_finalization(
        self,
        consumer: Consumer,
        message: CloudEvent,
        result: Any,
        exc: Exception | None,
    ) -> None:
        if exc is not None:
            control_flow_exc = find_control_flow_exception(exc)
            if control_flow_exc is not None:
                exc = control_flow_exc

        try:
            await self.dispatch_after(
                "process_message",
                consumer=consumer,
                message=message,
                result=result,
                exc=exc,
            )
        except Exception as e:
            # Keep the consumer's own exception as the routing decision: a failing
            # after-hook must not silently reclassify a Retry/Skip/Fail outcome.
            self.logger.exception("Error dispatching after process_message.")
            if exc is None:
                exc = e

        if exc is None:
            await self.ack(consumer, message.raw)
            return

        if isinstance(exc, Retry):
            await self.dispatch_after(
                "retry_message",
                consumer=consumer,
                message=message,
                exc=exc,
            )
            await self.nack(consumer, message.raw, delay=exc.delay)
            return

        if isinstance(exc, Skip):
            await self.dispatch_after(
                "skip_message",
                consumer=consumer,
                message=message,
                exc=exc,
            )
            await self.ack(consumer, message.raw)
            return

        if isinstance(exc, Fail):
            await self.dispatch_after(
                "fail_message",
                consumer=consumer,
                message=message,
                exc=exc,
            )
            await self.ack(consumer, message.raw)
            return

        await self.default_action(consumer, message.raw)

    @asynccontextmanager
    async def subscription(
        self,
        event_type: type[CloudEvent] = CloudEvent,
        topic: str | None = None,
        **options: Any,
    ) -> AsyncIterator[
        MemoryObjectReceiveStream[tuple[CloudEvent, Callable[[], None]]]
    ]:
        """Async with service.subscription(MyEvent, topic="example.topic") as subscription:
        async for event, ack in subscription:
            print(event)
            ack().
        """
        send_stream, receive_stream = create_memory_object_stream[Any](1)
        consumer_send, user_receive = create_memory_object_stream[
            tuple[CloudEvent, Callable[[], None]]
        ](1)
        consumer: Consumer[CloudEvent] = ChannelConsumer(
            channel=consumer_send,
            event_type=event_type,
            topic=topic,
            **options,
        )

        async with anyio.create_task_group() as tg, consumer_send, user_receive:
            tg.start_soon(self.broker.sender, self.name, consumer, send_stream)
            tg.start_soon(self.receiver, consumer, receive_stream)
            try:
                yield user_receive
            finally:
                tg.cancel_scope.cancel()
