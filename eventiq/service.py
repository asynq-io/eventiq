from __future__ import annotations

import asyncio
import signal
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress
from typing import TYPE_CHECKING, Any, Callable, Generic

import anyio
from anyio import CancelScope, create_memory_object_stream
from pydantic import ValidationError

from .broker import Broker, R
from .consumer import ChannelConsumer, Consumer, ConsumerGroup
from .exceptions import DecodeError, Fail, Retry, Skip
from .logging import LoggerMixin
from .models import CloudEvent, Publishes
from .results import Result, ResultBackend, ResultBackendMiddleware
from .types import ID, Decoder, Encoder, Message
from .utils import to_float

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from anyio.abc import TaskGroup
    from anyio.streams.memory import MemoryObjectReceiveStream

    from .middleware import Middleware

Lifespan = Callable[["Service"], AbstractAsyncContextManager[None]]


@asynccontextmanager
async def nullcontext(service: Service):
    yield


class Service(Generic[Message, R], LoggerMixin):
    """Logical group of consumers. Provides group (queue) name and handles versioning."""

    default_middlewares: list[Middleware] = []

    def __init__(
        self,
        name: str,
        broker: Broker[Message, R],
        title: str | None = None,
        version: str = "0.1.0",
        description: str = "",
        middlewares: list[Middleware] | None = None,
        lifespan: Lifespan = nullcontext,
        tags_metadata: list[dict[str, Any]] | None = None,
        publishes: list[Publishes] | None = None,
        async_api_extra: dict[str, Any] | None = None,
        **options,
    ) -> None:
        self.broker = broker
        self.name = name
        self.title = title or name.title()
        self.version = version
        self.description = description
        self.tags_metadata = tags_metadata or []
        self.consumer_group = ConsumerGroup()
        self.middlewares = self.default_middlewares
        if middlewares:
            self.middlewares.extend(middlewares)
        if isinstance(broker, ResultBackend) and not any(
            isinstance(m, ResultBackendMiddleware) for m in self.middlewares
        ):
            self.middlewares.append(ResultBackendMiddleware())
        self.lifespan = lifespan
        self.publishes = publishes or []
        self.async_api_extra = async_api_extra or {}
        self.options = options

    def add_middleware(self, middleware: type[Middleware], *args, **kwargs) -> None:
        self.middlewares.append(middleware(*args, **kwargs))

    @property
    def subscribe(self):
        return self.consumer_group.subscribe

    @property
    def add_consumer_group(self):
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
        **kwargs,
    ) -> R:
        if isinstance(type, str):
            kwargs["type"] = type
            type = CloudEvent
        ce = type.new(data, type=type, source=self.name, headers=headers, **kwargs)
        if headers:
            ce.headers.update(headers)
        return await self.publish(ce)

    async def publish(
        self,
        message: CloudEvent,
        encoder: Encoder | None = None,
        **kwargs,
    ) -> R:
        if not message.source:
            message.source = self.name

        await self.dispatch_before("publish", message=message)
        res = await self.broker.publish(message, encoder=encoder, **kwargs)
        await self.dispatch_after("publish", message=message)
        return res

    async def connect(self):
        await self.dispatch_before("broker_connect")
        await self.broker.connect()
        await self.dispatch_after("broker_connect")

    async def disconnect(self):
        await self.dispatch_before("broker_disconnect")
        await self.broker.disconnect()
        await self.dispatch_after("broker_disconnect")

    async def start_consumers(self, tg: TaskGroup):
        for consumer in self.consumers.values():
            await self.dispatch_before("consumer_start", consumer=consumer)
            send_stream, receive_stream = create_memory_object_stream[Any](
                consumer.concurrency * 2,
            )

            tg.start_soon(self.broker.sender, self.name, consumer, send_stream)

            for i in range(1, consumer.concurrency + 1):
                self.logger.info(f"Starting consumer {consumer.name} task {i}")
                tg.start_soon(
                    self.receiver,
                    consumer,
                    receive_stream.clone(),
                    name=f"{consumer.name}:{i}",
                )
            await self.dispatch_after("consumer_start", consumer=consumer)

    async def run(self, enable_signal_handler: bool = True) -> None:
        async with self.lifespan(self):
            try:
                await self.connect()
                async with anyio.create_task_group() as tg:
                    if enable_signal_handler:
                        tg.start_soon(self.watch_for_signals, tg.cancel_scope)
                    await self.start_consumers(tg)
            finally:
                with anyio.move_on_after(5, shield=True):
                    await self.disconnect()

    @asynccontextmanager
    async def context(self):
        task = asyncio.create_task(self.run(enable_signal_handler=False))
        await asyncio.sleep(0)
        yield
        with suppress(asyncio.CancelledError):
            task.cancel()
            await task

    async def watch_for_signals(self, scope: CancelScope) -> None:
        with anyio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
            async for signum in signals:
                self.logger.info(f"Received signal {signum.name}")
                scope.cancel()

    async def get_result(self, message_id: ID) -> Result | None:
        if not isinstance(self.broker, ResultBackend):
            msg = f"Broker {type(self.broker)} does not support results"
            raise TypeError(msg)
        return await self.broker.get_result(f"{self.name}:{message_id}")

    async def _dispatch(self, event: str, **kwargs) -> None:
        message = kwargs.get("message")
        for middleware in self.middlewares:
            if message and (
                middleware.requires is not None
                and not isinstance(message, middleware.requires)
            ):
                continue
            method = getattr(middleware, event, None)
            if method is None:
                continue

            await method(service=self, **kwargs)

    async def dispatch_before(self, event: str, **kwargs) -> None:
        await self._dispatch(f"before_{event}", **kwargs)

    async def dispatch_after(self, event: str, **kwargs) -> None:
        await self._dispatch(f"after_{event}", **kwargs)

    async def receiver(
        self,
        consumer: Consumer,
        receive_stream: MemoryObjectReceiveStream[Message],
    ):
        consumer_timeout = to_float(
            consumer.timeout or self.broker.default_consumer_timeout,
        )
        decoder = consumer.decoder or self.broker.decoder
        async with receive_stream:
            async for raw_message in receive_stream:
                with anyio.CancelScope(shield=True):
                    await self._process(
                        consumer,
                        raw_message,
                        decoder,
                        consumer_timeout,
                    )

    async def ack(self, consumer: Consumer, message: Message) -> None:
        await self.dispatch_before("ack", consumer=consumer, raw_message=message)
        await self.broker.ack(message)
        await self.dispatch_after("ack", consumer=consumer, raw_message=message)

    async def nack(
        self,
        consumer: Consumer,
        message: Message,
        delay: int | None = None,
    ) -> None:
        await self.dispatch_after(
            "nack",
            consumer=consumer,
            raw_message=message,
        )
        await self.broker.nack(message, delay=delay)
        await self.dispatch_after(
            "nack",
            consumer=consumer,
            raw_message=message,
        )

    async def _process(
        self,
        consumer: Consumer,
        raw_message: Message,
        decoder: Decoder,
        timeout: float,
    ) -> None:
        exc: Exception | None = None
        result = None

        try:
            data = self.broker.get_message_data(raw_message)
            message = decoder.decode(data, consumer.event_type)
            message.set_context(self, raw_message)
        except (DecodeError, ValidationError) as e:
            self.logger.exception(
                f"Failed to validate message {raw_message}.",
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

        try:
            await self.dispatch_before(
                "process_message",
                consumer=consumer,
                message=message,
            )
            self.logger.info(
                f"Running consumer {consumer.name} with message {message.id}",
            )
            with anyio.fail_after(timeout):
                result = await consumer.process(message)
        except anyio.get_cancelled_exc_class():
            with anyio.fail_after(1):
                await self.nack(consumer, raw_message)
            raise
        except Exception as e:
            exc = e
        await self._handle_message_finalization(consumer, message, result, exc)

    async def _handle_message_finalization(
        self,
        consumer: Consumer,
        message: CloudEvent,
        result: Any,
        exc: Exception | None,
    ) -> None:
        try:
            await self.dispatch_after(
                "process_message",
                consumer=consumer,
                message=message,
                result=result,
                exc=exc,
            )
        except Exception as e:
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
                delay=exc.delay,
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

        await getattr(self, self.broker.default_on_exc)(consumer, message.raw)

    @asynccontextmanager
    async def subscription(
        self,
        event_type: type[CloudEvent],
        auto_ack: bool = False,
        **options,
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
        options["dynamic"] = True
        consumer = ChannelConsumer(
            channel=consumer_send,
            auto_ack=auto_ack,
            event_type=event_type,
            **options,
        )

        async with anyio.create_task_group() as tg, consumer_send, user_receive:
            tg.start_soon(self.broker.sender, self.name, consumer, send_stream)
            tg.start_soon(self.receiver, consumer, receive_stream)
            yield user_receive
