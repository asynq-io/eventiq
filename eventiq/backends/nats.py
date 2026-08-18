from __future__ import annotations

import asyncio
from abc import ABC
from typing import TYPE_CHECKING, Annotated, Any

import anyio
from nats.aio.client import Client
from nats.aio.msg import Msg as NatsMsg
from nats.errors import NotJSMessageError
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js import JetStreamContext, api
from nats.js.api import ConsumerConfig
from pydantic import AnyUrl, Field, NonNegativeFloat, UrlConstraints

from eventiq.broker import R, UrlBroker
from eventiq.settings import UrlBrokerSettings
from eventiq.utils import to_float

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from eventiq.types import ID, DecodedMessage

NatsUrl = Annotated[AnyUrl, UrlConstraints(allowed_schemes=["nats"])]

PENDING_LIMIT_OPTIONS = ("pending_msgs_limit", "pending_bytes_limit")


class NatsSettings(UrlBrokerSettings[NatsUrl]):
    auto_flush: bool = True


class JetStreamSettings(NatsSettings):
    jetstream_options: dict[str, Any] = Field({})
    poll_interval: NonNegativeFloat = 0.0
    heartbeat: NonNegativeFloat = 0.1


if TYPE_CHECKING:
    from anyio.streams.memory import MemoryObjectSendStream

    from eventiq import Consumer


class AbstractNatsBroker(UrlBroker[NatsMsg, R], ABC):
    """:param auto_flush: Auto-flush messages after publish.
    :param kwargs: Options forwarded to the base class.
    """

    protocol = "nats"
    WILDCARD_ONE = "*"
    WILDCARD_MANY = ">"

    def __init__(
        self,
        *,
        auto_flush: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.client = Client()
        self._auto_flush = auto_flush
        self.connection_options.setdefault("max_reconnect_attempts", 10)
        for k in ("error", "closed", "reconnected", "disconnected"):
            self.connection_options.setdefault(f"{k}_cb", self._default_cb(k))

    def _default_cb(
        self,
        message: str,
    ) -> Callable[[Exception | None], Awaitable[None]]:
        async def wrapped(error: Exception | None = None) -> None:
            self.logger.warning(message)
            if error:
                self.logger.error(error)

        return wrapped

    @staticmethod
    def decode_message(raw_message: NatsMsg) -> DecodedMessage:
        return raw_message.data, raw_message.headers or {}

    @staticmethod
    def get_message_metadata(raw_message: NatsMsg) -> dict[str, str]:
        try:
            return {
                "messaging.nats.sequence.consumer": str(
                    raw_message.metadata.sequence.consumer,
                ),
                "messaging.nats.sequence.stream": str(
                    raw_message.metadata.sequence.stream,
                ),
                "messaging.nats.num_delivered": str(raw_message.metadata.num_delivered),
            }
        except (AttributeError, NotJSMessageError):
            return {}

    async def connect(self) -> None:
        if not self.client.is_connected:
            await self.client.connect(self.url, **self.connection_options)

    async def disconnect(self) -> None:
        if self.client.is_connected:
            await self.client.drain()
            await self.client.close()

    async def flush(self) -> None:
        await self.client.flush()

    @property
    def is_connected(self) -> bool:
        return self.client.is_connected


class NatsBroker(AbstractNatsBroker[None]):
    """
    Nats broker implementation
    """

    Settings = NatsSettings

    async def ack(self, raw_message: NatsMsg) -> None:
        """No-op: core NATS messages carry no reply subject to acknowledge."""

    async def nack(self, raw_message: NatsMsg, delay: int | None = None) -> None:
        """No-op: core NATS has no redelivery, so a message cannot be rejected."""

    async def sender(
        self,
        group: str,
        consumer: Consumer,
        send_stream: MemoryObjectSendStream,
    ) -> None:
        queue = "" if consumer.dynamic else f"{group}:{consumer.name}"
        # Core NATS has no publisher backpressure: once a subscription's pending
        # queue is full nats-py drops the message and reports a slow consumer.
        # Only forward the limits the user set explicitly, so nats-py's generous
        # defaults stay in place otherwise.
        pending_limits = {
            key: consumer.options[key]
            for key in PENDING_LIMIT_OPTIONS
            if key in consumer.options
        }
        subscription = await self.client.subscribe(
            subject=self.format_topic(consumer.topic),
            queue=queue,
            **pending_limits,
        )
        try:
            async with send_stream:
                async for message in subscription.messages:
                    await send_stream.send(message)
        finally:
            with anyio.move_on_after(1, shield=True):
                if consumer.dynamic:
                    await subscription.unsubscribe()
            self.logger.info("Sender finished for %s", consumer.name)

    async def publish(
        self,
        topic: str,
        body: bytes,
        *,
        headers: dict[str, str],
        reply: str = "",
        flush: bool = False,
        **_: Any,
    ) -> None:
        await self.client.publish(topic, body, headers=headers, reply=reply)
        if self._auto_flush or flush:
            await self.flush()


class JetStreamBroker(
    AbstractNatsBroker[api.PubAck],
):
    """NatsBroker with JetStream enabled.

    :param jetstream_options: Additional options passed to ``nc.jetstream(...)``.
    :param poll_interval: delay (in seconds) between consecutive pull-subscription
        fetch calls in the sender loop.
    :param heartbeat: heartbeat interval (in seconds) for pull-subscription fetch
        calls in the sender loop.
    :param kwargs: all other options for base classes NatsBroker, Broker.
    """

    _DEFAULT_MAX_RETRIES = 3
    Settings = JetStreamSettings

    async def ack(self, raw_message: NatsMsg) -> None:
        await raw_message.ack()

    async def nack(self, raw_message: NatsMsg, delay: int | None = None) -> None:
        await raw_message.nak(delay=delay)

    def __init__(
        self,
        *,
        jetstream_options: dict[str, Any] | None = None,
        poll_interval: float = 0.0,
        heartbeat: float = 0.1,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.jetstream_options = jetstream_options or {}
        self.js = JetStreamContext(self.client, **self.jetstream_options)
        self.poll_interval = poll_interval
        self.heartbeat = heartbeat

    async def publish(
        self,
        topic: str,
        body: bytes,
        *,
        headers: dict[str, str],
        message_id: ID,
        timeout: float | None = None,
        stream: str | None = None,
        **_: Any,
    ) -> api.PubAck:
        if "Nats-Msg-Id" not in headers:
            headers["Nats-Msg-Id"] = str(message_id)
        response = await self.js.publish(
            topic,
            payload=body,
            timeout=timeout,
            stream=stream,
            headers=headers,
        )
        if self._auto_flush:
            await self.flush()
        return response

    async def sender(
        self,
        group: str,
        consumer: Consumer,
        send_stream: MemoryObjectSendStream,
    ) -> None:
        config_kwargs: dict[str, Any] = {
            "name": f"{group}:{consumer.name}",
            "ack_wait": (to_float(consumer.timeout) or self.default_consumer_timeout)
            + 30,
            "max_ack_pending": 10_000,
        }
        for key in ConsumerConfig.__dataclass_fields__:
            if key in consumer.options:
                config_kwargs[key] = consumer.options[key]
        config = ConsumerConfig(**config_kwargs)
        fetch_timeout = consumer.options.get("fetch_timeout", 10)
        heartbeat = consumer.options.get("heartbeat", self.heartbeat)
        poll_interval = consumer.options.get("poll_interval", self.poll_interval)
        durable = None if consumer.dynamic else f"{group}:{consumer.name}"
        subscription = await self.js.pull_subscribe(
            subject=self.format_topic(consumer.topic),
            durable=durable,
            config=config,
        )
        try:
            async with send_stream:
                while True:
                    try:
                        batch = (
                            consumer.concurrency
                            - send_stream.statistics().current_buffer_used
                        )
                        if batch <= 0:
                            await anyio.sleep(0.1)
                            continue
                        self.logger.debug("Fetching %d messages", batch)
                        messages = await subscription.fetch(
                            batch=batch,
                            timeout=fetch_timeout,
                            heartbeat=heartbeat,
                        )
                        for message in messages:
                            await send_stream.send(message)
                    except NatsTimeoutError:
                        self.logger.debug("Suppressing nats timeout error")
                    await asyncio.sleep(poll_interval)
        finally:
            # Shielded: cancellation is level-triggered, so an unshielded await here
            # would be cancelled immediately and leak the ephemeral consumer.
            with anyio.move_on_after(1, shield=True):
                if consumer.dynamic:
                    await subscription.unsubscribe()
            self.logger.info("Stopped sender for consumer: %s", consumer.name)

    def should_nack(self, raw_message: NatsMsg) -> bool:
        return raw_message.metadata.num_delivered <= self._DEFAULT_MAX_RETRIES

    def get_num_delivered(self, raw_message: NatsMsg) -> int | None:
        return raw_message.metadata.num_delivered
