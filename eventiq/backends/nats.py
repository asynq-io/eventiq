from __future__ import annotations

from abc import ABC
from datetime import timedelta, timezone
from typing import TYPE_CHECKING, Annotated, Any, Callable

import anyio
from nats.aio.client import Client
from nats.aio.msg import Msg as NatsMsg
from nats.js import JetStreamContext, api
from nats.js.api import ConsumerConfig
from nats.js.errors import FetchTimeoutError, KeyNotFoundError
from pydantic import AnyUrl, Field, UrlConstraints

from eventiq.broker import R, UrlBroker
from eventiq.exceptions import BrokerError
from eventiq.results import ResultBackend
from eventiq.settings import UrlBrokerSettings
from eventiq.utils import to_float, utc_now

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from eventiq.types import ID, DecodedMessage

NatsUrl = Annotated[AnyUrl, UrlConstraints(allowed_schemes=["nats"])]


class NatsSettings(UrlBrokerSettings[NatsUrl]):
    auto_flush: bool = True


class JetStreamSettings(NatsSettings):
    jetstream_options: dict[str, Any] = Field({})
    kv_options: dict[str, Any] = Field({})


if TYPE_CHECKING:
    from anyio.streams.memory import MemoryObjectSendStream
    from nats.js.kv import KeyValue

    from eventiq import Consumer


class AbstractNatsBroker(UrlBroker[NatsMsg, R], ABC):
    """:param auto_flush: auto flush messages on publish
    :param kwargs: options for base class
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
        self, message: str
    ) -> Callable[[Exception | None], Awaitable[None]]:
        async def wrapped(error: Exception | None = None) -> None:
            self.logger.warning(message)
            if error:
                self.logger.error(error)

        return wrapped

    @staticmethod
    def decode_message(raw_message: NatsMsg) -> DecodedMessage:
        return raw_message.data, raw_message.headers

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
        except Exception:
            return {}

    async def connect(self) -> None:
        if not self.client.is_connected:
            await self.client.connect(self.url, **self.connection_options)

    async def disconnect(self) -> None:
        if self.client.is_connected:
            await self.client.close()

    async def flush(self) -> None:
        await self.client.flush()

    @property
    def is_connected(self) -> bool:
        return self.client.is_connected

    async def ack(self, raw_message: NatsMsg) -> None:
        await raw_message.ack()

    async def nack(self, raw_message: NatsMsg, delay: int | None = None) -> None:
        await raw_message.nak(delay=delay)


class NatsBroker(AbstractNatsBroker[None]):
    Settings = NatsSettings

    async def sender(
        self,
        group: str,
        consumer: Consumer,
        send_stream: MemoryObjectSendStream,
    ) -> None:
        subscription = await self.client.subscribe(
            subject=self.format_topic(consumer.topic),
            queue=f"{group}:{consumer.name}",
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
        **kwargs: Any,
    ) -> None:
        reply = kwargs.get("reply", "")
        await self.client.publish(topic, body, headers=headers, reply=reply)
        if self._auto_flush or kwargs.get("flush"):
            await self.flush()


class JetStreamBroker(
    AbstractNatsBroker[api.PubAck],
    ResultBackend[NatsMsg, api.PubAck],
):
    """NatsBroker with JetStream enabled
    :param prefetch_count: default number of messages to prefetch
    :param fetch_timeout: timeout for subscription pull
    :param jetstream_options: additional options passed to nc.jetstream(...)
    :param kwargs: all other options for base classes NatsBroker, Broker.
    """

    Settings = JetStreamSettings
    kv_error = "KeyVal not initialized"

    def __init__(
        self,
        *,
        jetstream_options: dict[str, Any] | None = None,
        kv_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.jetstream_options = jetstream_options or {}
        self.js = JetStreamContext(self.client, **self.jetstream_options)
        self.kv_options = kv_options or {}
        self._kv: KeyValue | None = None

    async def init_storage(self) -> None:
        self._kv = await self.js.create_key_value(**self.kv_options)

    @property
    def kv(self) -> KeyValue:
        if self._kv is None:
            raise BrokerError(self.kv_error)
        return self._kv

    async def get_result(self, key: str) -> bytes | None:
        try:
            data = await self.kv.get(key)
            return data.value  # noqa: TRY300
        except KeyNotFoundError:
            self.logger.warning("Key %s not found", key)

    async def store_result(self, key: str, result: bytes) -> None:
        await self.kv.put(key, result)

    async def publish(
        self,
        topic: str,
        body: bytes,
        *,
        headers: dict[str, str],
        message_id: ID | None = None,
        timeout: float | None = None,
        stream: str | None = None,
        **kwargs: Any,
    ) -> api.PubAck:
        if "Nats-Msg-Id" not in headers and message_id:
            headers["Nats-Msg-Id"] = str(message_id)
        response = await self.js.publish(
            topic, payload=body, timeout=timeout, stream=stream, headers=headers
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
            "ack_wait": (to_float(consumer.timeout) or self.default_consumer_timeout)
            + 30,
            "max_ack_pending": 10_000,
        }
        for key in ConsumerConfig.__dataclass_fields__:
            if key in consumer.options:
                config_kwargs[key] = consumer.options[key]
        config = ConsumerConfig(**config_kwargs)
        batch = consumer.options.get("batch", consumer.concurrency * 2)
        fetch_timeout = consumer.options.get("fetch_timeout", 10)
        heartbeat = consumer.options.get("heartbeat", 0.1)
        subscription = await self.js.pull_subscribe(
            subject=self.format_topic(consumer.topic),
            durable=f"{group}:{consumer.name}",
            config=config,
        )
        try:
            async with send_stream:
                while True:
                    try:
                        messages = await subscription.fetch(
                            batch=batch,
                            timeout=fetch_timeout,
                            heartbeat=heartbeat,
                        )
                        for message in messages:
                            await send_stream.send(message)
                    except FetchTimeoutError:  # noqa: PERF203
                        pass
        finally:
            if consumer.dynamic:
                await subscription.unsubscribe()
            self.logger.info("Sender finished for %s", consumer.name)

    def should_nack(self, raw_message: NatsMsg) -> bool:
        date = raw_message.metadata.timestamp.replace(tzinfo=timezone.utc)
        return date < (utc_now() - timedelta(seconds=self.validate_error_delay))

    def get_num_delivered(self, raw_message: NatsMsg) -> int | None:
        return raw_message.metadata.num_delivered
