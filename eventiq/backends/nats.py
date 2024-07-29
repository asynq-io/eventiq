from __future__ import annotations

import asyncio
from abc import ABC
from datetime import timedelta, timezone
from typing import TYPE_CHECKING, Annotated, Any

from anyio.streams.memory import MemoryObjectSendStream
from nats.aio.client import Client
from nats.aio.msg import Msg as NatsMsg
from nats.js import JetStreamContext, api
from nats.js.api import ConsumerConfig
from nats.js.errors import KeyNotFoundError
from nats.js.kv import KeyValue
from pydantic import AnyUrl, UrlConstraints

from eventiq.broker import R, UrlBroker
from eventiq.exceptions import BrokerError
from eventiq.results import Error, Ok, Result, ResultBackend
from eventiq.settings import UrlBrokerSettings
from eventiq.types import Encoder
from eventiq.utils import to_float, utc_now

NatsUrl = Annotated[AnyUrl, UrlConstraints(allowed_schemes=["nats"])]


class NatsSettings(UrlBrokerSettings[NatsUrl]):
    auto_flush: bool = True


class JetStreamSettings(NatsSettings):
    fetch_timeout: int = 10
    jetstream_options: dict[str, Any] = {}
    kv_options: dict[str, Any] = {}


if TYPE_CHECKING:
    from eventiq import CloudEvent, Consumer


class AbstractNatsBroker(UrlBroker[NatsMsg, R], ABC):
    """
    :param auto_flush: auto flush messages on publish
    :param kwargs: options for base class
    """

    protocol = "nats"
    WILDCARD_ONE = "*"
    WILDCARD_MANY = ">"
    Settings = NatsSettings

    def __init__(
        self,
        *,
        auto_flush: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.client = Client()
        self._auto_flush = auto_flush
        for k, v in self.default_connection_options.items():
            self.connection_options.setdefault(k, v)

    @property
    def default_connection_options(self) -> dict[str, Any]:
        return {
            "error_cb": self._error_cb,
            "closed_cb": self._closed_cb,
            "reconnected_cb": self._reconnect_cb,
            "disconnected_cb": self._disconnect_cb,
            "max_reconnect_attempts": 10,
        }

    @staticmethod
    def get_message_metadata(raw_message: NatsMsg) -> dict[str, str]:
        try:
            return {
                "messaging.nats.sequence.consumer": str(
                    raw_message.metadata.sequence.consumer
                ),
                "messaging.nats.sequence.stream": str(
                    raw_message.metadata.sequence.stream
                ),
                "messaging.nats.num_delivered": str(raw_message.metadata.num_delivered),
            }
        except Exception:
            return {}

    async def connect(self) -> None:
        await self.client.connect(self.url, **self.connection_options)

    async def disconnect(self) -> None:
        await self.client.close()

    async def flush(self) -> None:
        await self.client.flush()

    async def _disconnect_cb(self) -> None:
        self.logger.warning("Disconnected")

    async def _reconnect_cb(self) -> None:
        self.logger.info("Reconnected")

    async def _error_cb(self, e) -> None:
        self.logger.warning(f"Broker error {e}")

    async def _closed_cb(self) -> None:
        self.logger.warning("Connection closed")

    @property
    def is_connected(self) -> bool:
        return self.client.is_connected

    @staticmethod
    def get_message_data(raw_message: NatsMsg) -> bytes:
        return raw_message.data

    def get_asyncapi_bindings(self, event_type: type[CloudEvent]) -> dict[str, Any]:
        return {"queue": event_type.get_default_topic(), "bindingVersion": "0.1.0"}


class NatsBroker(AbstractNatsBroker[None]):
    async def sender(
        self, group: str, consumer: Consumer, send_stream: MemoryObjectSendStream
    ):
        subscription = await self.client.subscribe(
            subject=self.format_topic(consumer.topic),
            queue=f"{group}:{consumer.name}",
        )
        try:
            async with send_stream:
                async for message in subscription.messages:
                    await send_stream.send(message)

        finally:
            if consumer.dynamic:
                await subscription.unsubscribe()
            self.logger.info("Sender finished for %s", consumer.name)

    async def publish(
        self, message: CloudEvent, encoder: Encoder | None = None, **kwargs
    ) -> None:
        data = self._encode_message(message, encoder)
        reply = kwargs.get("reply", "")
        headers = message.headers
        headers.setdefault("Content-Type", message.content_type)
        await self.client.publish(message.topic, data, headers=headers, reply=reply)
        if self._auto_flush or kwargs.get("flush"):
            await self.flush()

    async def ack(self, raw_message: NatsMsg) -> None:
        pass

    async def nack(self, raw_message: NatsMsg, delay: int | None = None) -> None:
        pass


class JetStreamBroker(
    AbstractNatsBroker[api.PubAck], ResultBackend[NatsMsg, api.PubAck]
):
    """
    NatsBroker with JetStream enabled
    :param prefetch_count: default number of messages to prefetch
    :param fetch_timeout: timeout for subscription pull
    :param jetstream_options: additional options passed to nc.jetstream(...)
    :param kwargs: all other options for base classes NatsBroker, Broker
    """

    Settings = JetStreamSettings

    def __init__(
        self,
        *,
        fetch_timeout: int = 10,
        jetstream_options: dict[str, Any] | None = None,
        kv_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.fetch_timeout = fetch_timeout
        self.jetstream_options = jetstream_options or {}
        self.js = JetStreamContext(self.client, **self.jetstream_options)
        self.kv_options = kv_options or {}
        self._kv: KeyValue | None = None

    async def connect(self) -> None:
        await super().connect()
        if self.store_results:
            self._kv = await self.js.create_key_value(**self.kv_options)

    @property
    def kv(self) -> KeyValue:
        if self._kv is None:
            raise BrokerError("KeyVal not initialized")
        return self._kv

    async def get_result(self, key: str) -> Result | None:
        try:
            data = await self.kv.get(key)
            if data.value:
                return self.decoder.decode(data.value, as_type=Result)
            return None
        except KeyNotFoundError:
            self.logger.warning(f"Key {key} not found")

    async def store_result(self, key: str, result: Ok | Error) -> None:
        await self.kv.put(key, self.encoder.encode(result))

    async def publish(
        self,
        message: CloudEvent,
        encoder: Encoder | None = None,
        **kwargs,
    ) -> api.PubAck:
        encoder = encoder or self.encoder
        data = encoder.encode(message)
        headers = message.headers
        headers.setdefault("Content-Type", message.content_type)
        headers.setdefault("Nats-Msg-Id", str(message.id))
        response = await self.js.publish(
            subject=message.topic,
            payload=data,
            timeout=kwargs.get("timeout"),
            stream=kwargs.get("stream"),
            headers=headers,
        )
        if self._auto_flush:
            await self.flush()
        return response

    async def sender(
        self, group: str, consumer: Consumer, send_stream: MemoryObjectSendStream
    ) -> None:
        config = consumer.options.get("config", ConsumerConfig())

        if config.ack_wait is None:
            ack_wait = (
                to_float(consumer.timeout) or self.default_consumer_timeout
            ) + 30
            config.ack_wait = ack_wait  # consumer timeout + 30s for .ack()
        batch = consumer.options.get("batch", consumer.concurrency * 2)

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
                            batch=batch, timeout=self.fetch_timeout
                        )
                        for message in messages:
                            await send_stream.send(message)
                    except asyncio.TimeoutError:
                        await asyncio.sleep(1)
        finally:
            if consumer.dynamic:
                await subscription.unsubscribe()
            self.logger.info("Sender finished for %s", consumer.name)

    def should_nack(self, raw_message: NatsMsg) -> bool:
        date = raw_message.metadata.timestamp.replace(tzinfo=timezone.utc)
        return date < (utc_now() - timedelta(seconds=self.validate_error_delay))

    async def ack(self, raw_message: NatsMsg) -> None:
        if not raw_message._ackd:
            await raw_message.ack()

    async def nack(self, raw_message: NatsMsg, delay: int | None = None) -> None:
        if not raw_message._ackd:
            await raw_message.nak(delay=delay)

    def get_num_delivered(self, raw_message: NatsMsg) -> int | None:
        return raw_message.metadata.num_delivered
